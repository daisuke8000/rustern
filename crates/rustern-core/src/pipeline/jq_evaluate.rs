use std::sync::Arc;

use futures::stream::{Stream, StreamExt};
use jaq_core::data::JustLut;
use jaq_core::load::{Arena, File, Loader};
use jaq_core::{Compiler, Ctx, Filter, Vars};
use jaq_json::Val;

use crate::source::{LogEvent, LogSourceError};

#[derive(Debug, thiserror::Error)]
pub enum JqError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("evaluation error: {0}")]
    Eval(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryMode {
    Replace,
    Append,
    Filter,
}

#[derive(Clone)]
pub struct CompiledFilter {
    inner: Arc<Filter<JustLut<Val>>>,
}

fn val_is_truthy(v: &Val) -> bool {
    !matches!(v, Val::Null | Val::Bool(false))
}

pub fn validate_filter(expr: &str) -> Result<CompiledFilter, JqError> {
    let program = File {
        code: expr,
        path: (),
    };
    let defs = jaq_core::defs()
        .chain(jaq_std::defs())
        .chain(jaq_json::defs());
    let loader = Loader::new(defs);
    let arena = Arena::default();
    let modules = loader
        .load(&arena, program)
        .map_err(|e| JqError::Parse(format!("{e:?}")))?;
    let funs = jaq_core::funs::<JustLut<Val>>()
        .chain(jaq_std::funs::<JustLut<Val>>())
        .chain(jaq_json::funs());
    let filter = Compiler::default()
        .with_funs(funs)
        .compile(modules)
        .map_err(|e| JqError::Parse(format!("{e:?}")))?;
    Ok(CompiledFilter {
        inner: Arc::new(filter),
    })
}

fn run_filter(filter: &CompiledFilter, value: Val) -> Result<Vec<Val>, JqError> {
    let ctx = Ctx::<JustLut<Val>>::new(&filter.inner.lut, Vars::new([]));
    let out: Vec<Val> = filter
        .inner
        .id
        .run((ctx, value))
        .filter_map(|r| r.ok())
        .collect();
    Ok(out)
}

pub fn jq_evaluate<S>(
    inner: S,
    filter: CompiledFilter,
    mode: QueryMode,
) -> impl Stream<Item = Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    inner.filter_map(move |r| {
        let filter = filter.clone();
        async move {
            match r {
                Ok(mut ev) => {
                    let Some(v) = ev.structured.take() else {
                        return match mode {
                            QueryMode::Filter => None,
                            _ => Some(Ok(ev)),
                        };
                    };
                    let value: Val = match serde_json::from_value(v) {
                        Ok(v) => v,
                        Err(_) => return Some(Ok(ev)),
                    };
                    let outputs = match run_filter(&filter, value) {
                        Ok(o) => o,
                        Err(e) => return Some(Err(LogSourceError::Api(e.to_string()))),
                    };
                    match mode {
                        QueryMode::Filter => {
                            if outputs.iter().any(val_is_truthy) {
                                Some(Ok(ev))
                            } else {
                                None
                            }
                        }
                        QueryMode::Replace => {
                            let rendered = outputs
                                .iter()
                                .map(|v| format!("{v}"))
                                .collect::<Vec<_>>()
                                .join("\n");
                            ev.message = Arc::from(rendered.as_str());
                            Some(Ok(ev))
                        }
                        QueryMode::Append => {
                            let rendered = outputs
                                .iter()
                                .map(|v| format!("{v}"))
                                .collect::<Vec<_>>()
                                .join("\n");
                            ev.message =
                                Arc::from(format!("{} | {}", ev.message, rendered).as_str());
                            Some(Ok(ev))
                        }
                    }
                }
                e => Some(e),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{ContextName, Labels, SourceKind, SourceMeta};
    use chrono::Utc;
    use std::sync::Arc;

    fn ev_json(raw: &str) -> LogEvent {
        LogEvent {
            source: Arc::new(SourceMeta {
                context: ContextName("ctx".into()),
                namespace: "ns".into(),
                pod: "p".into(),
                container: "c".into(),
                kind: SourceKind::PodLog,
                node: None,
                labels: Arc::new(Labels::default()),
                uid: "u".into(),
                palette_index: None,
                container_palette_index: None,
            }),
            timestamp: Utc::now(),
            message: Arc::from(raw),
            structured: Some(serde_json::from_str(raw).unwrap()),
            level: None,
            palette_index: None,
            container_palette_index: None,
        }
    }

    #[test]
    fn validates_simple_path() {
        assert!(validate_filter(".level").is_ok());
    }

    #[test]
    fn rejects_unclosed_paren() {
        assert!(validate_filter("(unclosed").is_err());
    }

    #[tokio::test]
    async fn replace_mode_overwrites_message() {
        let f = validate_filter(".level").unwrap();
        let raw = r#"{"level":"error","msg":"boom"}"#;
        let s = futures::stream::iter(vec![Ok(ev_json(raw))]);
        let out: Vec<_> = jq_evaluate(s, f, QueryMode::Replace).collect().await;
        assert_eq!(&*out[0].as_ref().unwrap().message, "\"error\"");
        assert!(out[0].as_ref().unwrap().structured.is_none());
    }

    #[tokio::test]
    async fn append_mode_concatenates() {
        let f = validate_filter(".level").unwrap();
        let raw = r#"{"level":"error","msg":"boom"}"#;
        let s = futures::stream::iter(vec![Ok(ev_json(raw))]);
        let out: Vec<_> = jq_evaluate(s, f, QueryMode::Append).collect().await;
        let m = &*out[0].as_ref().unwrap().message;
        assert!(m.contains("error"));
        assert!(m.contains('|'));
    }

    #[tokio::test]
    async fn filter_mode_keeps_when_truthy() {
        let f = validate_filter(r#"select(.level=="error")"#).unwrap();
        let raw = r#"{"level":"error","msg":"boom"}"#;
        let s = futures::stream::iter(vec![Ok(ev_json(raw))]);
        let out: Vec<_> = jq_evaluate(s, f, QueryMode::Filter).collect().await;
        assert_eq!(out.len(), 1);
        assert_eq!(&*out[0].as_ref().unwrap().message, raw);
    }

    #[tokio::test]
    async fn filter_mode_drops_when_falsy_or_null() {
        let f = validate_filter(r#"select(.level=="error")"#).unwrap();
        let raw = r#"{"level":"info"}"#;
        let s = futures::stream::iter(vec![Ok(ev_json(raw))]);
        let out: Vec<_> = jq_evaluate(s, f, QueryMode::Filter).collect().await;
        assert!(out.is_empty());
    }
}
