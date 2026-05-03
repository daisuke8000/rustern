use crate::render::LineFormatter;
use crate::source::LogEvent;

pub struct RawLineFormatter;

impl LineFormatter for RawLineFormatter {
    fn format_line(&self, event: &LogEvent) -> String {
        format!("{}\n", event.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{ContextName, Labels, SourceKind, SourceMeta};
    use chrono::Utc;
    use std::sync::Arc;

    #[test]
    fn raw_is_message_only() {
        let ev = LogEvent {
            source: Arc::new(SourceMeta {
                context: ContextName("c".into()),
                namespace: "ns".into(),
                pod: "p".into(),
                container: "c".into(),
                kind: SourceKind::PodLog,
                node: None,
                labels: Arc::new(Labels::default()),
                uid: "u".into(),
            }),
            timestamp: Utc::now(),
            message: Arc::from("plain"),
            structured: None,
            level: None,
            palette_index: None,
        };
        assert_eq!(RawLineFormatter.format_line(&ev), "plain\n");
    }
}
