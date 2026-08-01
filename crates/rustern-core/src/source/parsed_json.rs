use jaq_json::Val;
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone)]
pub enum ParsedJson {
    Serde(Value),
    Jaq(Val),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("failed to convert parsed JSON to jaq value")]
pub(crate) struct JaqConvertError;

impl ParsedJson {
    pub fn level_str_at_dot_path(&self, dot_path: &str) -> Option<&str> {
        self.level_str_at_dot_path_with_serde_pointer(dot_path, None, None)
    }

    pub(crate) fn level_str_at_dot_path_with_serde_pointer(
        &self,
        dot_path: &str,
        serde_pointer: Option<&str>,
        jaq_path_keys: Option<&[Val]>,
    ) -> Option<&str> {
        match self {
            Self::Serde(v) => {
                if let Some(ptr) = serde_pointer {
                    v.pointer(ptr).and_then(|x| x.as_str())
                } else {
                    let ptr = json_pointer_from_dot_path(dot_path);
                    v.pointer(&ptr).and_then(|x| x.as_str())
                }
            }
            Self::Jaq(v) => {
                if let Some(keys) = jaq_path_keys {
                    val_dot_path_with_keys(v, keys).and_then(val_as_str)
                } else {
                    val_dot_path(v, dot_path).and_then(val_as_str)
                }
            }
        }
    }

    pub fn to_serde_value(&self) -> Value {
        match self {
            Self::Serde(v) => v.clone(),
            Self::Jaq(v) => serde_json::from_str(&format!("{v}")).unwrap_or(Value::Null),
        }
    }

    pub(crate) fn to_jaq_val(&self) -> Result<Val, JaqConvertError> {
        match self {
            Self::Jaq(v) => Ok(v.clone()),
            Self::Serde(v) => serde_json::from_value(v.clone()).map_err(|_| JaqConvertError),
        }
    }
}

pub(crate) fn json_pointer_from_dot_path(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }
    format!(
        "/{}",
        path.split('.')
            .map(|p| p.replace('/', "~1"))
            .collect::<Vec<_>>()
            .join("/")
    )
}

pub(crate) fn jaq_dot_path_keys(path: &str) -> Vec<Val> {
    path.split('.')
        .filter(|segment| !segment.is_empty())
        .map(|segment| Val::utf8_str(segment.to_owned()))
        .collect()
}

fn val_dot_path_with_keys<'a>(v: &'a Val, keys: &[Val]) -> Option<&'a Val> {
    let mut current = v;
    for key in keys {
        let Val::Obj(map) = current else {
            return None;
        };
        current = map.get(key)?;
    }
    Some(current)
}

fn val_dot_path<'a>(v: &'a Val, path: &str) -> Option<&'a Val> {
    let mut current = v;
    for segment in path.split('.') {
        if segment.is_empty() {
            continue;
        }
        let Val::Obj(map) = current else {
            return None;
        };
        let key = Val::utf8_str(segment.to_owned());
        current = map.get(&key)?;
    }
    Some(current)
}

fn val_as_str(v: &Val) -> Option<&str> {
    match v {
        Val::TStr(b) => std::str::from_utf8(b.as_ref()).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precomputed_jaq_keys_match_dot_path_lookup() {
        let raw = r#"{"meta":{"level":"warn"}}"#;
        let parsed = ParsedJson::Jaq(serde_json::from_str(raw).unwrap());
        let keys = jaq_dot_path_keys("meta.level");

        assert_eq!(
            parsed.level_str_at_dot_path("meta.level"),
            parsed.level_str_at_dot_path_with_serde_pointer(
                "meta.level",
                None,
                Some(keys.as_slice())
            )
        );
    }

    #[test]
    fn precomputed_jaq_keys_preserve_empty_segment_behavior() {
        let raw = r#"{"meta":{"level":"warn"}}"#;
        let parsed = ParsedJson::Jaq(serde_json::from_str(raw).unwrap());
        let keys = jaq_dot_path_keys(".meta..level.");

        assert_eq!(
            parsed.level_str_at_dot_path(".meta..level."),
            parsed.level_str_at_dot_path_with_serde_pointer(
                ".meta..level.",
                None,
                Some(keys.as_slice())
            )
        );
    }
}
