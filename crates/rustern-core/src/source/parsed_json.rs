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
        match self {
            Self::Serde(v) => {
                let ptr = dot_pointer(dot_path);
                v.pointer(&ptr).and_then(|x| x.as_str())
            }
            Self::Jaq(v) => val_dot_path(v, dot_path).and_then(val_as_str),
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

fn dot_pointer(path: &str) -> String {
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
