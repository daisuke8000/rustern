//! Shared limits for user-supplied regular expressions (ReDoS mitigation).

use regex::Regex;

/// Maximum length of a single user regex pattern (bytes).
pub const MAX_USER_REGEX_PATTERN_LEN: usize = 1024;

/// Compile a user regex after enforcing [`MAX_USER_REGEX_PATTERN_LEN`].
pub fn compile_user_regex(label: &str, pattern: &str) -> Result<Regex, String> {
    if pattern.len() > MAX_USER_REGEX_PATTERN_LEN {
        return Err(format!(
            "{label}: pattern exceeds {MAX_USER_REGEX_PATTERN_LEN} characters"
        ));
    }
    Regex::new(pattern).map_err(|e| format!("invalid {label} regex: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_oversized_pattern() {
        let long = "a".repeat(MAX_USER_REGEX_PATTERN_LEN + 1);
        let err = compile_user_regex("--include", &long).unwrap_err();
        assert!(err.contains("exceeds"));
    }

    #[test]
    fn accepts_normal_pattern() {
        compile_user_regex("--include", "error|warn").unwrap();
    }
}
