use std::sync::Arc;

use owo_colors::OwoColorize;
use regex::Regex;

use super::LineFormatter;
use crate::source::LogEvent;

pub struct SternHighlightLineFormatter {
    inner: Arc<dyn LineFormatter>,
    re: Regex,
}

impl SternHighlightLineFormatter {
    pub fn new(inner: Arc<dyn LineFormatter>, re: Regex) -> Self {
        Self { inner, re }
    }
}

impl LineFormatter for SternHighlightLineFormatter {
    fn format_line(&self, event: &LogEvent) -> String {
        let line = self.inner.format_line(event);
        highlight_default_line(&line, &self.re)
    }
}

fn highlight_default_line(text: &str, re: &Regex) -> String {
    let mut last = 0usize;
    let mut out = String::with_capacity(text.len().saturating_add(text.len().min(4096)));
    for m in re.find_iter(text) {
        out.push_str(&text[last..m.start()]);
        let styled = (&text[m.start()..m.end()]).red().bold().to_string();
        out.push_str(&styled);
        last = m.end();
    }
    out.push_str(&text[last..]);
    out
}

/// Stern merges `--include` and `--highlight`, sorts alternation segments by descending pattern-string
/// length, and wraps matched spans in bold-red style.
pub fn compile_stern_highlight_regex(
    includes: &[String],
    highlights: &[String],
) -> Result<Option<Regex>, regex::Error> {
    let mut branches: Vec<String> = Vec::with_capacity(includes.len() + highlights.len());
    for s in includes {
        let t = s.trim();
        if !t.is_empty() {
            branches.push(t.to_string());
        }
    }
    for s in highlights {
        let t = s.trim();
        if !t.is_empty() {
            branches.push(t.to_string());
        }
    }
    if branches.is_empty() {
        return Ok(None);
    }

    branches.sort_by_key(|b| std::cmp::Reverse(b.len()));
    let merged = branches.join("|");
    Ok(Some(Regex::new(&format!("({merged})"))?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_include_and_highlight_branches_sorted() {
        let re = compile_stern_highlight_regex(&["foobar".into(), "a".into()], &["zzz".into()])
            .unwrap()
            .expect("combined pattern");
        assert!(re.is_match("foobar"));

        let out = highlight_default_line("- foobar!", &re);
        assert!(out.contains("foobar"));
        assert!(out.starts_with('-'));
        assert!(out.contains('\x1b')); // ansi wrap
        assert!(
            !out.ends_with('-'),
            "prefix before match should survive: {out:?}"
        );
    }
}
