use std::fmt::Write as _;
use std::sync::Arc;

use owo_colors::OwoColorize;
use regex::Regex;

use super::LineFormatter;
use crate::source::LogEvent;

/// Wraps a line formatter and applies stern-style bold-red emphasis to regex matches.
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
    fn format_into(&self, event: &LogEvent, buf: &mut String) {
        self.inner.format_into(event, buf);
        if let Some(highlighted) = highlight_default_line(buf, &self.re) {
            *buf = highlighted;
        }
    }
}

fn highlight_default_line(text: &str, re: &Regex) -> Option<String> {
    let mut last = 0usize;
    let mut out = None::<String>;
    for m in re.find_iter(text) {
        let out = out.get_or_insert_with(|| {
            String::with_capacity(text.len().saturating_add(text.len().min(4096)))
        });
        out.push_str(&text[last..m.start()]);
        let _ = write!(out, "{}", (&text[m.start()..m.end()]).red().bold());
        last = m.end();
    }
    out.map(|mut highlighted| {
        highlighted.push_str(&text[last..]);
        highlighted
    })
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

        let out = highlight_default_line("- foobar!", &re).expect("match");
        assert!(out.contains("foobar"));
        assert!(out.starts_with('-'));
        assert!(out.contains('\x1b')); // ansi wrap
        assert!(
            !out.ends_with('-'),
            "prefix before match should survive: {out:?}"
        );
    }

    #[test]
    fn no_match_returns_none() {
        let re = compile_stern_highlight_regex(&["needle".into()], &[])
            .unwrap()
            .expect("pattern");
        assert!(highlight_default_line("no haystack here", &re).is_none());
    }
}
