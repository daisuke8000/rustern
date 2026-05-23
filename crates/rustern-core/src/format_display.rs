//! Timestamp display knobs for the default line formatter (`stern`-inspired presets).

use std::str::FromStr;

/// When and how wall-clock prefixes are emitted (default formatter only).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimestampStyle {
    Omit,
    /// RFC 3339 with nanosecond precision (stern `--timestamps=default`).
    Rfc3339,
    /// Compact `MM-DD HH:MM:SS` in [`TimestampZone`] (stern-inspired).
    SternShort,
    /// Unix epoch seconds (UTC clock; ignores display zone).
    EpochSeconds,
}

/// Zone mapping for textual stamps (`SternShort` / `Rfc3339`).
#[derive(Clone, Copy, Debug)]
pub enum TimestampZone {
    Utc,
    Local,
    Iana(chrono_tz::Tz),
}

impl TimestampZone {
    pub fn parse_arg(s: &str) -> Result<Self, String> {
        let t = s.trim();
        if t.is_empty() {
            return Err("empty --timezone".into());
        }
        if t.eq_ignore_ascii_case("utc") {
            return Ok(Self::Utc);
        }
        if t.eq_ignore_ascii_case("local") {
            return Ok(Self::Local);
        }
        chrono_tz::Tz::from_str(t)
            .map(Self::Iana)
            .map_err(|_| format!("invalid --timezone ({t})"))
    }
}
