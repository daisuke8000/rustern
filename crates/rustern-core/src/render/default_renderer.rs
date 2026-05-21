use chrono::{DateTime, Local, SecondsFormat, Utc};

use crate::format_display::{TimestampStyle, TimestampZone};
use crate::render::LineFormatter;
use crate::source::LogEvent;

const PALETTE: &[(u8, u8, u8)] = &[
    (255, 99, 71),
    (60, 179, 113),
    (100, 149, 237),
    (218, 112, 214),
    (255, 165, 0),
    (123, 104, 238),
    (32, 178, 170),
    (255, 192, 203),
];

pub struct DefaultLineFormatter {
    pub timestamp_style: TimestampStyle,
    pub timestamp_zone: TimestampZone,
    pub color_enabled: bool,
    pub pod_colors: bool,
    pub container_colors: bool,
}

fn format_wall_prefix(
    dt_utc: &DateTime<Utc>,
    style: TimestampStyle,
    zone: TimestampZone,
) -> Option<String> {
    Some(match style {
        TimestampStyle::Omit => return None,
        TimestampStyle::EpochSeconds => dt_utc.timestamp().to_string(),
        TimestampStyle::Rfc3339 => match zone {
            TimestampZone::Utc => dt_utc.to_rfc3339_opts(SecondsFormat::AutoSi, false),
            TimestampZone::Local => dt_utc
                .with_timezone(&Local)
                .to_rfc3339_opts(SecondsFormat::AutoSi, false),
            TimestampZone::Iana(tz) => dt_utc
                .with_timezone(&tz)
                .to_rfc3339_opts(SecondsFormat::AutoSi, false),
        },
        TimestampStyle::SternShort => match zone {
            TimestampZone::Utc => dt_utc.format("%m-%d %H:%M:%S").to_string(),
            TimestampZone::Local => dt_utc
                .with_timezone(&Local)
                .format("%m-%d %H:%M:%S")
                .to_string(),
            TimestampZone::Iana(tz) => dt_utc
                .with_timezone(&tz)
                .format("%m-%d %H:%M:%S")
                .to_string(),
        },
    })
}

fn push_colored(slice: &mut String, text: &str, idx: Option<u8>, color_enabled: bool) {
    if color_enabled
        && let Some(idx) = idx
    {
        let (r, g, b) = PALETTE[(idx as usize) % PALETTE.len()];
        use owo_colors::OwoColorize;
        use std::fmt::Write as _;
        let _ = write!(slice, "{}", text.truecolor(r, g, b));
        return;
    }
    slice.push_str(text);
}

impl LineFormatter for DefaultLineFormatter {
    fn format_line(&self, event: &LogEvent) -> String {
        let mut line = String::new();
        if let Some(p) =
            format_wall_prefix(&event.timestamp, self.timestamp_style, self.timestamp_zone)
        {
            line.push_str(&p);
            line.push(' ');
        }
        let colorize = self.color_enabled;
        push_colored(
            &mut line,
            &event.source.pod,
            if self.pod_colors {
                event.palette_index
            } else {
                None
            },
            colorize,
        );
        line.push('/');
        push_colored(
            &mut line,
            &event.source.container,
            if self.container_colors {
                event.container_palette_index
            } else {
                None
            },
            colorize,
        );
        line.push_str(" | ");
        line.push_str(&event.message);
        line.push('\n');
        line
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::{RenderCommand, render_task};
    use crate::source::{ContextName, Labels, SourceKind, SourceMeta};
    use chrono::Utc;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, duplex};
    use tokio::sync::mpsc;

    fn ev() -> LogEvent {
        LogEvent {
            source: Arc::new(SourceMeta {
                context: ContextName("c".into()),
                namespace: "ns".into(),
                pod: "p".into(),
                container: "ctn".into(),
                kind: SourceKind::PodLog,
                node: None,
                labels: Arc::new(Labels::default()),
                uid: "uid-1".into(),
            }),
            timestamp: Utc::now(),
            message: Arc::from("hello"),
            structured: None,
            level: None,
            palette_index: None,
            container_palette_index: None,
        }
    }

    fn formatter(color_enabled: bool) -> DefaultLineFormatter {
        DefaultLineFormatter {
            timestamp_style: TimestampStyle::Omit,
            timestamp_zone: TimestampZone::Utc,
            color_enabled,
            pod_colors: true,
            container_colors: true,
        }
    }

    #[test]
    fn formats_prefix_and_message_no_color() {
        assert_eq!(formatter(false).format_line(&ev()), "p/ctn | hello\n");
    }

    #[test]
    fn colors_pod_and_container_when_indices_set() {
        let mut e = ev();
        e.palette_index = Some(0);
        e.container_palette_index = Some(1);
        let out = formatter(true).format_line(&e);
        assert!(out.contains('\x1b'), "expected ANSI escapes");
        assert!(out.contains("hello"));
        assert!(out.contains('/'));
    }

    #[test]
    fn pod_only_colors_pod_segment() {
        let mut e = ev();
        e.palette_index = Some(0);
        e.container_palette_index = Some(1);
        let f = DefaultLineFormatter {
            pod_colors: true,
            container_colors: false,
            ..formatter(true)
        };
        let out = f.format_line(&e);
        let slash = out.find('/').expect("pod/container separator");
        let container_part = &out[slash + 1..];
        let container_end = container_part.find(" | ").expect("message separator");
        assert!(
            out[..slash].contains('\x1b'),
            "pod segment should be colored"
        );
        assert!(
            !container_part[..container_end].contains('\x1b'),
            "container segment should be plain"
        );
        assert!(out.contains("hello"));
    }

    #[tokio::test]
    async fn render_task_writes_lines_and_flush_on_shutdown() {
        let (mut rd, wr) = duplex(4096);
        let (tx, rx) = mpsc::channel::<RenderCommand>(8);
        let h = tokio::spawn(async move {
            render_task(rx, wr, Arc::new(formatter(false)))
                .await
                .unwrap();
        });
        tx.send(RenderCommand::Line(ev())).await.unwrap();
        tx.send(RenderCommand::Shutdown).await.unwrap();
        h.await.unwrap();
        let mut buf = vec![0u8; 64];
        let n = rd.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"p/ctn | hello\n");
    }
}
