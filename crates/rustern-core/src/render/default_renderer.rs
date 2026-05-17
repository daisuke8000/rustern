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

impl LineFormatter for DefaultLineFormatter {
    fn format_line(&self, event: &LogEvent) -> String {
        let mut line = String::new();
        if let Some(p) =
            format_wall_prefix(&event.timestamp, self.timestamp_style, self.timestamp_zone)
        {
            line.push_str(&p);
            line.push(' ');
        }
        let prefix = format!("{}/{}", event.source.pod, event.source.container);
        if self.color_enabled {
            if let Some(idx) = event.palette_index {
                let (r, g, b) = PALETTE[(idx as usize) % PALETTE.len()];
                use owo_colors::OwoColorize;
                line.push_str(&format!("{}", prefix.truecolor(r, g, b)));
            } else {
                line.push_str(&prefix);
            }
        } else {
            line.push_str(&prefix);
        }
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
        }
    }

    #[test]
    fn formats_prefix_and_message_no_color() {
        let f = DefaultLineFormatter {
            timestamp_style: TimestampStyle::Omit,
            timestamp_zone: TimestampZone::Utc,
            color_enabled: false,
        };
        assert_eq!(f.format_line(&ev()), "p/ctn | hello\n");
    }

    #[tokio::test]
    async fn render_task_writes_lines_and_flush_on_shutdown() {
        let (mut rd, wr) = duplex(4096);
        let (tx, rx) = mpsc::channel::<RenderCommand>(8);
        let f = DefaultLineFormatter {
            timestamp_style: TimestampStyle::Omit,
            timestamp_zone: TimestampZone::Utc,
            color_enabled: false,
        };
        let h = tokio::spawn(async move {
            render_task(rx, wr, Arc::new(f)).await.unwrap();
        });
        tx.send(RenderCommand::Line(ev())).await.unwrap();
        tx.send(RenderCommand::Shutdown).await.unwrap();
        h.await.unwrap();
        let mut buf = vec![0u8; 64];
        let n = rd.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"p/ctn | hello\n");
    }
}
