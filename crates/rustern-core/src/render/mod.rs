//! Single writer task + flush ticker (50ms).

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::source::LogEvent;

pub mod default_renderer;
pub mod ext_json_renderer;
pub(crate) mod highlight;
pub mod json_renderer;
pub mod raw_renderer;

pub trait LineFormatter: Send + Sync + 'static {
    fn format_line(&self, event: &LogEvent) -> String;
}

pub enum RenderCommand {
    Line(LogEvent),
    Flush,
    Shutdown,
}

pub async fn render_task<W>(
    mut rx: mpsc::Receiver<RenderCommand>,
    writer: W,
    formatter: Arc<dyn LineFormatter>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let mut buf = BufWriter::new(writer);
    while let Some(cmd) = rx.recv().await {
        match cmd {
            RenderCommand::Line(ev) => {
                let line = formatter.format_line(&ev);
                buf.write_all(line.as_bytes()).await?;
            }
            RenderCommand::Flush => {
                buf.flush().await?;
            }
            RenderCommand::Shutdown => {
                buf.flush().await?;
                break;
            }
        }
    }
    Ok(())
}

pub async fn flush_ticker(
    tx: mpsc::Sender<RenderCommand>,
    root_token: CancellationToken,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = root_token.cancelled() => break,
            _ = ticker.tick() => {
                let _ = tx.try_send(RenderCommand::Flush);
            }
        }
    }
}
