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
pub mod highlight;
pub mod json_renderer;
pub mod raw_renderer;
pub(crate) mod setup;

pub trait LineFormatter: Send + Sync + 'static {
    fn format_into(&self, event: &LogEvent, buf: &mut String);

    fn format_line(&self, event: &LogEvent) -> String {
        let mut buf = String::new();
        self.format_into(event, &mut buf);
        buf
    }
}

const RENDER_WRITER_CAPACITY: usize = 64 * 1024;
const RENDER_RECV_BATCH: usize = 256;

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
    let mut buf = BufWriter::with_capacity(RENDER_WRITER_CAPACITY, writer);
    let mut line_buf = String::new();
    let mut batch = Vec::with_capacity(RENDER_RECV_BATCH);
    loop {
        batch.clear();
        let n = rx.recv_many(&mut batch, RENDER_RECV_BATCH).await;
        if n == 0 {
            buf.flush().await?;
            break;
        }
        for cmd in batch.drain(..) {
            match cmd {
                RenderCommand::Line(ev) => {
                    line_buf.clear();
                    let min_cap = ev.message.len().saturating_add(64);
                    line_buf.reserve(min_cap);
                    formatter.format_into(&ev, &mut line_buf);
                    buf.write_all(line_buf.as_bytes()).await?;
                }
                RenderCommand::Flush => {
                    buf.flush().await?;
                }
                RenderCommand::Shutdown => {
                    buf.flush().await?;
                    return Ok(());
                }
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
