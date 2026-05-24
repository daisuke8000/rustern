use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamMap;

use crate::source::{BoxedLogStream, LogEvent, LogSourceError, SourceKey};

pub(crate) enum MuxCmd {
    Add(SourceKey, BoxedLogStream),
    Remove(SourceKey),
}

async fn mux_multiplex_loop(
    mut mux_rx: mpsc::Receiver<MuxCmd>,
    raw_event_tx: mpsc::Sender<Result<LogEvent, LogSourceError>>,
) {
    let mut map: StreamMap<SourceKey, BoxedLogStream> = StreamMap::new();
    loop {
        tokio::select! {
            cmd = mux_rx.recv() => {
                match cmd {
                    Some(MuxCmd::Add(key, stream)) => { map.insert(key, stream); }
                    Some(MuxCmd::Remove(key)) => { map.remove(&key); }
                    None => break,
                }
            }
            item = map.next() => {
                if let Some((_k, row)) = item
                    && raw_event_tx.send(row).await.is_err()
                {
                    break;
                }
            }
        }
    }
}

pub(crate) fn spawn_mux_task(
    mux_rx: mpsc::Receiver<MuxCmd>,
    raw_event_tx: mpsc::Sender<Result<LogEvent, LogSourceError>>,
) -> JoinHandle<()> {
    tokio::spawn(mux_multiplex_loop(mux_rx, raw_event_tx))
}
