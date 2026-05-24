use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::mux::MuxCmd;
use super::watch::PodWatchCtx;
use crate::source::pod_log::PodLogSource;
use crate::source::{ContextName, Labels, LogSource, SourceKey, SourceKind, SourceMeta};

struct AttachPodLogParams {
    ctx: Arc<PodWatchCtx>,
    meta: SourceMeta,
    pod_token: CancellationToken,
    key: SourceKey,
}

fn source_meta_for_key(context_name: &ContextName, key: &SourceKey) -> SourceMeta {
    SourceMeta {
        context: context_name.clone(),
        namespace: key.namespace.clone(),
        pod: key.pod.clone(),
        container: key.container.clone(),
        kind: SourceKind::PodLog,
        node: None,
        labels: Arc::new(Labels::default()),
        uid: key.uid.clone(),
    }
}

async fn attach_pod_log_stream(p: AttachPodLogParams) {
    let permit = if p.ctx.pod_log.follow {
        match Arc::clone(&p.ctx.sem).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                if let Some(tx) = &p.ctx.follow_limit_notifier {
                    let _ = tx.try_send(());
                }
                p.ctx.root_child.cancel();
                return;
            }
        }
    } else {
        match Arc::clone(&p.ctx.sem).acquire_owned().await {
            Ok(p) => p,
            Err(_) => return,
        }
    };

    let client = p.ctx.client.clone();
    match PodLogSource::start(client, p.meta, p.pod_token, p.ctx.pod_log.clone()).await {
        Ok(src) => {
            let stream = Box::new(src).into_stream();
            let _ = p.ctx.mux_tx.send(MuxCmd::Add(p.key, stream)).await;
        }
        Err(e) => tracing::warn!(?e, "pod log start"),
    }
    drop(permit);
}

pub(crate) fn spawn_attach_pod_log(
    ctx: &Arc<PodWatchCtx>,
    key: SourceKey,
    pod_token: CancellationToken,
) {
    let meta = source_meta_for_key(&ctx.context_name, &key);
    tokio::spawn(attach_pod_log_stream(AttachPodLogParams {
        ctx: Arc::clone(ctx),
        meta,
        pod_token,
        key,
    }));
}
