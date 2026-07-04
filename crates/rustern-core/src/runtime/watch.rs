use std::sync::Arc;

use futures::{Stream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::watcher::Event;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::mux::MuxCmd;
use super::pod_lifecycle::PodLifecycle;
use super::watch_ctx::PodWatchCtx;

pub(crate) fn spawn_watch_task<S>(
    w: S,
    root_w: CancellationToken,
    mux_tx_w: mpsc::Sender<MuxCmd>,
    watch_ctx: Arc<PodWatchCtx>,
) -> JoinHandle<()>
where
    S: Stream<Item = Result<Event<Pod>, kube::runtime::watcher::Error>> + Send + 'static,
{
    tokio::spawn(async move {
        let mut lifecycle = PodLifecycle::new();
        tokio::pin!(w);
        loop {
            tokio::select! {
                _ = root_w.cancelled() => break,
                ev = w.next() => {
                    let Some(ev) = ev else { break };
                    let ev = match ev {
                        Ok(e) => e,
                        Err(e) => { tracing::warn!(?e, "watch"); continue; }
                    };
                    match ev {
                        Event::Delete(pod) => {
                            lifecycle
                                .on_delete(&pod, watch_ctx.as_ref(), &mux_tx_w)
                                .await;
                        }
                        Event::Apply(pod) => {
                            lifecycle
                                .on_apply(&pod, &watch_ctx, &mux_tx_w)
                                .await;
                        }
                        Event::Init => {
                            lifecycle.on_init_begin(watch_ctx.as_ref()).await;
                        }
                        Event::InitApply(pod) => {
                            lifecycle.on_init_apply(pod, watch_ctx.as_ref()).await;
                        }
                        Event::InitDone => {
                            lifecycle.on_init_done(&watch_ctx).await;
                        }
                    }
                }
            }
        }
        drop(mux_tx_w);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::test_support::TestOrchestratorBuilder;
    use crate::source::ContextName;
    use crate::source::SourceKey;
    use futures::stream;
    use k8s_openapi::api::core::v1::{
        Container, ContainerState, ContainerStateRunning, ContainerStatus, PodSpec, PodStatus,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    use std::time::Duration;

    fn sample_key(container: &str, uid: &str) -> SourceKey {
        SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: container.into(),
            uid: uid.into(),
        }
    }

    fn test_pod(name: &str, uid: &str, containers: &[&str]) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("ns".into()),
                uid: Some(uid.into()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: containers
                    .iter()
                    .map(|n| Container {
                        name: n.to_string(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }),
            status: Some(PodStatus {
                container_statuses: Some(
                    containers
                        .iter()
                        .map(|n| ContainerStatus {
                            name: n.to_string(),
                            ready: true,
                            state: Some(ContainerState {
                                running: Some(ContainerStateRunning::default()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .collect(),
                ),
                phase: Some("Running".into()),
                ..Default::default()
            }),
        }
    }

    async fn run_scripted_events(
        events: Vec<Event<Pod>>,
    ) -> (Arc<PodWatchCtx>, mpsc::Receiver<MuxCmd>) {
        let root = CancellationToken::new();
        let (mux_tx, mux_rx) = mpsc::channel(16);
        let fixture = TestOrchestratorBuilder::new()
            .mux_tx(mux_tx.clone())
            .build();
        let ctx = fixture.arc();
        let w = stream::iter(events.into_iter().map(Ok));
        let h = spawn_watch_task(w, root, mux_tx, Arc::clone(&ctx));
        h.await.expect("watch task panicked");
        (ctx, mux_rx)
    }

    fn drain_mux(rx: &mut mpsc::Receiver<MuxCmd>) -> (usize, usize) {
        let mut adds = 0;
        let mut removes = 0;
        while let Ok(cmd) = rx.try_recv() {
            match cmd {
                MuxCmd::Add(..) => adds += 1,
                MuxCmd::Remove(_) => removes += 1,
            }
        }
        (adds, removes)
    }

    async fn wait_for_attach_mux(rx: &mut mpsc::Receiver<MuxCmd>, expected_adds: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let mut total_adds = 0;
        loop {
            let (adds, _) = drain_mux(rx);
            total_adds += adds;
            if total_adds >= expected_adds {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for {expected_adds} mux attach(es)");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn init_events_reconcile_snapshot() {
        let events = vec![
            Event::Init,
            Event::InitApply(test_pod("pod-a", "uid-a", &["app"])),
            Event::InitApply(test_pod("pod-b", "uid-b", &["app"])),
            Event::InitDone,
        ];
        let (_ctx, mut mux_rx) = run_scripted_events(events).await;
        wait_for_attach_mux(&mut mux_rx, 2).await;
    }

    #[tokio::test]
    async fn apply_event_attaches_admitted_pod() {
        let events = vec![Event::Apply(test_pod("pod-a", "uid-a", &["app"]))];
        let (_ctx, mut mux_rx) = run_scripted_events(events).await;
        wait_for_attach_mux(&mut mux_rx, 1).await;
    }

    #[tokio::test]
    async fn delete_event_drops_streams() {
        let pod = test_pod("pod-a", "uid-a", &["app"]);
        let events = vec![Event::Apply(pod.clone()), Event::Delete(pod.clone())];
        let (ctx, mut mux_rx) = run_scripted_events(events).await;
        let (_, removes) = drain_mux(&mut mux_rx);
        assert_eq!(removes, 1);
        assert_eq!(
            ctx.attach
                .pod_meta
                .lookup(&sample_key("app", "uid-a"))
                .await,
            Default::default()
        );
    }

    #[tokio::test]
    async fn init_done_prunes_stale_pod_meta() {
        let events = vec![
            Event::Init,
            Event::InitApply(test_pod("pod-a", "uid-a", &["app"])),
            Event::InitApply({
                let mut idle = test_pod("idle", "uid-idle", &["app"]);
                idle.status = None;
                idle
            }),
            Event::InitDone,
        ];
        let (ctx, mut mux_rx) = run_scripted_events(events).await;
        wait_for_attach_mux(&mut mux_rx, 1).await;
        let stale = SourceKey {
            pod: "idle".into(),
            uid: "uid-idle".into(),
            container: "app".into(),
            ..sample_key("app", "uid-idle")
        };
        assert_eq!(ctx.attach.pod_meta.lookup(&stale).await, Default::default());
    }
}
