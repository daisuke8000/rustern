use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use kube::Client;
use tokio_util::sync::CancellationToken;

use super::pod_log::{PodLogRequest, PodLogSource};
use super::{LogSource, LogSourceError, SourceMeta};

#[cfg(any(test, feature = "bench"))]
use super::BoxedLogStream;

type OpenLogSourceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn LogSource>, LogSourceError>> + Send + 'a>>;

pub(crate) trait LogSourceOpener: Send + Sync {
    fn open(
        &self,
        meta: Arc<SourceMeta>,
        token: CancellationToken,
        request: PodLogRequest,
    ) -> OpenLogSourceFuture<'_>;
}

pub(crate) struct PodLogSourceOpener {
    client: Client,
}

impl PodLogSourceOpener {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
}

impl LogSourceOpener for PodLogSourceOpener {
    fn open(
        &self,
        meta: Arc<SourceMeta>,
        token: CancellationToken,
        request: PodLogRequest,
    ) -> OpenLogSourceFuture<'_> {
        let client = self.client.clone();
        Box::pin(async move {
            let src = PodLogSource::start(client, meta, token, request).await?;
            Ok(Box::new(src) as Box<dyn LogSource>)
        })
    }
}

#[cfg(any(test, feature = "bench"))]
struct ScriptLogSource {
    meta: Arc<SourceMeta>,
    token: CancellationToken,
    inner: BoxedLogStream,
}

#[cfg(any(test, feature = "bench"))]
impl LogSource for ScriptLogSource {
    fn meta(&self) -> &SourceMeta {
        self.meta.as_ref()
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.token.clone()
    }

    fn into_stream(self: Box<Self>) -> BoxedLogStream {
        self.inner
    }
}

#[cfg(any(test, feature = "bench"))]
pub struct ScriptLogSourceOpener {
    scripts: std::sync::Mutex<Vec<Vec<Result<super::LogEvent, LogSourceError>>>>,
}

#[cfg(any(test, feature = "bench"))]
impl ScriptLogSourceOpener {
    pub fn new(scripts: Vec<Vec<Result<super::LogEvent, LogSourceError>>>) -> Arc<Self> {
        Arc::new(Self {
            scripts: std::sync::Mutex::new(scripts),
        })
    }
}

#[cfg(any(test, feature = "bench"))]
impl LogSourceOpener for ScriptLogSourceOpener {
    fn open(
        &self,
        meta: Arc<SourceMeta>,
        token: CancellationToken,
        _request: PodLogRequest,
    ) -> OpenLogSourceFuture<'_> {
        let script = {
            let mut scripts = self.scripts.lock().expect("script queue");
            if scripts.is_empty() {
                Vec::new()
            } else {
                scripts.remove(0)
            }
        };
        Box::pin(async move {
            let inner: BoxedLogStream = Box::pin(futures::stream::iter(script));
            Ok(Box::new(ScriptLogSource { meta, token, inner }) as Box<dyn LogSource>)
        })
    }
}
