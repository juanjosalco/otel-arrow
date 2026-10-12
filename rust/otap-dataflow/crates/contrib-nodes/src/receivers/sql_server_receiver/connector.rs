// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Verified TLS connection establishment without blocking the pipeline's local executor.

use super::{SqlServerConnectionConfig, SqlServerConnector, SqlServerError as Error};
use async_trait::async_trait;
use std::{fs::File, io::Read, path::PathBuf};
use tiberius::{Client, Config, EncryptionLevel};
use tokio::{
    net::TcpStream,
    runtime::Handle,
    sync::{oneshot, watch},
};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::instrument::WithSubscriber;

const MAX_CA_BYTES: u64 = 4 * 1024 * 1024;

/// TCP/TLS connector using the process-installed Rustls crypto provider.
///
/// Tiberius loads platform roots synchronously inside its async handshake. One
/// bounded, adapter-owned thread handles connection establishment only, using
/// the existing Tokio reactor. After handoff, all query I/O remains local.
/// A dropped connect future requests cancellation; shutdown confirms completion.
#[derive(Default)]
pub struct TcpConnector {
    worker: Option<ConnectionWorker>,
    cleanup_failed: bool,
}

struct ConnectionWorker {
    cancel: watch::Sender<bool>,
    exited: oneshot::Receiver<()>,
}

impl Drop for ConnectionWorker {
    fn drop(&mut self) {
        _ = self.cancel.send_replace(true);
    }
}

struct CancelOnDrop(watch::Sender<bool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        _ = self.0.send_replace(true);
    }
}

impl TcpConnector {
    async fn finish(&mut self) -> Result<(), Error> {
        if self.cleanup_failed {
            return Err(Error::Driver);
        }
        if let Some(worker) = self.worker.as_mut()
            && (&mut worker.exited).await.is_err()
        {
            self.cleanup_failed = true;
            self.worker = None;
            return Err(Error::Driver);
        }
        self.worker = None;
        Ok(())
    }
}

#[async_trait(?Send)]
impl SqlServerConnector for TcpConnector {
    type Stream = Compat<TcpStream>;

    async fn connect(
        &mut self,
        endpoint: &SqlServerConnectionConfig,
        mut config: Config,
    ) -> Result<Client<Self::Stream>, Error> {
        if !otel_arrow_dfe_otap::crypto::is_crypto_provider_installed() {
            return Err(Error::CryptoProvider);
        }
        // Join an interrupted attempt before admitting another connection.
        self.shutdown().await?;
        let host = endpoint.host().to_owned();
        let port = endpoint.port();
        let ca_file = endpoint.ca_file().map(PathBuf::from);
        config.encryption(EncryptionLevel::Required);
        let reactor = Handle::current();
        let (cancel, mut cancelled) = watch::channel(false);
        let (result_tx, result_rx) = oneshot::channel();
        let (exited_tx, exited) = oneshot::channel();
        let guard = CancelOnDrop(cancel.clone());

        // This thread isolates the driver's synchronous platform CA loading.
        // Socket registration uses the pipeline reactor, not a second runtime.
        // There is at most one attempt/thread per connector and no queued jobs.
        _ = std::thread::Builder::new()
            .name("sql-server-connect".into())
            .spawn(move || {
                let result = {
                    let _runtime = reactor.enter();
                    futures::executor::block_on(
                        async {
                            tokio::select! {
                                biased;
                                _ = cancelled.changed() => Err(Error::Cancelled),
                                result = establish(host, port, ca_file, config) => result,
                            }
                        }
                        .with_subscriber(tracing::subscriber::NoSubscriber::default()),
                    )
                };
                // Failed delivery drops the session here, before the exit acknowledgement.
                drop(result_tx.send(result));
                _ = exited_tx.send(());
            })
            .map_err(|_| Error::Driver)?;
        self.worker = Some(ConnectionWorker { cancel, exited });
        let result = result_rx.await.map_err(|_| Error::Driver)?;
        self.finish().await?;
        drop(guard);
        result
    }

    async fn shutdown(&mut self) -> Result<(), Error> {
        if let Some(worker) = &self.worker {
            _ = worker.cancel.send_replace(true);
        }
        self.finish().await
    }
}

async fn establish(
    host: String,
    port: u16,
    ca_file: Option<PathBuf>,
    mut config: Config,
) -> Result<Client<Compat<TcpStream>>, Error> {
    if let Some(path) = ca_file {
        let mut bytes = Vec::new();
        _ = File::open(path)
            .map_err(|_| Error::Tls)?
            .take(MAX_CA_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::Tls)?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_CA_BYTES {
            return Err(Error::Tls);
        }
        config.trust_cert_ca_bundle(bytes);
    }
    let stream = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|error| Error::Transport(error.kind()))?;
    stream
        .set_nodelay(true)
        .map_err(|error| Error::Transport(error.kind()))?;
    Client::connect(config, stream.compat_write())
        .await
        .map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Scenario: A worker exits without acknowledging cleanup, such as after a panic.
    /// Guarantees: Repeated shutdown attempts stay failed without polling a completed oneshot again.
    #[tokio::test]
    async fn unconfirmed_exit_remains_failed() {
        let (cancel, _cancelled) = watch::channel(false);
        let (tx, exited) = oneshot::channel();
        drop(tx);
        let mut connector = TcpConnector {
            worker: Some(ConnectionWorker { cancel, exited }),
            cleanup_failed: false,
        };
        assert_eq!(connector.shutdown().await, Err(Error::Driver));
        assert_eq!(connector.shutdown().await, Err(Error::Driver));
    }

    /// Scenario: A TCP peer accepts the handshake but never answers prelogin.
    /// Guarantees: Dropping the connect future cancels and joins its owned worker without blocking the core.
    #[tokio::test]
    async fn abandoned_handshake_stops_owned_worker() {
        otel_arrow_dfe_otap::crypto::ensure_crypto_provider();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint: SqlServerConnectionConfig = serde_json::from_value(serde_json::json!({
            "host":"127.0.0.1", "port":address.port(), "database":"fixture"
        }))
        .unwrap();
        let mut connector = TcpConnector::default();
        let mut config = Config::new();
        config.host("127.0.0.1");
        config.port(address.port());
        let (connection, ()) = tokio::join!(
            tokio::time::timeout(
                Duration::from_millis(100),
                connector.connect(&endpoint, config)
            ),
            async {
                let (socket, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(200)).await;
                drop(socket);
            },
        );
        assert!(connection.is_err());
        tokio::time::timeout(Duration::from_secs(2), connector.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(connector.worker.is_none());
    }
}
