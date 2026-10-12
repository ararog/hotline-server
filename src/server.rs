use crossfire::mpmc::{self};
use hotline_core::{HotlineError, SystemMessage};
use std::time::Duration;
use tokio::task::JoinSet;
use wtransport::{Endpoint, Identity, RecvStream, SendStream, ServerConfig};

use crate::broker::Broker;
use crate::worker::Worker;

pub struct Server {
    num_workers: usize,
    workers: JoinSet<Result<(), HotlineError>>,
}

#[derive(Clone)]
pub struct Message {
    name: String,
}

impl Message {
    pub fn new(name: &str) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Server {
    pub fn new() -> Self {
        Self {
            num_workers: 2,
            workers: JoinSet::new(),
        }
    }

    pub async fn run(&mut self) -> Result<(), HotlineError> {
        let config = ServerConfig::builder()
            .with_bind_default(4433)
            .with_identity(Identity::self_signed(["localhost"]).unwrap())
            .keep_alive_interval(Some(Duration::from_secs(3)))
            .build();

        let server = Endpoint::server(config).map_err(|e| HotlineError(e.to_string()))?;

        let (broker_send, broker_recv) = mpmc::bounded_async::<SystemMessage>(2000);
        let mut broker = Broker::new(broker_recv);
        let _broker_handle = tokio::spawn(async move { broker.run().await });

        let (worker_send, worker_recv) = mpmc::bounded_async::<(SendStream, RecvStream)>(2000);
        for worker_id in 1..=self.num_workers {
            let worker = Worker::new(worker_id, worker_recv.clone(), broker_send.clone());
            self.workers.spawn(async move { worker.run().await });
        }

        loop {
            let incoming_session = server.accept().await;
            let request = incoming_session
                .await
                .map_err(|e| HotlineError(e.to_string()))?;
            let connection = request
                .accept()
                .await
                .map_err(|e| HotlineError(e.to_string()))?;

            let (send, recv) = connection
                .accept_bi()
                .await
                .map_err(|e| HotlineError(e.to_string()))?;

            worker_send.send((send, recv)).await;
        }
    }
}
