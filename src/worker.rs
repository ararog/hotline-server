use crate::{reader::Reader, writer::Writer};
use crossfire::{MAsyncRx, MAsyncTx, mpmc, mpsc, spsc};
use hotline_core::{Command, HotlineError, SystemMessage};
use wtransport::{RecvStream, SendStream};

pub struct Worker {
    id: usize,
    stream_receiver: MAsyncRx<mpmc::Array<(SendStream, RecvStream)>>,
    command_sender: MAsyncTx<mpmc::Array<SystemMessage>>,
}

impl Worker {
    pub fn new(
        id: usize,
        stream_receiver: MAsyncRx<mpmc::Array<(SendStream, RecvStream)>>,
        command_sender: MAsyncTx<mpmc::Array<SystemMessage>>,
    ) -> Self {
        Self {
            id,
            stream_receiver,
            command_sender,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub async fn run(&self) -> Result<(), HotlineError> {
        while let Ok((send_stream, recv_stream)) = self.stream_receiver.recv().await {
            let (reader_send, reader_recv) = mpsc::bounded_async::<Command>(2000);

            let reader = Reader::new();
            tokio::spawn(async move {
                reader.run(send_stream, reader_recv).await;
            });

            let writer = Writer::new(self.command_sender.clone());
            tokio::spawn(async move {
                writer.run(recv_stream, reader_send.clone()).await;
            });
        }
        Ok(())
    }
}
