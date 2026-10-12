use crossfire::{MAsyncTx, mpmc, mpsc};
use hotline_core::{Command, HotlineError, SystemMessage};
use wtransport::RecvStream;

pub struct Writer {
    registry_sender: MAsyncTx<mpmc::Array<SystemMessage>>,
}

impl Writer {
    pub fn new(registry_sender: MAsyncTx<mpmc::Array<SystemMessage>>) -> Self {
        Self { registry_sender }
    }

    pub async fn run(
        &self,
        mut recv_stream: RecvStream,
        reader_sender: MAsyncTx<mpsc::Array<Command>>,
    ) -> Result<(), HotlineError> {
        let mut buf = [0u8; 8192];
        while let Ok(Some(size)) = recv_stream.read(&mut buf).await
            && size > 0
        {
            let command: Command = bitcode::decode(&buf).unwrap();
            let _ = match command {
                Command::Subscribe(command) => {
                    let message = SystemMessage::new(Command::Subscribe(command))
                        .with_sender(reader_sender.clone());
                    self.registry_sender.send(message).await
                }
                _ => self.registry_sender.send(SystemMessage::new(command)).await,
            };
        }

        Ok(())
    }
}
