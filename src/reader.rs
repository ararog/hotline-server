use crossfire::{AsyncRx, mpsc};
use hotline_core::{Command, HotlineError};
use wtransport::SendStream;

pub struct Reader {}

impl Reader {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn run(
        &self,
        mut send_stream: SendStream,
        receiver: AsyncRx<mpsc::Array<Command>>,
    ) -> Result<(), HotlineError> {
        while let Ok(command) = receiver.recv().await {
            //
        }
        Ok(())
    }
}
