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
        mut _send_stream: SendStream,
        receiver: AsyncRx<mpsc::Array<Command>>,
    ) -> Result<(), HotlineError> {
        while let Ok(_command) = receiver.recv().await {
            //
        }
        Ok(())
    }
}
