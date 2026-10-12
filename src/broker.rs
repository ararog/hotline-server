use crossfire::{MAsyncRx, MAsyncTx, mpmc, mpsc};
use hotline_core::{Command, HotlineError, PublishCommand, SubscribeCommand, SystemMessage};
use std::collections::HashMap;

pub struct Broker {
    subscribers: HashMap<String, Vec<MAsyncTx<mpsc::Array<Command>>>>,
    command_receiver: MAsyncRx<mpmc::Array<SystemMessage>>,
}

impl Broker {
    pub fn new(command_receiver: MAsyncRx<mpmc::Array<SystemMessage>>) -> Self {
        Self {
            subscribers: HashMap::new(),
            command_receiver,
        }
    }

    async fn publish(&mut self, command: PublishCommand) -> Result<(), HotlineError> {
        let topic = command.topic();
        if let Some(subscribers) = self.subscribers.get_mut(topic) {
            for subscriber in subscribers {
                subscriber.send(Command::Publish(command.clone())).await;
            }
        }

        Ok(())
    }

    async fn subscribe(
        &mut self,
        command: SubscribeCommand,
        sender: Option<MAsyncTx<mpsc::Array<Command>>>,
    ) -> Result<(), HotlineError> {
        let topic = command.topic();

        let Some(sender) = sender else {
            return Err(HotlineError("Destination not set".to_string()));
        };

        if let Some(subscribers) = self.subscribers.get_mut(topic) {
            subscribers.push(sender);
        } else {
            let mut new_subscribers = Vec::new();
            new_subscribers.push(sender);
            self.subscribers.insert(topic.to_string(), new_subscribers);
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), HotlineError> {
        while let Ok(message) = self.command_receiver.recv().await {
            let (sender, command) = message.into_parts();
            match command {
                Command::Publish(data) => match self.publish(data).await {
                    Ok(_) => println!("Message published"),
                    Err(e) => {
                        println!("Could not publish message: {}", e.0.to_string())
                    }
                },
                Command::Subscribe(data) => match self.subscribe(data, sender).await {
                    Ok(_) => println!("Message published"),
                    Err(e) => {
                        println!("Could not publish message: {}", e.0.to_string())
                    }
                },
            }
        }
        Ok(())
    }
}
