use hotline_core::HotlineError;
use std::hash::Hash;

pub struct Subscription {
    topic: String,
    name: String,
}

impl Hash for Subscription {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.name.hash(state);
    }
}

impl Eq for Subscription {}

impl PartialEq for Subscription {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name) && self.topic.eq(&other.topic)
    }

    fn ne(&self, other: &Self) -> bool {
        self.name.ne(&other.name) && self.topic.ne(&other.topic)
    }
}

impl Subscription {
    pub fn new(topic: &str, name: &str) -> Self {
        Self {
            topic: topic.into(),
            name: name.into(),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
