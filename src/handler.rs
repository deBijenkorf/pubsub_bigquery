use uuid::Uuid;

pub struct MessageCounter {
    pub max_messages: u32,
    pub current_messages: u32,
    pub current_file: String,
}

impl MessageCounter {
    pub fn new(max_messages: u32) -> Self {
        MessageCounter {
            max_messages,
            current_messages: 0,
            current_file: Uuid::new_v4().to_string(),
        }
    }

    pub fn increase(&mut self, amount: u32) {
        self.current_messages += amount;
    }

    pub fn reached_threshold(&self) -> bool {
        self.current_messages >= self.max_messages
    }

    pub fn reset(&mut self) {
        self.current_messages = 0;
        self.current_file = Uuid::new_v4().to_string();
    }
}

pub trait Handler {
    fn handle(&mut self, messages: Vec<String>);
}