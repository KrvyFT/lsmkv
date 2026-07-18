use std::{
    sync::{Arc, Mutex, mpsc::Receiver},
    thread::{self, JoinHandle},
};

use crate::message::Message;

pub struct Worker {
    pub id: usize,
    pub thread: Option<JoinHandle<()>>,
}

impl Worker {
    pub fn new(id: usize, revice: Arc<Mutex<Receiver<Message>>>) -> Self {
        let thread = thread::spawn(move || {
            loop {
                let result = revice.lock().unwrap().recv();
                match result {
                    Ok(Message::NewJob(job)) => {
                        job();
                    }
                    Ok(Message::Terminate) => {
                        break;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}
