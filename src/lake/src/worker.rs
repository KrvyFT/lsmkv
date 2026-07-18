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
                        println!("Worker {id} got a job; executing.");
                        job();
                    }
                    Ok(Message::Terminate) => {
                        println!("Worker {id} received Terminate; shutting down.");
                        break;
                    }
                    Err(_) => {
                        println!("Worker {id} channel disconnected; shutting down.");
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
