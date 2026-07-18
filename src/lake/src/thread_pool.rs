use std::sync::{
    Arc, Mutex,
    mpsc::{self, Sender},
};

use crate::{message::Message, worker::Worker};

pub struct ThreadPool {
    pub workers: Vec<Worker>,
    pub sender: Option<Sender<Message>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        let (tx, rx) = mpsc::channel::<Message>();

        let rx = Arc::new(Mutex::new(rx));
        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&rx)));
        }

        ThreadPool {
            workers,
            sender: Some(tx),
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        if let Some(ref tx) = self.sender {
            tx.send(Message::NewJob(job)).unwrap();
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            for _ in 0..self.workers.len() {
                let _ = sender.send(Message::Terminate);
            }
        }

        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}
