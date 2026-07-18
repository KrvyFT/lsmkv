use crate::Job;

pub enum Message {
    NewJob(Job),
    Terminate,
}
