pub mod message;
pub mod thread_pool;
pub mod worker;

pub type Job = Box<dyn FnOnce() + Send + 'static>;

#[cfg(test)]
mod tests {
    use crate::thread_pool::ThreadPool;

    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn test_counts() {
        let pool = ThreadPool::new(10);
        let counter = Arc::new(Mutex::new(0));

        for _ in 0..100 {
            let counter = Arc::clone(&counter);
            pool.execute(move || {
                let mut num = counter.lock().unwrap();
                *num += 1;
            });
        }

        drop(pool);

        assert_eq!(*counter.lock().unwrap(), 100);
    }

    #[test]
    fn test_atom_count() {
        let pool = ThreadPool::new(10);
        let num = Arc::new(AtomicUsize::new(0));

        for _ in 0..100 {
            let num = Arc::clone(&num);
            pool.execute(move || {
                num.fetch_add(1, Ordering::SeqCst);
            });
        }

        drop(pool);

        assert_eq!(num.load(Ordering::SeqCst), 100);
    }
}
