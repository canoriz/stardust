use std::collections::VecDeque;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub struct BufferPool<T> {
    storage: Mutex<VecDeque<T>>,
    semaphore: Arc<Semaphore>,
    capacity: usize,
}

impl<T> fmt::Debug for BufferPool<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferPool")
            .field("capacity", &self.capacity)
            .field("available", &self.semaphore.available_permits())
            .finish()
    }
}

pub struct PooledBuf<T> {
    buf: Option<T>,
    pool: Arc<BufferPool<T>>,
    _permit: OwnedSemaphorePermit, // declared last — dropped last
}

impl<T: fmt::Debug> fmt::Debug for PooledBuf<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.buf.as_ref().unwrap(), f)
    }
}

impl<T: Send + 'static> BufferPool<T> {
    pub fn new<F>(capacity: usize, create: F) -> Arc<Self>
    where
        F: Fn() -> T,
    {
        let storage = (0..capacity).map(|_| create()).collect();
        Arc::new(Self {
            storage: Mutex::new(storage),
            semaphore: Arc::new(Semaphore::new(capacity)),
            capacity,
        })
    }

    pub async fn acquire(self: &Arc<Self>) -> PooledBuf<T> {
        let permit = Arc::clone(&self.semaphore).acquire_owned().await.unwrap();
        let buf = self.storage.lock().unwrap().pop_front().unwrap();
        PooledBuf { buf: Some(buf), pool: Arc::clone(self), _permit: permit }
    }

    pub fn try_acquire(self: &Arc<Self>) -> Option<PooledBuf<T>> {
        let permit = Arc::clone(&self.semaphore).try_acquire_owned().ok()?;
        let buf = self.storage.lock().unwrap().pop_front().unwrap();
        Some(PooledBuf { buf: Some(buf), pool: Arc::clone(self), _permit: permit })
    }

    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T> Drop for PooledBuf<T> {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            match self.pool.storage.lock() {
                Ok(mut g) => g.push_back(buf),
                Err(e) => e.into_inner().push_back(buf),
            }
        }
        // _permit drops here → semaphore releases → next waiter unblocked
    }
}

impl<T> Deref for PooledBuf<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.buf.as_ref().unwrap()
    }
}

impl<T> DerefMut for PooledBuf<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.buf.as_mut().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // Test 1: acquire decrements available, drop restores it
    #[tokio::test]
    async fn acquire_decrements_available_and_drop_restores() {
        let pool = BufferPool::new(2, || vec![0u8; 64]);
        assert_eq!(pool.available(), 2);

        let buf = pool.acquire().await;
        assert_eq!(pool.available(), 1);

        drop(buf);
        assert_eq!(pool.available(), 2);
    }

    // Test 2: task blocked on exhausted pool unblocks when a buffer is returned
    #[tokio::test]
    async fn acquire_waits_when_exhausted_and_unblocks_on_drop() {
        let pool = BufferPool::new(1, || vec![0u8; 64]);
        let buf = pool.acquire().await;

        let pool2 = Arc::clone(&pool);
        let task = tokio::spawn(async move {
            pool2.acquire().await
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(!task.is_finished());

        drop(buf);
        task.await.unwrap();
    }

    // Test 3: try_acquire returns None when pool is exhausted
    #[tokio::test]
    async fn try_acquire_returns_none_when_exhausted() {
        let pool = BufferPool::new(1, || vec![0u8; 64]);
        let _buf = pool.acquire().await;
        assert!(pool.try_acquire().is_none());
    }

    // Test 4: N concurrent tasks each acquire once from N-capacity pool
    #[tokio::test]
    async fn concurrent_tasks_all_succeed_within_capacity() {
        let pool = BufferPool::new(4, || vec![0u8; 64]);

        let handles: Vec<_> = (0..4).map(|_| {
            let pool = Arc::clone(&pool);
            tokio::spawn(async move {
                let _buf = pool.acquire().await;
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            })
        }).collect();

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(pool.available(), 4);
    }
}
