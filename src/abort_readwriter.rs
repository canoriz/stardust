use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncRead;

struct AbortableReader<T> {
    r: T,
}

enum Result {
    Finish(io::Result),
    Abort(AbortDetail),
}

struct AbortDetail {}

impl<T> Future for AbortableReader<T>
where
    T: AsyncRead,
{
    type Output = Result;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.poll_read(cx)
    }
}

impl<T> AbortableReader<T>
where
    T: AsyncRead,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result> {
        todo!()
    }
}
