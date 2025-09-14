# waitq

*Experimental, not ready to be used*

No-std, no-alloc async waker queue optimized for `!Send` tasks. Tasks using `waitq` waiters will be `!Send` and so cannot be used with work-stealing executors such as Tokio unless they are spawned inside e.g. a [`LocalSet`](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html).

# License

MIT
