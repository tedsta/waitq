//! `no_std`, no-alloc implementation of an async wait queue using an intrusive linked list.

#![no_std]

use core::{
    cell::{Cell, UnsafeCell},
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll, Waker},
};

pub trait IFulfillment {
    fn take_one(&mut self) -> Self;

    fn append(&mut self, other: Self, other_count: usize);
}

impl IFulfillment for () {
    fn take_one(&mut self) -> Self {
        ()
    }

    fn append(&mut self, _other: Self, _other_count: usize) {}
}

pub struct Fulfillment<T> {
    pub count: usize,
    pub inner: T,
}

impl<T: IFulfillment> Fulfillment<T> {
    pub fn take_one(&mut self) -> T {
        self.count -= 1;
        self.inner.take_one()
    }

    pub fn append(&mut self, other: Self) {
        self.inner.append(other.inner, other.count);
        self.count += other.count;
    }
}

pub struct WaiterQueue<T> {
    state: spin::Mutex<WaiterQueueState<T>>,
    local: thid::ThreadLocal<Local<T>>,
}

struct WaiterQueueState<T> {
    front: Option<NonNull<WaiterNode<T>>>,
    back: Option<NonNull<WaiterNode<T>>>,
    count: usize,
}

struct WaiterNode<T> {
    state: spin::Mutex<WaiterNodeState<T>>,

    // SAFETY: These can only be accessed while the WaiterQueue lock is held.
    previous: UnsafeCell<Option<NonNull<Self>>>,
    next: UnsafeCell<Option<NonNull<Self>>>,

    // SAFETY: These can only be accessed from the awaiting task's thread, and that task cannot be
    // `Send`.
    local_lifecycle: Cell<WaiterLifecycle>,
    local_state: UnsafeCell<WaiterNodeState<T>>,
    local_next: Cell<Option<NonNull<Self>>>,
    local_prev: Cell<Option<NonNull<Self>>>,
}

enum WaiterNodeState<T> {
    Pending,
    Polled { waker: Waker },
    Notified { fulfillment: Fulfillment<T> },
    Releasing,
}

unsafe impl<T> Send for WaiterQueue<T> {}
unsafe impl<T> Sync for WaiterQueue<T> {}

impl<T: IFulfillment> WaiterNode<T> {
    pub fn new() -> Self {
        Self {
            previous: UnsafeCell::new(None),
            next: UnsafeCell::new(None),
            state: spin::Mutex::new(WaiterNodeState::Pending),
            local_lifecycle: Cell::new(WaiterLifecycle::Unregistered),
            local_state: UnsafeCell::new(WaiterNodeState::Pending),
            local_next: Cell::new(None),
            local_prev: Cell::new(None),
        }
    }

    #[inline]
    fn with_state<R>(&self, f: impl FnOnce(&mut WaiterNodeState<T>) -> R) -> R {
        f(&mut self.state.lock())
    }

    #[inline]
    fn fulfill(&self, fulfillment: Fulfillment<T>) -> Option<Waker> {
        self.with_state(|state| Self::fulfill_common(state, fulfillment))
    }

    #[inline]
    fn fulfill_local(&self, fulfillment: Fulfillment<T>) -> Option<Waker> {
        let state = unsafe { &mut *self.local_state.get() };
        Self::fulfill_common(state, fulfillment)
    }

    #[inline]
    fn fulfill_common(
        state: &mut WaiterNodeState<T>,
        fulfillment: Fulfillment<T>,
    ) -> Option<Waker> {
        match state {
            WaiterNodeState::Pending => {
                *state = WaiterNodeState::Notified { fulfillment };
                None
            }
            WaiterNodeState::Polled { .. } => {
                let WaiterNodeState::Polled { waker } =
                    core::mem::replace(&mut *state, WaiterNodeState::Notified { fulfillment })
                else {
                    unreachable!();
                };
                Some(waker)
            }
            // A WaiterNode can be notified exactly once.
            WaiterNodeState::Notified {
                fulfillment: existing_fulfillment,
            } => {
                existing_fulfillment.append(fulfillment);
                None
            }
            // A WaiterNode shouldn't be reachable after it's been dropped.
            WaiterNodeState::Releasing => unreachable!(),
        }
    }
}

struct Local<T> {
    nodes: Cell<Option<(NonNull<WaiterNode<T>>, NonNull<WaiterNode<T>>)>>,
    count: Cell<usize>,
}

unsafe impl<T> Send for Local<T> {}

impl<T> Default for Local<T> {
    fn default() -> Self {
        Self {
            nodes: Cell::new(None),
            count: Cell::new(0),
        }
    }
}

impl<T> Local<T> {
    #[inline]
    fn add_node(&self, new_node: NonNull<WaiterNode<T>>) {
        self.count.set(self.count.get() + 1);
        if let Some((head, tail)) = self.nodes.get() {
            unsafe { new_node.as_ref() }.local_prev.set(Some(tail));
            unsafe { tail.as_ref() }.local_next.set(Some(new_node));
            self.nodes.set(Some((head, new_node)));
        } else {
            // List is empty
            self.nodes.set(Some((new_node, new_node)));
            debug_assert_eq!(self.count.get(), 1);
        }
    }

    #[inline]
    fn remove_node(&self, to_remove: &WaiterNode<T>) {
        let Some((head, tail)) = self.nodes.get() else {
            // List is empty, so to_remove is definitely not in the list.
            return;
        };

        let prev = to_remove.local_prev.replace(None);
        let next = to_remove.local_next.replace(None);

        if prev.is_none() && next.is_none() && head != NonNull::from(to_remove) {
            // This node is not in the local list
            return;
        }
        self.count.set(self.count.get() - 1);

        if let Some(next) = next {
            unsafe { next.as_ref() }.local_prev.set(prev);
        } else {
            // This is the tail node.
            debug_assert_eq!(NonNull::from(to_remove), tail);
            if let Some(prev) = prev {
                unsafe { prev.as_ref() }.local_next.set(None);
                self.nodes.set(Some((head, prev)));
            } else {
                // This is the only node.
                debug_assert_eq!(head, tail);
                debug_assert_eq!(self.count.get(), 0);
                self.nodes.set(None);
            }
            return;
        }

        if let Some(prev) = prev {
            unsafe { prev.as_ref() }.local_next.set(next);
        } else {
            // This is the head node.
            debug_assert_eq!(NonNull::from(to_remove), head);
            if let Some(next) = next {
                self.nodes.set(Some((next, tail)));
            } else {
                // This is the only node.
                debug_assert_eq!(head, tail);
                debug_assert_eq!(self.count.get(), 0);
                self.nodes.set(None);
                return;
            }
        }
    }

    #[inline]
    fn pop_node(&self) -> Option<NonNull<WaiterNode<T>>> {
        let (head, tail) = self.nodes.take()?;
        self.count.set(self.count.get() - 1);
        if head != tail {
            let new_head = unsafe { head.as_ref() }.local_next.take().unwrap();
            unsafe { new_head.as_ref() }.local_prev.set(None);
            self.nodes.set(Some((new_head, tail)));
        } else {
            debug_assert_eq!(self.count.get(), 0);
        }

        Some(head)
    }
}

pub struct WaiterQueueGuard<'a, T> {
    state: spin::MutexGuard<'a, WaiterQueueState<T>>,
}

impl<T> WaiterQueueGuard<'_, T> {
    pub fn waiter_count(&self) -> usize {
        self.state.count
    }
}

impl<T: IFulfillment> WaiterQueue<T> {
    pub fn new() -> Self {
        Self {
            state: spin::Mutex::new(WaiterQueueState {
                front: None,
                back: None,
                count: 0,
            }),
            local: thid::ThreadLocal::new(),
        }
    }

    pub fn lock(&self) -> WaiterQueueGuard<'_, T> {
        WaiterQueueGuard {
            state: self.state.lock(),
        }
    }

    pub fn notify_one_local(&self, fulfillment: T) -> Option<T> {
        let local = self.local.get_or_default();
        let Some((local_head, _)) = local.nodes.get() else {
            return Some(fulfillment);
        };

        debug_assert!(unsafe { local_head.as_ref() }.local_prev.get().is_none());
        debug_assert_eq!(
            unsafe { local_head.as_ref() }.local_lifecycle.get(),
            WaiterLifecycle::Registered,
        );

        let fulfillment = Fulfillment {
            inner: fulfillment,
            count: 1,
        };

        let mut guard = self.lock();
        if guard.remove_waiter(local_head) {
            // This waiter hasn't been notified yet. Convert it to a local registration and
            // fulfill it. Also upgrade the next waiter to be in the shared queue.

            local.pop_node();

            if let Some((new_head, _)) = local.nodes.get() {
                Self::upgrade_local_waiter(&mut guard, new_head);
            }
            drop(guard);

            unsafe { local_head.as_ref() }
                .local_lifecycle
                .set(WaiterLifecycle::RegisteredLocal);
            if let Some(waker) = unsafe { local_head.as_ref() }.fulfill_local(fulfillment) {
                waker.wake();
            }
        } else {
            // This waiter was already notified by another thread but hasn't been polled yet. We
            // can tack this fulfillment on - when the waiter is polled, the extra fulfillments
            // will be used to notify any local waiters, and the next local waiter in line will be
            // upgraded to the shared queue.

            drop(guard);

            if let Some(waker) = unsafe { local_head.as_ref() }.fulfill(fulfillment) {
                waker.wake();
            }
        }

        None
    }

    fn remove_local_waiter(&self, to_remove: &WaiterNode<T>) {
        let local = self.local.get_or_default();
        local.remove_node(to_remove);
    }

    fn upgrade_local_waiter(guard: &mut WaiterQueueGuard<'_, T>, waiter: NonNull<WaiterNode<T>>) {
        debug_assert_eq!(
            unsafe { waiter.as_ref() }.local_lifecycle.get(),
            WaiterLifecycle::RegisteredLocal,
        );

        // Before registering the waiter in the shared queue, set its shared state from
        // its local state in case it was already polled (so that the waker is properly set).
        let waiter_ref = unsafe { waiter.as_ref() };
        *waiter_ref.state.lock() = match unsafe { &*waiter_ref.local_state.get() } {
            WaiterNodeState::Pending => WaiterNodeState::Pending,
            WaiterNodeState::Polled { waker } => WaiterNodeState::Polled {
                waker: waker.clone(),
            },
            WaiterNodeState::Notified { .. } => unreachable!(),
            WaiterNodeState::Releasing => unreachable!(),
        };

        guard.add_waiter(waiter);
        waiter_ref.local_lifecycle.set(WaiterLifecycle::Registered);
    }
}

impl WaiterQueue<()> {
    #[inline]
    pub fn notify_all(&self) -> usize {
        self.lock().notify_all(())
    }

    /// # Safety
    ///
    /// You must cancel the returned waiter before it is dropped.
    #[inline]
    pub unsafe fn wait(&self) -> Waiter<'_, ()> {
        Waiter::new(&self)
    }

    #[inline]
    pub async fn wait_for<R>(&self, mut condition: impl FnMut() -> Option<R>) -> R {
        if let Some(r) = condition() {
            return r;
        }

        let result = Cell::new(None);
        loop {
            let wait_until = core::pin::pin!(WaitUntil {
                // SAFETY: WaitUntil::drop cancels the Waiter if necessary.
                waiter: unsafe { self.wait() },
                condition: UnsafeCell::new(|| {
                    if let Some(r) = condition() {
                        result.set(Some(r));
                        true
                    } else {
                        false
                    }
                }),
            });
            core::future::poll_fn(|cx| wait_until.as_ref().poll(cx)).await;

            if let Some(r) = result.take() {
                return r;
            }
        }
    }

    #[inline]
    pub async fn wait_until(&self, condition: impl Fn() -> bool) {
        let wait_until = core::pin::pin!(WaitUntil {
            // SAFETY: WaitUntil::drop cancels the Waiter if necessary.
            waiter: unsafe { self.wait() },
            condition: UnsafeCell::new(condition),
        });
        core::future::poll_fn(|cx| wait_until.as_ref().poll(cx)).await;
    }
}

impl<T: IFulfillment> WaiterQueueGuard<'_, T> {
    pub fn notify(mut self, fulfillment: T, count: usize) -> Option<Fulfillment<T>> {
        let Some(front_ptr) = self.state.front else {
            // There are currently no waiters.
            return Some(Fulfillment {
                count,
                inner: fulfillment,
            });
        };

        self.state.count -= 1;

        // Advance the front cursor
        let next_ptr = core::mem::replace(unsafe { &mut *front_ptr.as_ref().next.get() }, None);
        self.state.front = next_ptr;

        if let Some(new_front_ptr) = self.state.front {
            unsafe { *new_front_ptr.as_ref().previous.get() = None };
        } else {
            debug_assert_eq!(Some(front_ptr), self.state.back);
            debug_assert!(unsafe { *front_ptr.as_ref().previous.get() }.is_none());

            // We've reached the end of the waiter list - clear the `back` pointer.
            self.state.back = None;
        }
        // Release the waiter queue lock before waking.
        drop(self);

        let maybe_waker = unsafe { front_ptr.as_ref() }.fulfill(Fulfillment {
            inner: fulfillment,
            count,
        });
        if let Some(waker) = maybe_waker {
            waker.wake();
        }

        None
    }

    /// Returns true if the waiter was removed.
    /// Returns false if the waiter had already been removed before this call.
    fn remove_waiter(&mut self, node: NonNull<WaiterNode<T>>) -> bool {
        let prev = unsafe { *node.as_ref().previous.get() };
        let next = unsafe { *node.as_ref().next.get() };

        if prev.is_none() && next.is_none() && self.state.front != Some(node) {
            // This waiter has already been removed.
            return false;
        }

        self.state.count -= 1;

        unsafe {
            *node.as_ref().next.get() = None;
        }
        unsafe {
            *node.as_ref().previous.get() = None;
        }

        // Check if we are removing the back node, move `back` pointer to earlier waiter in line.
        if Some(node) == self.state.back {
            self.state.back = prev;
            debug_assert!(next.is_none());
        }

        if Some(node) == self.state.front {
            // We are removing the front node
            self.state.front = next;
            if let Some(next) = next {
                // SAFETY: the shared lock protects access to all `previous` values.
                unsafe {
                    *next.as_ref().previous.get() = None;
                }
            } else {
                debug_assert!(self.state.back.is_none());
            }
        } else if let Some(prev) = prev {
            // Previous node is guaranteed not to be the back node, and we have the shared lock, so
            // we have exclusive access to `next`.
            unsafe { *prev.as_ref().next.get() = next };
            if let Some(next) = next {
                // SAFETY: the shared lock protects access to all `previous` values.
                unsafe {
                    *next.as_ref().previous.get() = Some(prev);
                }
            }
        }

        true
    }

    fn add_waiter(&mut self, new_node: NonNull<WaiterNode<T>>) {
        let state = &mut self.state;
        state.count += 1;

        debug_assert!(unsafe { (*new_node.as_ref().next.get()).is_none() });
        debug_assert!(unsafe { (*new_node.as_ref().previous.get()).is_none() });

        let prev_back = core::mem::replace(&mut state.back, Some(new_node));
        if let Some(prev_back) = prev_back {
            unsafe {
                // Set my node's previous node.
                *new_node.as_ref().previous.get() = Some(prev_back);
                // Link my node as next after the previous `back`.
                *prev_back.as_ref().next.get() = Some(new_node);
            }
        } else {
            // We are the first in line - set the queue's front.
            state.front = Some(new_node);
            debug_assert!(unsafe { &*new_node.as_ref().next.get() }.is_none());
            debug_assert!(unsafe { &*new_node.as_ref().previous.get() }.is_none());
        }
    }
}

impl<T: IFulfillment + Copy> WaiterQueueGuard<'_, T> {
    pub fn notify_all(&mut self, fulfillment: T) -> usize {
        let mut notified_count = 0;

        while let Some(front_ptr) = self.state.front {
            notified_count += 1;
            self.state.count -= 1;

            // Advance the front cursor
            let next_ptr = core::mem::replace(unsafe { &mut *front_ptr.as_ref().next.get() }, None);
            self.state.front = next_ptr;

            if let Some(new_front_ptr) = self.state.front {
                unsafe { *new_front_ptr.as_ref().previous.get() = None };
            } else {
                debug_assert_eq!(Some(front_ptr), self.state.back);
                debug_assert!(unsafe { *front_ptr.as_ref().previous.get() }.is_none());

                // We've reached the end of the waiter list - clear the `back` pointer.
                self.state.back = None;
            }

            let maybe_waker = unsafe { front_ptr.as_ref() }.fulfill(Fulfillment {
                inner: fulfillment,
                count: usize::MAX,
            });
            if let Some(waker) = maybe_waker {
                waker.wake();
            }
        }

        notified_count
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum WaiterLifecycle {
    Unregistered,
    Registered,
    RegisteredLocal,
    Releasing,
}

pub struct Waiter<'a, T: IFulfillment> {
    waiter_queue: &'a WaiterQueue<T>,
    waiter_node: UnsafeCell<WaiterNode<T>>,
}

impl<'a, T: IFulfillment> Waiter<'a, T> {
    pub fn new(waiter_queue: &'a WaiterQueue<T>) -> Self {
        Self {
            waiter_queue,
            waiter_node: UnsafeCell::new(WaiterNode::new()),
        }
    }

    #[inline]
    fn lifecycle(&self) -> WaiterLifecycle {
        unsafe { &*self.waiter_node.get() }.local_lifecycle.get()
    }

    #[inline]
    fn set_lifecycle(&self, new_value: WaiterLifecycle) {
        unsafe { &*self.waiter_node.get() }
            .local_lifecycle
            .set(new_value);
    }

    #[inline]
    fn register(
        self: Pin<&Self>,
        mut try_fulfill: impl FnMut() -> Option<Fulfillment<T>>,
    ) -> Option<Fulfillment<T>> {
        if self.lifecycle() != WaiterLifecycle::Unregistered {
            return None;
        }

        let local = self.waiter_queue.local.get_or_default();
        let waiter_node_ptr = NonNull::from(unsafe { &*self.waiter_node.get() });

        if local.nodes.get().is_some() {
            self.set_lifecycle(WaiterLifecycle::RegisteredLocal);
            local.add_node(waiter_node_ptr);
            None
        } else {
            // Try to fulfill the waiter with the waiter queue locked before registering it.
            let mut guard = self.waiter_queue.lock();
            if let Some(fulfillment) = try_fulfill() {
                drop(guard);
                Some(fulfillment)
            } else {
                guard.add_waiter(waiter_node_ptr);
                self.set_lifecycle(WaiterLifecycle::Registered);
                local.add_node(waiter_node_ptr);
                None
            }
        }
    }

    pub fn cancel(&self) -> Option<Fulfillment<T>> {
        match self.lifecycle() {
            WaiterLifecycle::Registered => {
                self.set_lifecycle(WaiterLifecycle::Releasing);

                // Lock waker queue to prevent this node from being notified if it wasn't already notified.
                let mut waiter_queue_guard = self.waiter_queue.lock();

                let waiter_node = unsafe { &*self.waiter_node.get() };
                let mut state = waiter_node.state.lock();
                match core::mem::replace(&mut *state, WaiterNodeState::Releasing) {
                    WaiterNodeState::Notified { fulfillment } => {
                        self.waiter_queue.remove_local_waiter(waiter_node);
                        Some(fulfillment)
                    }
                    // Fulfillment was already processed, so this waiter has already been deregistered.
                    WaiterNodeState::Releasing => None,
                    _ => {
                        // Deregister the waiter.
                        waiter_queue_guard.remove_waiter(NonNull::from(waiter_node));
                        self.waiter_queue.remove_local_waiter(waiter_node);
                        None
                    }
                }
            }
            WaiterLifecycle::RegisteredLocal => {
                self.set_lifecycle(WaiterLifecycle::Releasing);

                let waiter_node = unsafe { &*self.waiter_node.get() };
                let state = unsafe { &mut *waiter_node.local_state.get() };
                match core::mem::replace(&mut *state, WaiterNodeState::Releasing) {
                    WaiterNodeState::Notified { fulfillment } => {
                        // Local waiters are deregistered immediately when fulfilled, so don't need to deregister here.
                        Some(fulfillment)
                    }
                    // Fulfillment was already processed, so this waiter has already been deregistered.
                    WaiterNodeState::Releasing => None,
                    _ => {
                        // Deregister the waiter.
                        self.waiter_queue.remove_local_waiter(waiter_node);
                        None
                    }
                }
            }
            _ => None,
        }
    }

    pub fn poll_fulfillment(
        self: Pin<&'_ Self>,
        context: &'_ mut Context<'_>,
        mut try_fulfill: impl FnMut() -> Option<Fulfillment<T>>,
    ) -> Poll<Fulfillment<T>> {
        if let Some(fulfillment) = self.as_ref().register(&mut try_fulfill) {
            return Poll::Ready(fulfillment);
        }

        let waiter_node = unsafe { &*self.waiter_node.get() };

        let update_state = |state: &mut WaiterNodeState<T>| {
            let mut maybe_fulfillment = None;
            let state_ptr = &mut *state as *mut WaiterNodeState<T>;
            let taken_state = unsafe { core::ptr::read(state_ptr) };

            // SAFETY: the match block below must not panic.
            let new_state = match taken_state {
                WaiterNodeState::Pending => WaiterNodeState::Polled {
                    waker: context.waker().clone(),
                },
                WaiterNodeState::Polled { waker } => {
                    let new_waker = context.waker();
                    if !waker.will_wake(new_waker) {
                        WaiterNodeState::Polled {
                            waker: new_waker.clone(),
                        }
                    } else {
                        WaiterNodeState::Polled { waker }
                    }
                }
                WaiterNodeState::Notified { fulfillment } => {
                    maybe_fulfillment = Some(fulfillment);
                    WaiterNodeState::Releasing
                }
                WaiterNodeState::Releasing => unreachable!(),
            };

            unsafe {
                state_ptr.write(new_state);
            }

            maybe_fulfillment
        };

        // Always poll the local state - a waiter can be notified locally even if it is registered
        // in the shared queue.
        let local_state = unsafe { &mut *waiter_node.local_state.get() };
        if let Some(fulfillment) = update_state(local_state) {
            //debug_assert_eq!(fulfillment.count, 1);
            debug_assert_eq!(self.lifecycle(), WaiterLifecycle::RegisteredLocal);
            self.set_lifecycle(WaiterLifecycle::Releasing);
            return Poll::Ready(fulfillment);
        }

        if self.as_ref().lifecycle() == WaiterLifecycle::Registered {
            // This waiter is registered in the shared queue.

            if let Some(mut fulfillment) = waiter_node.with_state(update_state) {
                let waiter_queue_local = self.as_ref().waiter_queue.local.get_or_default();
                let popped_head = waiter_queue_local.pop_node();
                debug_assert_eq!(popped_head, Some(NonNull::from(waiter_node)));

                self.set_lifecycle(WaiterLifecycle::Releasing);

                // Use extra fulfillments to notify any local waiters.
                while fulfillment.count > 1 {
                    let Some(local_next) = waiter_queue_local.pop_node() else {
                        break;
                    };
                    debug_assert!(unsafe { local_next.as_ref() }.local_prev.get().is_none());
                    debug_assert_eq!(
                        unsafe { local_next.as_ref() }.local_lifecycle.get(),
                        WaiterLifecycle::RegisteredLocal,
                    );

                    if let Some(waker) = unsafe { local_next.as_ref() }.fulfill_local(Fulfillment {
                        inner: fulfillment.take_one(),
                        count: 1,
                    }) {
                        waker.wake();
                    }
                }

                // Upgrade the next local waiter to be in the shared queue.
                if let Some((local_head, local_tail)) = waiter_queue_local.nodes.get() {
                    let mut guard = self.as_ref().waiter_queue.lock();

                    // Acquire as many fulfillments as possible with the shared queue locked.
                    while let Some(new_fulfillment) = try_fulfill() {
                        fulfillment.append(new_fulfillment);
                        if fulfillment.count > waiter_queue_local.count.get() {
                            break;
                        }
                    }

                    if fulfillment.count == 1 {
                        // Can't notify any additional local waiters - upgrade the next local
                        // waiter.
                        WaiterQueue::<T>::upgrade_local_waiter(&mut guard, local_head);
                        drop(guard);
                    } else if fulfillment.count > waiter_queue_local.count.get() {
                        // Notify all local waiters

                        drop(guard);
                        while let Some(next_local) = waiter_queue_local.pop_node() {
                            let local_fulfillment = Fulfillment {
                                inner: fulfillment.take_one(),
                                count: 1,
                            };
                            if let Some(waker) =
                                unsafe { next_local.as_ref() }.fulfill_local(local_fulfillment)
                            {
                                waker.wake();
                            }
                        }
                    } else {
                        // Upgrade as many local waiters as possible and upgrade the new local head
                        // to be in the shared queue.

                        let notify_count = fulfillment.count - 1;
                        let mut cursor = local_head;
                        for _ in 0..notify_count - 1 {
                            cursor = unsafe { cursor.as_ref() }
                                .local_next
                                .get()
                                .expect("bug: missing local waiter");
                        }

                        let new_head = unsafe { cursor.as_ref() }
                            .local_next
                            .replace(None)
                            .expect("bug: missing local waiter");
                        unsafe { new_head.as_ref() }.local_prev.set(None);
                        waiter_queue_local.nodes.set(Some((new_head, local_tail)));
                        waiter_queue_local
                            .count
                            .set(waiter_queue_local.count.get() - notify_count);

                        // Upgrade the new local head to be in the shared queue and release the
                        // shared queue lock.
                        WaiterQueue::<T>::upgrade_local_waiter(&mut guard, new_head);
                        drop(guard);

                        let mut wake_cursor = Some(local_head);
                        while let Some(next) = wake_cursor {
                            let local_fulfillment = Fulfillment {
                                inner: fulfillment.take_one(),
                                count: 1,
                            };
                            if let Some(waker) =
                                unsafe { next.as_ref() }.fulfill_local(local_fulfillment)
                            {
                                waker.wake();
                            }
                            unsafe { next.as_ref() }.local_prev.set(None);
                            wake_cursor = unsafe { next.as_ref() }.local_next.replace(None);
                        }
                    }
                }

                return Poll::Ready(fulfillment);
            }
        }

        Poll::Pending
    }
}

pub struct WaitUntil<'a, F> {
    waiter: Waiter<'a, ()>,
    condition: UnsafeCell<F>,
}

impl<F> WaitUntil<'_, F>
where
    F: FnMut() -> bool,
{
    // Miri is unhappy with `self: Pin<&mut Self>` even if we do `Pin::as_ref(self)` first thing. So
    // we can't impl Future. So we do a custom poll method and use poll_fn instead.
    fn poll(self: Pin<&Self>, context: &mut Context<'_>) -> Poll<()> {
        // SAFETY: we continue to treat `self.waiter` as pinned, and `self.condition` is never
        // considered pinned.
        let unpinned_self = unsafe { Pin::into_inner_unchecked(self) };
        let waiter = unsafe { Pin::new_unchecked(&unpinned_self.waiter) };
        // SAFETY: This is the only place that dereferences `self.condition`.
        let condition = unsafe { &mut *unpinned_self.condition.get() };

        let Poll::Ready(fulfillment) = waiter.poll_fulfillment(context, || {
            if condition() {
                Some(Fulfillment {
                    inner: (),
                    count: usize::MAX,
                })
            } else {
                None
            }
        }) else {
            return Poll::Pending;
        };

        Poll::Ready(fulfillment.inner)
    }
}

impl<F> Drop for WaitUntil<'_, F> {
    fn drop(&mut self) {
        // TODO we can't just drop already sent fulfillments on the floor.
        let _ = self.waiter.cancel();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_add_remove_local_node() {
        let a = WaiterNode::new();
        let b = WaiterNode::new();
        let c = WaiterNode::new();

        let a_ptr = NonNull::from(&a);
        let b_ptr = NonNull::from(&b);
        let c_ptr = NonNull::from(&c);

        let local = Local::<()>::default();

        local.add_node(a_ptr);
        local.add_node(b_ptr);
        local.add_node(c_ptr);

        assert_eq!(local.nodes.get(), Some((a_ptr, c_ptr)));
        assert_eq!(a.local_prev.get(), None);
        assert_eq!(a.local_next.get(), Some(b_ptr));
        assert_eq!(b.local_prev.get(), Some(a_ptr));
        assert_eq!(b.local_next.get(), Some(c_ptr));
        assert_eq!(c.local_prev.get(), Some(b_ptr));
        assert_eq!(c.local_next.get(), None);

        local.remove_node(&b);

        assert_eq!(local.nodes.get(), Some((a_ptr, c_ptr)));
        assert_eq!(a.local_prev.get(), None);
        assert_eq!(a.local_next.get(), Some(c_ptr));
        assert_eq!(c.local_prev.get(), Some(a_ptr));
        assert_eq!(c.local_next.get(), None);

        local.remove_node(&a);

        assert_eq!(local.nodes.get(), Some((c_ptr, c_ptr)));
        assert_eq!(c.local_prev.get(), None);
        assert_eq!(c.local_next.get(), None);

        local.remove_node(&c);

        assert_eq!(local.nodes.get(), None);
    }

    #[test]
    fn test_add_waiter() {
        let waiter_queue = WaiterQueue::<()>::new();

        let a = WaiterNode::new();
        let b = WaiterNode::new();
        let c = WaiterNode::new();

        let a_ptr = NonNull::from(&a);
        let b_ptr = NonNull::from(&b);
        let c_ptr = NonNull::from(&c);

        let mut guard = waiter_queue.lock();

        guard.add_waiter(a_ptr);
        guard.add_waiter(b_ptr);
        guard.add_waiter(c_ptr);

        assert!(guard.remove_waiter(b_ptr));
        assert!(guard.remove_waiter(a_ptr));
        assert!(guard.remove_waiter(c_ptr));

        assert!(!guard.remove_waiter(a_ptr));
        assert!(!guard.remove_waiter(b_ptr));
        assert!(!guard.remove_waiter(c_ptr));
    }

    #[test]
    fn test_register_waiter() {
        let waiter_queue = WaiterQueue::<()>::new();

        let a = core::pin::pin!(Waiter::new(&waiter_queue));
        let b = core::pin::pin!(Waiter::new(&waiter_queue));
        let c = core::pin::pin!(Waiter::new(&waiter_queue));

        a.as_ref().register(|| None);
        b.as_ref().register(|| None);
        c.as_ref().register(|| None);

        assert!(b.cancel().is_none());
        assert!(a.cancel().is_none());
        assert!(c.cancel().is_none());
    }
}
