#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicU64;

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicU64;

#[cfg(loom)]
pub(crate) use loom::sync::atomic::Ordering;

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::Ordering;

#[cfg(loom)]
pub(crate) use loom::sync::Arc;

#[cfg(not(loom))]
pub(crate) use std::sync::Arc;
