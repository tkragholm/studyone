//! Derived models module
//!
//! This module contains models that are derived from the core Individual model,
//! representing specialized roles and groupings.

pub mod child;
pub mod parent;
pub mod family;

pub use child::Child;
pub use family::Family;
pub use parent::Parent;