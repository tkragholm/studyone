//! Common utilities and traits
//!
//! This module provides shared functionality that is used across
//! different parts of the codebase, helping to avoid circular dependencies.

pub mod traits;

// Re-export common traits for easier imports
pub use traits::*;