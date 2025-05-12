//! Core models module
//!
//! This module contains the fundamental model types that form the foundation
//! of the domain model hierarchy.

pub mod individual;
pub mod registry_traits;
pub mod traits;
pub mod types;

pub use individual::{Individual, Role};
pub use registry_traits::*;
pub use traits::*;
pub use types::*;
