//! Collections module
//!
//! This module contains collection types and utilities for managing
//! groups of domain models efficiently.

pub mod collection_traits;
pub mod individual_collection;

pub use collection_traits::*;
pub use individual_collection::IndividualCollection;