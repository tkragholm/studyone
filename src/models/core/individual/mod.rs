//! Individual entity model
//!
//! This module defines the consolidated Individual entity that represents a person in the study.

// Re-export the main Individual struct and related types
pub use self::consolidated::Individual;
pub use self::consolidated::Role;
pub use self::temporal::TimePeriod;

// Core implementation
pub mod consolidated;
pub mod implementations;

mod relationships;
pub mod temporal;
