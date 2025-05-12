//! Individual entity model
//!
//! This module defines the consolidated Individual entity that represents a person in the study.

// Re-export the main Individual struct and related types
pub use self::consolidated::Individual;
pub use self::consolidated::Role;

// Core implementation
pub mod consolidated;

// Legacy modules for backward compatibility
// These can be gradually removed as code transitions to the consolidated model
//mod base;
//mod conversion;

mod relationships;
//mod serde;
mod temporal;
