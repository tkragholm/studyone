//! Individual entity model
//!
//! This module defines the core Individual entity that represents a person in the study.
//! The functionality is organized into several submodules for better maintainability.

// Re-export the main Individual struct and related types
pub use self::base::Individual;
pub use self::base::Role;
pub use self::serde::SerdeIndividual;

// Core implementation and submodules
pub mod base; // Basic Individual struct definition and core methods
pub mod conversion; // Serialization and conversion methods
pub mod registry_integration; // Registry data enhancement methods
pub mod relationships; // Family and relationship-related methods
pub mod serde; // Serde integration and field mapping
pub mod temporal; // Validity, age calculation, and temporal methods
