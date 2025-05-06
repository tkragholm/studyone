//! Domain models for study population framework
//!
//! This module contains the core entity models used throughout the application.
//! These models represent the domain entities for the study design as outlined
//! in the STUDY_FLOW.md document.

// Re-export entity models
pub mod individual;
pub mod family;
pub mod parent;
pub mod child;
pub mod diagnosis;
pub mod income;

// Adapters submodule for registry-to-model mapping
pub mod adapters;

// Re-export commonly used types
pub use individual::Individual;
pub use family::Family;
pub use parent::Parent;
pub use child::Child;
pub use diagnosis::Diagnosis;
pub use income::Income;