//! Domain models for study population framework
//!
//! This module contains the core entity models used throughout the application.
//! These models represent the domain entities for the study design as outlined
//! in the `STUDY_FLOW.md` document.

// Re-export entity models
pub mod child;
pub mod diagnosis;
pub mod family;
pub mod income;
pub mod individual;
pub mod parent;

// Schema-aware model constructors (direct registry integration)
pub mod child_schema_constructors;
pub mod diagnosis_schema_constructors;
pub mod income_schema_constructors;

// Re-export commonly used types
pub use child::Child;
pub use diagnosis::Diagnosis;
pub use family::Family;
pub use income::Income;
pub use individual::Individual;
pub use parent::Parent;
