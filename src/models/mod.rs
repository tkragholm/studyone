//! Domain models for study population framework
//!
//! This module contains the core entity models used throughout the application.
//! These models represent the domain entities for the study design as outlined
//! in the `STUDY_FLOW.md` document.

// Common traits and types
pub mod traits;
pub mod types;
// The registry module has been moved to common/traits/registry
pub mod collections;

// Entity models
pub mod child;
pub mod diagnosis;
pub mod family;
pub mod income;
pub mod individual;
pub mod parent;

// Re-export commonly used traits
pub use traits::{
    ArrowSchema, EntityModel, Filterable, HealthStatus, ModelCollection, 
    TemporalValidity
};

// Re-export registry-specific traits from common module
pub use crate::common::traits::{
    RegistryAware, BefRegistry, DodRegistry, IndRegistry, LprRegistry, MfrRegistry,
};

// Re-export common types
pub use types::{
    DiseaseOrigin, DiseaseSeverity, DiagnosisType, EducationLevel,
    FamilyType, Gender, JobSituation, Origin, ScdCategory,
};

// Re-export commonly used entity models
pub use child::Child;
pub use diagnosis::Diagnosis;
pub use family::Family;
pub use income::Income;
pub use individual::Individual;
pub use parent::Parent;

// Re-export model collections
pub use child::ChildCollection;
pub use diagnosis::DiagnosisCollection;
pub use family::FamilyCollection;
pub use parent::ParentCollection;
pub use collections::IndividualCollection;