//! Domain models for study population framework
//!
//! This module contains the core entity models used throughout the application.
//! These models represent the domain entities for the study design as outlined
//! in the `STUDY_FLOW.md` document.

// New hierarchical modules
pub mod collections;
pub mod core;
pub mod derived;
pub mod economic;
pub mod health;

// Re-export commonly used traits for backward compatibility
pub use core::traits::{ArrowSchema, EntityModel, Filterable, HealthStatus, TemporalValidity};

// Re-export registry-specific traits from common module
pub use crate::common::traits::{
    BefRegistry, DodRegistry, IndRegistry, LprRegistry, MfrRegistry, RegistryAware,
};

// Re-export common types for backward compatibility
pub use core::types::{
    CitizenshipStatus, DiagnosisType, DiseaseOrigin, DiseaseSeverity, EducationLevel, FamilyType,
    Gender, HousingType, JobSituation, MaritalStatus, Origin, ScdCategory, SocioeconomicStatus,
};

// Re-export commonly used entity models for backward compatibility
pub use core::individual::Individual;
pub use derived::Child;
pub use derived::Family;
pub use derived::Parent;
pub use economic::Income;
pub use health::Diagnosis;

// Re-export model collections for backward compatibility
pub use crate::collections::IndividualCollection;
pub use derived::child::ChildCollection;
pub use derived::family::FamilyCollection;
pub use derived::parent::ParentCollection;
pub use health::diagnosis::DiagnosisCollection;
