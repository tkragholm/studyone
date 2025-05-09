//! Domain models for study population framework
//!
//! This module contains the core entity models used throughout the application.
//! These models represent the domain entities for the study design as outlined
//! in the `STUDY_FLOW.md` document.

// New hierarchical modules
pub mod core;
pub mod derived;
pub mod health;
pub mod economic;
pub mod collections;

// Re-export commonly used traits for backward compatibility
pub use core::traits::{
    ArrowSchema, EntityModel, Filterable, HealthStatus, TemporalValidity
};

// Re-export registry-specific traits from common module
pub use crate::common::traits::{
    BefRegistry, DodRegistry, IndRegistry, LprRegistry, MfrRegistry, RegistryAware,
};

// Re-export common types for backward compatibility
pub use core::types::{
    CitizenshipStatus, DiseaseOrigin, DiseaseSeverity, DiagnosisType, EducationField,
    EducationLevel, FamilyType, Gender, HousingType, JobSituation, MaritalStatus,
    Origin, ScdCategory, SocioeconomicStatus,
};

// Re-export commonly used entity models for backward compatibility
pub use core::individual::Individual;
pub use derived::Child;
pub use derived::Family;
pub use derived::Parent;
pub use health::Diagnosis;
pub use economic::Income;

// Re-export model collections for backward compatibility
pub use derived::child::ChildCollection;
pub use health::diagnosis::DiagnosisCollection;
pub use derived::family::FamilyCollection;
pub use derived::parent::ParentCollection;
pub use collections::IndividualCollection;

// Old module paths
// These will be deprecated and eventually removed,
// but are kept for backward compatibility during migration
#[deprecated(
    since = "0.2.0",
    note = "Use models::core::traits instead. Will be removed in a future version."
)]
pub mod traits {
    pub use crate::models::core::traits::*;
}

#[deprecated(
    since = "0.2.0",
    note = "Use models::core::types instead. Will be removed in a future version."
)]
pub mod types {
    pub use crate::models::core::types::*;
}

#[deprecated(
    since = "0.2.0",
    note = "Use models::derived::child instead. Will be removed in a future version."
)]
pub mod child {
    pub use crate::models::derived::child::*;
}

#[deprecated(
    since = "0.2.0",
    note = "Use models::derived::family instead. Will be removed in a future version."
)]
pub mod family {
    pub use crate::models::derived::family::*;
}

#[deprecated(
    since = "0.2.0",
    note = "Use models::derived::parent instead. Will be removed in a future version."
)]
pub mod parent {
    pub use crate::models::derived::parent::*;
}

#[deprecated(
    since = "0.2.0",
    note = "Use models::health::diagnosis instead. Will be removed in a future version."
)]
pub mod diagnosis {
    pub use crate::models::health::diagnosis::*;
}

#[deprecated(
    since = "0.2.0",
    note = "Use models::economic::income instead. Will be removed in a future version."
)]
pub mod income {
    pub use crate::models::economic::income::*;
}

#[deprecated(
    since = "0.2.0",
    note = "Use models::core::individual instead. Will be removed in a future version."
)]
pub mod individual {
    pub use crate::models::core::individual::*;
}