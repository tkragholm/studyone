//! Registry-specific model conversion traits
//!
//! This module contains traits for converting registry data into domain models.
//! Each registry has specific schema layouts and data formats, and these traits
//! provide standardized interfaces for converting that data.

use crate::error::Result;

use arrow::record_batch::RecordBatch;

/// Trait for models that are aware of registry-specific details
///
/// `RegistryAware` provides methods for constructing models from
/// specific registry schemas, understanding the column layout and
/// data formats of each registry.
pub trait RegistryAware: Sized {
    /// Get the registry name for this model
    fn registry_name() -> &'static str;
    
    /// Create a model from a registry-specific record
    fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>>;
    
    /// Create models from an entire registry record batch
    fn from_registry_batch(batch: &RecordBatch) -> Result<Vec<Self>>;
}

/// Trait for converting BEF registry data to domain models
pub trait BefRegistry: RegistryAware {
    /// Convert a BEF record to a model
    fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>>
    where
        Self: Sized;

    /// Convert a BEF batch to model collection
    fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Self>>
    where
        Self: Sized;
}

/// Trait for converting IND registry data to domain models
pub trait IndRegistry: RegistryAware {
    /// Convert an IND record to a model
    fn from_ind_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>>
    where
        Self: Sized;

    /// Convert an IND batch to model collection
    fn from_ind_batch(batch: &RecordBatch) -> Result<Vec<Self>>
    where
        Self: Sized;
}

/// Trait for converting LPR registry data to domain models
pub trait LprRegistry: RegistryAware {
    /// Convert an LPR record to a model
    fn from_lpr_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>>
    where
        Self: Sized;

    /// Convert an LPR batch to model collection
    fn from_lpr_batch(batch: &RecordBatch) -> Result<Vec<Self>>
    where
        Self: Sized;
}

/// Trait for converting MFR registry data to domain models
pub trait MfrRegistry: RegistryAware {
    /// Convert an MFR record to a model
    fn from_mfr_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>>
    where
        Self: Sized;

    /// Convert an MFR batch to model collection
    fn from_mfr_batch(batch: &RecordBatch) -> Result<Vec<Self>>
    where
        Self: Sized;
}

/// Trait for converting DOD registry data to domain models
pub trait DodRegistry: RegistryAware {
    /// Enhance a model with death registry data
    fn enhance_with_death_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool>;
}

/// Trait for converting AKM registry data to domain models
pub trait AkmRegistry: RegistryAware {
    /// Convert an AKM record to a model
    fn from_akm_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>>
    where
        Self: Sized;

    /// Convert an AKM batch to model collection
    fn from_akm_batch(batch: &RecordBatch) -> Result<Vec<Self>>
    where
        Self: Sized;
    
    /// Enhance an existing model with employment data from AKM
    fn enhance_with_employment_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool>;
}

/// Trait for converting UDDF registry data to domain models
pub trait UddfRegistry: RegistryAware {
    /// Convert a UDDF record to a model
    fn from_uddf_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>>
    where
        Self: Sized;

    /// Convert a UDDF batch to model collection
    fn from_uddf_batch(batch: &RecordBatch) -> Result<Vec<Self>>
    where
        Self: Sized;
    
    /// Enhance an existing model with education data from UDDF
    fn enhance_with_education_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool>;
}

/// Trait for converting VNDS registry data to domain models
pub trait VndsRegistry: RegistryAware {
    /// Convert a VNDS record to a model
    fn from_vnds_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>>
    where
        Self: Sized;

    /// Convert a VNDS batch to model collection
    fn from_vnds_batch(batch: &RecordBatch) -> Result<Vec<Self>>
    where
        Self: Sized;
    
    /// Enhance an existing model with migration data from VNDS
    fn enhance_with_migration_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool>;
}