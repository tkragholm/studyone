//! Mapper traits for registry models to health models
//! 
//! This module provides traits and implementations to map registry-specific
//! data models to the standard health domain models.

use crate::models::health::diagnosis::{Diagnosis, DiagnosisCollection};
use crate::models::core::types::DiagnosisType;
use crate::models::collections::collection_traits::ModelCollection;
use std::collections::HashMap;

/// Trait for mapping registry models to health models
pub trait HealthMapper<T> {
    /// Map a registry model to a health model
    fn map_to_health_model(&self, lookup_data: Option<&HashMap<String, String>>) -> Option<T>;
}

/// Trait specifically for mapping to Diagnosis
pub trait DiagnosisMapper {
    /// Convert this registry model to a Diagnosis
    fn to_diagnosis(&self, pnr_lookup: &HashMap<String, String>) -> Option<Diagnosis>;
}

/// Trait for registry models that can be mapped to Diagnosis
pub trait DiagnosisRegistryMapper {
    /// Map a batch of registry records to a `DiagnosisCollection`
    fn map_batch_to_diagnoses(
        records: &[Self],
        pnr_lookup: &HashMap<String, String>,
    ) -> DiagnosisCollection
    where
        Self: Sized + DiagnosisMapper;
}

// Implement DiagnosisRegistryMapper for any type that implements DiagnosisMapper
impl<T: DiagnosisMapper> DiagnosisRegistryMapper for T {
    fn map_batch_to_diagnoses(
        records: &[Self], 
        pnr_lookup: &HashMap<String, String>
    ) -> DiagnosisCollection {
        let mut collection = DiagnosisCollection::new();
        
        for record in records {
            if let Some(diagnosis) = record.to_diagnosis(pnr_lookup) {
                collection.add(diagnosis);
            }
        }
        
        collection
    }
}

/// A simple structure to hold a mapping between registry record numbers and PNRs
#[derive(Debug, Default)]
pub struct RecnumToPnrMap {
    /// Map from RECNUM to PNR values
    pub recnum_to_pnr: HashMap<String, String>,
}

impl RecnumToPnrMap {
    /// Create a new empty mapping
    #[must_use] pub fn new() -> Self {
        Self {
            recnum_to_pnr: HashMap::new(),
        }
    }
    
    /// Add a mapping from RECNUM to PNR
    pub fn add_mapping(&mut self, recnum: String, pnr: String) {
        self.recnum_to_pnr.insert(recnum, pnr);
    }
    
    /// Build a mapping from a batch of `LPR_ADM` records
    pub fn build_from_adm_records<T>(records: &[T]) -> Self 
    where 
        T: RecnumProvider + PnrProvider,
    {
        let mut map = Self::new();
        
        for record in records {
            if let (Some(recnum), Some(pnr)) = (record.record_number(), record.pnr()) {
                map.add_mapping(recnum, pnr);
            }
        }
        
        map
    }
    
    /// Look up a PNR by RECNUM
    #[must_use] pub fn lookup_pnr(&self, recnum: &str) -> Option<&String> {
        self.recnum_to_pnr.get(recnum)
    }
}

/// Trait for registry models that provide a record number
pub trait RecnumProvider {
    /// Get the record number from this registry model
    fn record_number(&self) -> Option<String>;
}

/// Trait for registry models that provide a PNR
pub trait PnrProvider {
    /// Get the PNR from this registry model
    fn pnr(&self) -> Option<String>;
}

/// Helper function to determine diagnosis type
#[must_use] pub fn determine_diagnosis_type(type_code: &str) -> DiagnosisType {
    match type_code {
        "A" => DiagnosisType::Primary,
        "B" => DiagnosisType::Secondary,
        _ => DiagnosisType::Other,
    }
}