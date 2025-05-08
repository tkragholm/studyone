//! Diagnosis model collection
//!
//! This module provides a specialized collection implementation for Diagnosis models.

use crate::collections::GenericCollection;
use crate::common::traits::{ModelCollection, LookupCollection};
use crate::models::diagnosis::{Diagnosis, ScdResult};
use std::collections::HashMap;
use std::sync::Arc;

/// Specialized collection for Diagnosis models
#[derive(Debug, Default)]
pub struct DiagnosisCollection {
    /// Base generic collection implementation
    inner: GenericCollection<Diagnosis>,
    /// Diagnoses by individual PNR for efficient lookup
    diagnoses_by_pnr: HashMap<String, Vec<Arc<Diagnosis>>>,
    /// SCD results by individual PNR
    scd_results: HashMap<String, ScdResult>,
}

impl DiagnosisCollection {
    /// Create a new empty diagnosis collection
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: GenericCollection::new(),
            diagnoses_by_pnr: HashMap::new(),
            scd_results: HashMap::new(),
        }
    }
    
    /// Create a collection from a vector of diagnoses
    #[must_use]
    pub fn from_diagnoses(diagnoses: Vec<Diagnosis>) -> Self {
        let mut collection = Self::new();
        for diagnosis in diagnoses {
            collection.add(diagnosis);
        }
        collection
    }
    
    /// Get all diagnoses for an individual
    #[must_use]
    pub fn get_diagnoses(&self, pnr: &str) -> Vec<Arc<Diagnosis>> {
        self.diagnoses_by_pnr.get(pnr).cloned().unwrap_or_default()
    }
    
    /// Add an SCD result
    pub fn add_scd_result(&mut self, result: ScdResult) {
        self.scd_results.insert(result.pnr.clone(), result);
    }
    
    /// Get SCD result for an individual
    #[must_use]
    pub fn get_scd_result(&self, pnr: &str) -> Option<&ScdResult> {
        self.scd_results.get(pnr)
    }
    
    /// Get all individuals with SCD
    #[must_use]
    pub fn individuals_with_scd(&self) -> Vec<String> {
        self.scd_results
            .iter()
            .filter(|(_, result)| result.has_scd)
            .map(|(pnr, _)| pnr.clone())
            .collect()
    }
    
    /// Get all individuals without SCD
    #[must_use]
    pub fn individuals_without_scd(&self) -> Vec<String> {
        self.scd_results
            .iter()
            .filter(|(_, result)| !result.has_scd)
            .map(|(pnr, _)| pnr.clone())
            .collect()
    }
    
    /// Get individuals with a specific SCD category
    #[must_use]
    pub fn individuals_with_category(&self, category: u8) -> Vec<String> {
        self.scd_results
            .iter()
            .filter(|(_, result)| result.has_category(category))
            .map(|(pnr, _)| pnr.clone())
            .collect()
    }
    
    /// Count individuals with SCD
    #[must_use]
    pub fn scd_count(&self) -> usize {
        self.individuals_with_scd().len()
    }
    
    /// Count individuals by severity level
    #[must_use]
    pub fn count_by_severity(&self) -> HashMap<i32, usize> {
        let mut counts = HashMap::new();
        
        for result in self.scd_results.values() {
            if result.has_scd {
                let severity = result.combined_severity();
                *counts.entry(severity).or_insert(0) += 1;
            }
        }
        
        counts
    }
    
    /// Get the raw collection
    #[must_use]
    pub fn raw(&self) -> &GenericCollection<Diagnosis> {
        &self.inner
    }
    
    /// Get a mutable reference to the raw collection
    pub fn raw_mut(&mut self) -> &mut GenericCollection<Diagnosis> {
        &mut self.inner
    }
}

impl ModelCollection<Diagnosis> for DiagnosisCollection {
    fn add(&mut self, diagnosis: Diagnosis) {
        let pnr = diagnosis.individual_pnr.clone();
        let diagnosis_arc = Arc::new(diagnosis);
        
        // Add to main collection
        self.inner.add((*diagnosis_arc).clone());
        
        // Add to by-PNR index
        self.diagnoses_by_pnr
            .entry(pnr)
            .or_default()
            .push(diagnosis_arc);
    }
    
    fn get(&self, id: &(String, String)) -> Option<Arc<Diagnosis>> {
        let (pnr, code) = id;
        
        if let Some(diagnoses) = self.diagnoses_by_pnr.get(pnr) {
            diagnoses.iter()
                .find(|diag| diag.diagnosis_code == *code)
                .cloned()
        } else {
            None
        }
    }
    
    fn all(&self) -> Vec<Arc<Diagnosis>> {
        self.inner.all()
    }
    
    fn filter<F>(&self, predicate: F) -> Vec<Arc<Diagnosis>>
    where
        F: Fn(&Diagnosis) -> bool,
    {
        self.inner.filter(predicate)
    }
    
    fn count(&self) -> usize {
        self.inner.count()
    }
}

impl LookupCollection<Diagnosis> for DiagnosisCollection {
    // Use the default implementations from the trait
}