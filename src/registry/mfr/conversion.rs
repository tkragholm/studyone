//! MFR registry model conversion implementation
//!
//! This module implements direct conversion between MFR registry data and
//! Child domain models without requiring a separate adapter.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use crate::common::traits::MfrRegistry;
use crate::error::Result;
use crate::models::child::Child;
use crate::models::individual::Individual;
use crate::registry::RegisterLoader;
use crate::registry::mfr::MfrRegister;
use crate::registry::model_conversion::ModelConversion;
use arrow::record_batch::RecordBatch;

// Implement ModelConversion for MfrRegister
impl ModelConversion<Child> for MfrRegister {
    /// Convert MFR registry data to Child domain models
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Child>> {
        // Use the trait implementation from Child (in models/child.rs)
        use crate::common::traits::MfrRegistry;
        Child::from_mfr_batch(batch)
    }

    /// Convert Child domain models back to MFR registry data
    fn from_models(&self, _models: &[Child]) -> Result<RecordBatch> {
        // Converting Child models back to MFR registry format is complex
        // and not currently implemented
        Err(anyhow::anyhow!(
            "Converting Child models to MFR registry format is not yet implemented"
        ))
    }

    /// Apply additional transformations to Child models if needed
    fn transform_models(&self, _models: &mut [Child]) -> Result<()> {
        // No additional transformations needed
        Ok(())
    }
}

/// MFR register with Individual lookup capability for Child model conversion
#[derive(Debug)]
pub struct MfrChildRegister {
    /// Base register
    base_register: MfrRegister,
    /// Lookup for individuals by PNR
    individual_lookup: HashMap<String, Arc<Individual>>,
}

impl MfrChildRegister {
    /// Create a new MFR Child register with an empty individual lookup
    #[must_use]
    pub fn new() -> Self {
        Self {
            base_register: MfrRegister::new(),
            individual_lookup: HashMap::new(),
        }
    }

    /// Create a new MFR Child register with a provided individual lookup
    #[must_use]
    pub fn new_with_lookup(lookup: HashMap<String, Arc<Individual>>) -> Self {
        Self {
            base_register: MfrRegister::new(),
            individual_lookup: lookup,
        }
    }

    /// Process a batch to extract Child models from MFR data
    pub fn process_batch(&self, batch: &RecordBatch) -> Result<Vec<Child>> {
        // Use the to_models method to convert the batch
        self.to_models(batch)
    }

    /// Set the individual lookup map
    pub fn set_individual_lookup(&mut self, lookup: HashMap<String, Arc<Individual>>) {
        self.individual_lookup = lookup;
    }

    /// Add individuals to the lookup map
    pub fn add_individuals(&mut self, individuals: Vec<Individual>) {
        for individual in individuals {
            let pnr = individual.pnr.clone();
            self.individual_lookup.insert(pnr, Arc::new(individual));
        }
    }

    /// Get a reference to the base register
    #[must_use]
    pub const fn base_register(&self) -> &MfrRegister {
        &self.base_register
    }

    /// Get a reference to the individual lookup
    #[must_use]
    pub const fn get_individual_lookup(&self) -> &HashMap<String, Arc<Individual>> {
        &self.individual_lookup
    }
}

impl Default for MfrChildRegister {
    fn default() -> Self {
        Self::new()
    }
}

impl RegisterLoader for MfrChildRegister {
    fn get_register_name(&self) -> &'static str {
        self.base_register.get_register_name()
    }

    fn get_schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.base_register.get_schema()
    }

    fn load(
        &self,
        base_path: &Path,
        pnr_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<RecordBatch>> {
        self.base_register.load(base_path, pnr_filter)
    }

    fn load_async<'a>(
        &'a self,
        base_path: &'a Path,
        pnr_filter: Option<&'a HashSet<String>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<RecordBatch>>> + Send + 'a>>
    {
        self.base_register.load_async(base_path, pnr_filter)
    }

    fn supports_pnr_filter(&self) -> bool {
        self.base_register.supports_pnr_filter()
    }

    fn get_pnr_column_name(&self) -> Option<&'static str> {
        self.base_register.get_pnr_column_name()
    }
}

impl ModelConversion<Child> for MfrChildRegister {
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Child>> {
        // We require the individual lookup to be populated
        if self.individual_lookup.is_empty() {
            log::warn!("Individual lookup is empty, cannot convert MFR data to Child models");
            return Ok(Vec::new());
        }

        // First get basic Child models using the trait implementation
        let mut children = Child::from_mfr_batch(batch)?;
        
        // Then enhance them with Individual information from the lookup
        for child in &mut children {
            if let Some(individual) = self.individual_lookup.get(&child.individual().pnr) {
                // Replace the individual with the one from the lookup that has more complete information
                *child = Child::from_individual(individual.clone());
            }
        }
        
        Ok(children)
    }

    fn from_models(&self, _models: &[Child]) -> Result<RecordBatch> {
        // Converting Child models back to MFR registry format is complex
        // and not currently implemented
        Err(anyhow::anyhow!(
            "Converting Child models to MFR registry format is not yet implemented"
        ))
    }

    fn transform_models(&self, _models: &mut [Child]) -> Result<()> {
        // No additional transformations needed
        Ok(())
    }
}
