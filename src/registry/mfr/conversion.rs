//! MFR registry model conversion implementation
//!
//! This module implements direct conversion between MFR registry data and
//! Child domain models without requiring a separate adapter.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;
use crate::models::Child;
use crate::models::Individual;
use crate::registry::RegisterLoader;
use crate::registry::mfr::MfrRegister;
use arrow::record_batch::RecordBatch;

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
        // Use the deserializer directly to convert the batch to Child models
        let children = crate::registry::mfr::deserializer::deserialize_child_batch(
            batch,
            &self.individual_lookup,
        )?;
        Ok(children)
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
