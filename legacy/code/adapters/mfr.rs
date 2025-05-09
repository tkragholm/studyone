//! MFR Registry Adapters
//!
//! This module provides adapters for converting MFR registry data
//! to Child domain models using the unified adapter interface.

use crate::common::traits::{ModelLookup, RegistryAdapter, StatefulAdapter};
use crate::error::Result;
use crate::models::Child;
use crate::registry::mfr::conversion::MfrChildRegister;
use crate::registry::ModelConversion;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Adapter for converting MFR registry data to Child models
#[derive(Debug)]
pub struct MfrChildAdapter {
    registry: MfrChildRegister,
}

impl MfrChildAdapter {
    /// Create a new MFR child adapter
    #[must_use] pub fn new() -> Self {
        Self {
            registry: MfrChildRegister::new(),
        }
    }
}

impl Default for MfrChildAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl StatefulAdapter<Child> for MfrChildAdapter {
    fn convert_batch(&self, batch: &RecordBatch) -> Result<Vec<Child>> {
        self.registry.to_models(batch)
    }

    fn transform_models(&self, models: &mut [Child]) -> Result<()> {
        self.registry.transform_models(models)
    }
}

impl RegistryAdapter<Child> for MfrChildAdapter {
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Child>> {
        let registry = MfrChildRegister::new();
        registry.to_models(batch)
    }

    fn transform(models: &mut [Child]) -> Result<()> {
        let registry = MfrChildRegister::new();
        registry.transform_models(models)
    }
}

/// Implement `ModelLookup` for Child
impl ModelLookup<Child, String> for Child {
    /// Create a lookup map from PNR to Child
    fn create_lookup(children: &[Child]) -> HashMap<String, Arc<Child>> {
        let mut lookup = HashMap::with_capacity(children.len());
        for child in children {
            lookup.insert(child.individual().pnr.clone(), Arc::new(child.clone()));
        }
        lookup
    }
}
