//! DOD registry trait implementations for Individual
//! 
//! This module contains the implementation of `DodRegistry` for the Individual model.

use crate::RecordBatch;
use crate::common::traits::registry::DodRegistry;
use crate::error::Result;
use crate::models::core::Individual;

impl DodRegistry for Individual {
    fn enhance_with_death_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // Use the serde_arrow-based deserializer
        crate::registry::death::dod::deserializer::enhance_with_death_data(self, batch, row)
    }
}