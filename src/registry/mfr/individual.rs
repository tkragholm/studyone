//! MFR registry trait implementations for Individual and Child
//!
//! This module contains the implementation of MfrRegistry for the Individual and Child models.

use crate::RecordBatch;
use crate::common::traits::MfrRegistry;
use crate::error::Result;
use crate::models::core::Individual;
use crate::models::derived::Child;
use std::sync::Arc;

impl MfrRegistry for Individual {
    fn from_mfr_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Use the MFR-specific deserializer
        crate::registry::mfr::deserializer::deserialize_row(batch, row)
    }

    fn from_mfr_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Use the MFR-specific deserializer
        crate::registry::mfr::deserializer::deserialize_batch(batch)
    }
}

// Implement MfrRegistry for Child
impl MfrRegistry for Child {
    fn from_mfr_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // First create an Individual from the MFR registry record
        if let Some(individual) = Individual::from_mfr_record(batch, row)? {
            // Then convert that Individual to a Child
            Ok(Some(Self::from_individual(Arc::new(individual))))
        } else {
            Ok(None)
        }
    }

    fn from_mfr_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // First create Individuals from the MFR registry batch
        let individuals = Individual::from_mfr_batch(batch)?;

        // Then convert those Individuals to Children
        let children = individuals
            .into_iter()
            .map(|individual| Self::from_individual(Arc::new(individual)))
            .collect();

        Ok(children)
    }
}
