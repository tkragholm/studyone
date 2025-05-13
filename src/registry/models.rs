//! Generic registry trait implementations for models
//!
//! This module contains generic registry trait implementations that are not specific
//! to any particular registry type.

use crate::RecordBatch;
use crate::common::traits::RegistryAware;
use crate::error::Result;
use crate::models::core::Individual;
use crate::registry::lpr::deserializer;
//use crate::models::derived::Child;
//use std::sync::Arc;

// Implement RegistryAware for Individual
impl RegistryAware for Individual {
    /// Get the registry name for this model
    fn registry_name() -> &'static str {
        "BEF" // Primary registry for Individuals
    }

    /// Create a model from a registry-specific record
    fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Use the central deserializer which will route to the appropriate registry-specific deserializer
        deserializer::deserialize_row(batch, row)
    }

    /// Create models from an entire registry record batch
    fn from_registry_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Use the central deserializer which will route to the appropriate registry-specific deserializer
        deserializer::deserialize_batch(batch)
    }
}

// // Implement RegistryAware for Child
// impl RegistryAware for Child {
//     /// Get the registry name for this model
//     fn registry_name() -> &'static str {
//         "MFR" // Primary registry for Children
//     }

//     /// Create a model from a registry-specific record
//     fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
//         // First create an Individual from the registry record
//         if let Some(individual) = Individual::from_registry_record(batch, row)? {
//             // Then convert that Individual to a Child
//             Ok(Some(Self::from_individual(Arc::new(individual))))
//         } else {
//             Ok(None)
//         }
//     }

//     /// Create models from an entire registry record batch
//     fn from_registry_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
//         // First create Individuals from the registry batch
//         let individuals = Individual::from_registry_batch(batch)?;

//         // Then convert those Individuals to Children
//         let children = individuals
//             .into_iter()
//             .map(|individual| Self::from_individual(Arc::new(individual)))
//             .collect();

//         Ok(children)
//     }
// }

/// Extension methods for Individual for direct `serde_arrow` conversion from registry data
impl Individual {
    /// Convert a registry batch to Individual models using `serde_arrow`
    ///
    /// This method uses `serde_arrow` for efficient direct deserialization from Arrow to Rust structs.
    /// It handles field name mapping and type conversions automatically.
    pub fn from_registry_batch_with_serde_arrow(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Use the central deserializer which will route to the appropriate registry-specific deserializer
        deserializer::deserialize_batch(batch)
    }
}
