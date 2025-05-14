//! Trait-based deserializer interface
//!
//! This module defines traits for deserializing registry data using
//! the trait-based field access system. It provides a more type-safe
//! approach to deserializing registry data into the Individual model.

use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::collections::HashMap;

use crate::error::Result;
use crate::models::core::Individual;

/// Marker trait for registry-specific types
///
/// This trait is used to mark types that can be used as registry models.
/// It's implemented for Individual and for user-defined registry types.
pub trait RegistryType: Any + Send + Sync {}

// Implement the trait for Individual
impl RegistryType for Individual {}

/// Trait for registry field extraction from Arrow data
///
/// This trait defines the interface for extracting fields from
/// Arrow data and setting them on an Individual using registry-specific
/// trait methods. It's designed to work with both manual implementations
/// and auto-generated extractors from the unified schema.
pub trait RegistryFieldExtractor: Send + Sync + std::fmt::Debug {
    /// Extract a field value from the given record batch and row
    ///
    /// # Arguments
    ///
    /// * `batch` - The Arrow record batch
    /// * `row` - The row index to extract from
    /// * `target` - The target object to set the value on, as a dyn Any
    ///
    /// This method extracts a value and sets it on the target object
    /// using the appropriate trait setter method.
    fn extract_and_set(&self, batch: &RecordBatch, row: usize, target: &mut dyn Any) -> Result<()>;

    /// Get the source field name in the registry data
    fn source_field_name(&self) -> &str;

    /// Get the target field name in the Individual model
    fn target_field_name(&self) -> &str;
}

/// Trait for registry-specific deserialization
///
/// This trait defines the interface for deserializing registry data
/// into the Individual model using registry-specific field extractors.
pub trait RegistryDeserializer: Send + Sync + std::fmt::Debug {
    /// Get the registry type name
    fn registry_type(&self) -> &str;

    /// Get field extractors for this registry
    fn field_extractors(&self) -> &[Box<dyn RegistryFieldExtractor>];

    /// Get field name mapping
    ///
    /// This provides a mapping from registry field names to `SerdeIndividual`
    /// field names for backward compatibility.
    fn field_mapping(&self) -> HashMap<String, String>;

    /// Deserialize a record batch into a vec of Individuals
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch to deserialize
    ///
    /// # Returns
    ///
    /// A Result containing a Vec of deserialized Individuals
    fn deserialize_batch(&self, batch: &RecordBatch) -> Result<Vec<Individual>> {
        let mut individuals = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            if let Some(individual) = self.deserialize_row(batch, row)? {
                individuals.push(individual);
            }
        }

        Ok(individuals)
    }

    /// Deserialize a single row from a record batch
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch
    /// * `row` - The row index to deserialize
    ///
    /// # Returns
    ///
    /// A Result containing an Option with the deserialized Individual
    fn deserialize_row(&self, batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
        // Create a new Individual with empty values
        let mut individual = Individual::new(
            String::new(), // Empty PNR to be filled by extractors
            None,          // No birth date yet
        );

        // Apply all field extractors
        for extractor in self.field_extractors() {
            extractor.extract_and_set(batch, row, &mut individual as &mut dyn Any)?;
        }

        // Return the deserialized Individual if it has a valid PNR
        if individual.pnr.is_empty() {
            Ok(None)
        } else {
            Ok(Some(individual))
        }
    }
}
