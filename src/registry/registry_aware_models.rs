//! Registry-aware model implementations
//!
//! This module provides implementations of the RegistryAware trait for domain models,
//! centralizing the registry-specific behavior for model creation.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::individual_new::Individual;
use crate::common::traits::{BefRegistry, IndRegistry, RegistryAware};

// Implement RegistryAware for Individual
impl RegistryAware for Individual {
    /// Get the registry name for this model
    fn registry_name() -> &'static str {
        "BEF" // Primary registry for Individuals
    }

    /// Create a model from a registry-specific record
    fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Route to the appropriate registry-specific implementation
        if batch.schema().field_with_name("RECNUM").is_ok() {
            // This appears to be a LPR registry batch
            log::warn!("LPR registry conversion for Individual not yet implemented");
            Ok(None)
        } else if batch.schema().field_with_name("PERINDKIALT").is_ok() {
            // This is likely an IND registry batch
            Self::from_ind_record(batch, row)
        } else {
            // Default to BEF registry format
            Self::from_bef_record(batch, row)
        }
    }

    /// Create models from an entire registry record batch
    fn from_registry_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Route to the appropriate registry-specific implementation
        if batch.schema().field_with_name("RECNUM").is_ok() {
            // This appears to be a LPR registry batch
            log::warn!("LPR registry batch conversion for Individual not yet implemented");
            Ok(Vec::new())
        } else if batch.schema().field_with_name("PERINDKIALT").is_ok() {
            // This is likely an IND registry batch
            Self::from_ind_batch(batch)
        } else {
            // Default to BEF registry format
            Self::from_bef_batch(batch)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use arrow::array::StringBuilder;
    use arrow::record_batch::RecordBatchBuilder;

    #[test]
    fn test_individual_from_registry_record() -> Result<()> {
        // Create a test batch with BEF data
        let schema = Schema::new(vec![
            Field::new("PNR", DataType::Utf8, false),
            // Add other fields as needed
        ]);
        
        let mut batch_builder = RecordBatchBuilder::new_with_capacity(schema, 1);
        
        // Add PNR data
        let mut pnr_builder = StringBuilder::new_with_capacity(1, 12);
        pnr_builder.append_value("1234567890")?;
        batch_builder.column_builder::<StringBuilder>(0).unwrap()
            .append_builder(&pnr_builder)?;
        
        let batch = batch_builder.build()?;
        
        // Test generic conversion
        let individual = Individual::from_registry_record(&batch, 0)?;
        
        assert!(individual.is_some());
        let individual = individual.unwrap();
        assert_eq!(individual.pnr, "1234567890");
        
        Ok(())
    }
}