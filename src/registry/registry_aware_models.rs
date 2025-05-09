//! Registry-aware model implementations
//!
//! This module provides implementations of the RegistryAware trait for domain models,
//! centralizing the registry-specific behavior for model creation.

use crate::RecordBatch;
use crate::common::traits::{BefRegistry, MfrRegistry, RegistryAware};
use crate::error::Result;
use crate::models::core::Individual;
use crate::models::derived::Child;
use crate::models::core::types::Gender;
use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

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
            let array_opt =
                crate::utils::array_utils::get_column(batch, "PNR", &DataType::Utf8, false)?;
            if array_opt.is_none() {
                return Ok(None);
            }

            // Create a binding to avoid temporary value error
            let array_value = array_opt.unwrap();
            let pnr_array = crate::utils::array_utils::downcast_array::<StringArray>(
                &array_value,
                "PNR",
                "String",
            )?;

            if row >= pnr_array.len() || pnr_array.is_null(row) {
                return Ok(None);
            }

            let pnr = pnr_array.value(row).to_string();
            let gender = crate::models::types::Gender::Unknown;
            let birth_date = None;

            Ok(Some(Self::new(pnr, gender, birth_date)))
        } else {
            // Default to BEF registry format
            use crate::registry::bef::conversion;
            conversion::from_bef_record(batch, row)
        }
    }

    /// Create models from an entire registry record batch
    fn from_registry_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Route to the appropriate registry-specific implementation
        let mut individuals = Vec::new();

        for row in 0..batch.num_rows() {
            if let Some(individual) = Self::from_registry_record(batch, row)? {
                individuals.push(individual);
            }
        }

        Ok(individuals)
    }
}

// Implement BefRegistry for Individual
impl BefRegistry for Individual {
    fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        use crate::registry::bef::conversion;
        conversion::from_bef_record(batch, row)
    }

    fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        use crate::registry::bef::conversion;
        conversion::from_bef_batch(batch)
    }
}

// Implement RegistryAware for Child
impl RegistryAware for Child {
    /// Get the registry name for this model
    fn registry_name() -> &'static str {
        "MFR" // Primary registry for Children
    }

    /// Create a model from a registry-specific record
    fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Delegate to the MFR registry trait implementation
        Self::from_mfr_record(batch, row)
    }

    /// Create models from an entire registry record batch
    fn from_registry_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Delegate to the MFR registry trait implementation
        Self::from_mfr_batch(batch)
    }
}

// Implement MfrRegistry for Child
impl MfrRegistry for Child {
    fn from_mfr_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        use crate::utils::array_utils::{get_column, downcast_array};

        // Try to get PNR of child
        let pnr_col = get_column(batch, "PNR", &DataType::Utf8, false)?;
        let pnr = if let Some(array) = pnr_col {
            let string_array = downcast_array::<StringArray>(&array, "PNR", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                string_array.value(row).to_string()
            } else {
                return Ok(None); // No valid PNR
            }
        } else {
            return Ok(None); // No PNR column
        };

        // Create a basic individual and wrap it in a Child
        let individual = Individual::new(
            pnr,
            Gender::Unknown, // Would be determined from data
            None, // Birth date would come from the data
        );

        Ok(Some(Self::from_individual(Arc::new(individual))))
    }

    fn from_mfr_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let mut children = Vec::new();

        for row in 0..batch.num_rows() {
            if let Some(child) = Self::from_mfr_record(batch, row)? {
                children.push(child);
            }
        }

        Ok(children)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_individual_from_registry_record() -> Result<()> {
        // Create a simple test batch with a PNR column
        let schema = Schema::new(vec![
            Field::new("PNR", DataType::Utf8, false),
        ]);

        // Create a simple batch with one row
        let pnr_array = StringArray::from(vec!["1234567890"]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(pnr_array)],
        ).unwrap();

        // Test generic conversion
        let individual = Individual::from_registry_record(&batch, 0)?;

        assert!(individual.is_some());
        let individual = individual.unwrap();
        assert_eq!(individual.pnr, "1234567890");

        Ok(())
    }

    #[test]
    fn test_child_from_registry_record() -> Result<()> {
        // Create a simple test batch with a PNR column
        let schema = Schema::new(vec![
            Field::new("PNR", DataType::Utf8, false),
        ]);

        // Create a simple batch with one row
        let pnr_array = StringArray::from(vec!["1234567890"]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(pnr_array)],
        ).unwrap();

        // Test generic conversion
        let child = Child::from_registry_record(&batch, 0)?;

        assert!(child.is_some());
        let child = child.unwrap();
        assert_eq!(child.individual().pnr, "1234567890");

        Ok(())
    }
}
