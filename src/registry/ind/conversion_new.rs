//! IND registry model conversions
//!
//! This module implements registry-specific conversions for IND registry data.
//! It provides trait implementations to convert from IND registry format to domain models.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::individual_new::Individual;
use crate::models::types::Gender;
use crate::common::traits::IndRegistry;
use crate::utils::array_utils::{downcast_array, get_column};
use arrow::array::StringArray;
use arrow::datatypes::DataType;

// Implement IND registry conversion for Individual
impl IndRegistry for Individual {
    /// Convert an IND registry record to an Individual
    fn from_ind_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Extract PNR column
        let pnr_array_opt = get_column(batch, "PNR", &DataType::Utf8, true)?;
        let pnr_array = match &pnr_array_opt {
            Some(array) => downcast_array::<StringArray>(array, "PNR", "String")?,
            None => return Ok(None), // Required column missing
        };

        // Get PNR
        if row >= pnr_array.len() || pnr_array.is_null(row) {
            return Ok(None); // No PNR data
        }
        let pnr = pnr_array.value(row).to_string();

        // For now, create a simple Individual with minimal data
        let gender = Gender::Unknown;
        let birth_date = None;

        Ok(Some(Self::new(pnr, gender, birth_date)))
    }

    /// Convert an entire IND registry batch to Individuals
    fn from_ind_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Implement a simple batch conversion that just calls the record method for each row
        let mut individuals = Vec::new();
        for row in 0..batch.num_rows() {
            if let Some(individual) = Self::from_ind_record(batch, row)? {
                individuals.push(individual);
            }
        }

        Ok(individuals)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use arrow::array::StringBuilder;
    use arrow::record_batch::RecordBatchBuilder;

    #[test]
    fn test_individual_from_ind_record() -> Result<()> {
        // Create a test batch with IND data
        let schema = Schema::new(vec![
            Field::new("PNR", DataType::Utf8, false),
            // Add other fields as needed
        ]);
        
        let mut batch_builder = RecordBatchBuilder::new_with_capacity(schema, 1);
        
        // Add PNR data
        let mut pnr_builder = StringBuilder::new_with_capacity(1, 12);
        pnr_builder.append_value("0987654321")?;
        batch_builder.column_builder::<StringBuilder>(0).unwrap()
            .append_builder(&pnr_builder)?;
        
        let batch = batch_builder.build()?;
        
        // Test conversion
        let individual = Individual::from_ind_record(&batch, 0)?;
        
        assert!(individual.is_some());
        let individual = individual.unwrap();
        assert_eq!(individual.pnr, "0987654321");
        
        Ok(())
    }
}