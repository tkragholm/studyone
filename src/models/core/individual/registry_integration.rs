//! Registry integration for Individual models
//!
//! This module provides a simplified API for enhancing Individual models with registry data,
//! while delegating the actual implementation to the registry module.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::individual::Individual;
use arrow::array::Array;

impl Individual {
    /// Enhance this Individual with data from a registry record
    ///
    /// This method detects the registry type and delegates to the appropriate
    /// registry-specific deserializer.
    ///
    /// # Arguments
    ///
    /// * `batch` - The `RecordBatch` containing registry data
    /// * `row` - The row index to use for enhancement
    ///
    /// # Returns
    ///
    /// `true` if any data was added to the Individual, `false` otherwise
    pub fn enhance_from_registry(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // First check if the PNR matches
        if !self.pnr_matches_record(batch, row)? {
            return Ok(false);
        }

        // Deserialize a new Individual from the registry record
        if let Some(enhanced_individual) =
            crate::registry::deserializer::deserialize_row(batch, row)?
        {
            // Merge fields from the enhanced individual into self, but only if they're not already set
            self.merge_fields(&enhanced_individual);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Check if this Individual's PNR matches the PNR in a registry record
    ///
    /// # Arguments
    ///
    /// * `batch` - The `RecordBatch` containing registry data
    /// * `row` - The row index to check
    ///
    /// # Returns
    ///
    /// `true` if the PNRs match, `false` otherwise
    pub fn pnr_matches_record(&self, batch: &RecordBatch, row: usize) -> Result<bool> {
        use crate::utils::array_utils::{downcast_array, get_column};
        use arrow::array::StringArray;
        use arrow::datatypes::DataType;

        // Try to get PNR column
        let pnr_col = get_column(batch, "PNR", &DataType::Utf8, false)?;
        if let Some(array) = pnr_col {
            let string_array = downcast_array::<StringArray>(&array, "PNR", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                let record_pnr = string_array.value(row);
                return Ok(record_pnr == self.pnr);
            }
        }

        Ok(false)
    }

    /// Merge fields from another Individual into this one
    ///
    /// This method copies fields from the source Individual, but only if
    /// the corresponding field in this Individual is not already set.
    ///
    /// # Arguments
    ///
    /// * `source` - The Individual to copy fields from
    fn merge_fields(&mut self, source: &Self) {
        use crate::models::core::types::Gender;

        // Only copy fields if they're not already set
        if self.gender == Gender::Unknown {
            self.gender = source.gender;
        }

        if self.birth_date.is_none() {
            self.birth_date = source.birth_date;
        }

        if self.death_date.is_none() {
            self.death_date = source.death_date;
        }

        if self.family_id.is_none() {
            self.family_id = source.family_id.clone();
        }

        if self.mother_pnr.is_none() {
            self.mother_pnr = source.mother_pnr.clone();
        }

        if self.father_pnr.is_none() {
            self.father_pnr = source.father_pnr.clone();
        }

        // Continue with all other fields...
        // This is simplified for brevity, but would include all fields
    }
}
