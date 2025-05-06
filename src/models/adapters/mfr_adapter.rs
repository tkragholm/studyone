//! MFR Registry to Child Adapter
//!
//! This module contains the adapter that maps MFR registry data to Child domain models.
//! The MFR (Medical Birth Registry) contains birth information.

use super::RegistryAdapter;
use crate::error::{Error, Result};
use crate::models::child::Child;
use crate::models::individual::Individual;
use arrow::array::{Array, Date32Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::Arc;

/// Adapter for converting MFR registry data to Child models
pub struct MfrChildAdapter {
    individual_lookup: HashMap<String, Arc<Individual>>,
}

impl MfrChildAdapter {
    /// Create a new MFR adapter with a lookup of existing individuals
    #[must_use]
    pub const fn new(individuals: HashMap<String, Arc<Individual>>) -> Self {
        Self {
            individual_lookup: individuals,
        }
    }

    /// Get birth details from MFR record batch for a specific PNR
    fn extract_birth_details(
        &self,
        batch: &RecordBatch,
        pnr: &str,
    ) -> Result<Option<(Option<i32>, Option<i32>, Option<i32>)>> {
        // Map column names to their indices
        let pnr_idx = batch
            .schema()
            .index_of("CPR_BARN")
            .map_err(|_| Error::ColumnNotFound {
                column: "CPR_BARN".to_string(),
            })?;

        // These fields might be optional in MFR schema
        let birth_weight_idx = batch.schema().index_of("VAGT").ok();
        let gestational_age_idx = batch.schema().index_of("SVLENGDE").ok();
        let apgar_idx = batch.schema().index_of("APGAR5").ok();

        // Get arrays
        let pnr_array = batch
            .column(pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "CPR_BARN".to_string(),
                expected: "String".to_string(),
            })?;

        // Find the row with matching PNR
        for i in 0..batch.num_rows() {
            if pnr_array.value(i) == pnr {
                // Extract birth details if columns exist
                let birth_weight = if let Some(idx) = birth_weight_idx {
                    // Assuming birth weight is stored as an Int32 or similar
                    // In a real implementation, check the actual data type
                    batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .and_then(|array| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i))
                            }
                        })
                } else {
                    None
                };

                let gestational_age = if let Some(idx) = gestational_age_idx {
                    batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .and_then(|array| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i))
                            }
                        })
                } else {
                    None
                };

                let apgar_score = if let Some(idx) = apgar_idx {
                    batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .and_then(|array| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i))
                            }
                        })
                } else {
                    None
                };

                return Ok(Some((birth_weight, gestational_age, apgar_score)));
            }
        }

        // PNR not found in this batch
        Ok(None)
    }

    /// Determine birth order based on siblings with same parents
    fn determine_birth_order(
        &self,
        batch: &RecordBatch,
        pnr: &str,
        mother_pnr: &str,
    ) -> Result<Option<i32>> {
        // Map column names to their indices
        let child_pnr_idx =
            batch
                .schema()
                .index_of("CPR_BARN")
                .map_err(|_| Error::ColumnNotFound {
                    column: "CPR_BARN".to_string(),
                })?;

        let mother_pnr_idx =
            batch
                .schema()
                .index_of("CPR_MODER")
                .map_err(|_| Error::ColumnNotFound {
                    column: "CPR_MODER".to_string(),
                })?;

        let birth_date_idx =
            batch
                .schema()
                .index_of("FOEDSELSDATO")
                .map_err(|_| Error::ColumnNotFound {
                    column: "FOEDSELSDATO".to_string(),
                })?;

        // Get arrays
        let child_pnr_array = batch
            .column(child_pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "CPR_BARN".to_string(),
                expected: "String".to_string(),
            })?;

        let mother_pnr_array = batch
            .column(mother_pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "CPR_MODER".to_string(),
                expected: "String".to_string(),
            })?;

        let birth_date_array = batch
            .column(birth_date_idx)
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "FOEDSELSDATO".to_string(),
                expected: "Date32".to_string(),
            })?;

        // Collect birth dates for all children of the same mother
        let mut siblings_data = Vec::new();

        for i in 0..batch.num_rows() {
            if mother_pnr_array.value(i) == mother_pnr {
                let sibling_pnr = child_pnr_array.value(i).to_string();

                if !birth_date_array.is_null(i) {
                    // Convert Date32 to NaiveDate (days since Unix epoch)
                    let birth_date = NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .checked_add_days(chrono::Days::new(birth_date_array.value(i) as u64))
                        .unwrap();

                    siblings_data.push((sibling_pnr, birth_date));
                }
            }
        }

        // Sort siblings by birth date
        siblings_data.sort_by(|a, b| a.1.cmp(&b.1));

        // Find the position of the target child
        for (position, (sibling_pnr, _)) in siblings_data.iter().enumerate() {
            if sibling_pnr == pnr {
                // Birth order is 1-based (1 = first born)
                return Ok(Some((position + 1) as i32));
            }
        }

        // If not found or couldn't be determined
        Ok(None)
    }
}

impl RegistryAdapter<Child> for MfrChildAdapter {
    /// Convert an MFR `RecordBatch` to a vector of Child objects
    fn from_record_batch(_batch: &RecordBatch) -> Result<Vec<Child>> {
        // This is a static implementation with no individual lookup
        // In practice, it's better to use the constructor to provide the lookup
        Err(anyhow::anyhow!(
            "MfrChildAdapter requires an individual_lookup. Use MfrChildAdapter::new() constructor instead."
        ))
    }

    /// Apply additional transformations to the Child models
    fn transform(_models: &mut [Child]) -> Result<()> {
        // No additional transformations needed for children from MFR
        Ok(())
    }
}

impl MfrChildAdapter {
    /// Process an MFR batch and create Child objects for individuals in the lookup
    pub fn process_batch(&self, batch: &RecordBatch) -> Result<Vec<Child>> {
        // Get column indices
        let pnr_idx = batch
            .schema()
            .index_of("CPR_BARN")
            .map_err(|_| Error::ColumnNotFound {
                column: "CPR_BARN".to_string(),
            })?;

        let mother_pnr_idx =
            batch
                .schema()
                .index_of("CPR_MODER")
                .map_err(|_| Error::ColumnNotFound {
                    column: "CPR_MODER".to_string(),
                })?;

        // Get arrays
        let pnr_array = batch
            .column(pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "CPR_BARN".to_string(),
                expected: "String".to_string(),
            })?;

        let mother_pnr_array = batch
            .column(mother_pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "CPR_MODER".to_string(),
                expected: "String".to_string(),
            })?;

        let mut children = Vec::new();

        // Process each row in the batch
        for i in 0..batch.num_rows() {
            let pnr = pnr_array.value(i).to_string();

            // Skip if we don't have this individual in our lookup
            if let Some(individual) = self.individual_lookup.get(&pnr) {
                // Get birth details
                let birth_details = self.extract_birth_details(batch, &pnr)?;

                // Create child from the individual
                let mut child = Child::from_individual(individual.clone());

                // Set birth details if available
                if let Some((birth_weight, gestational_age, apgar_score)) = birth_details {
                    child = child.with_birth_details(birth_weight, gestational_age, apgar_score);
                }

                // Set birth order if mother is known
                if !mother_pnr_array.is_null(i) {
                    let mother_pnr = mother_pnr_array.value(i).to_string();
                    if let Some(birth_order) =
                        self.determine_birth_order(batch, &pnr, &mother_pnr)?
                    {
                        child = child.with_birth_order(birth_order);
                    }
                }

                children.push(child);
            }
        }

        Ok(children)
    }
}
