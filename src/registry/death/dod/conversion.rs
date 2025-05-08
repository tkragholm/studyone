//! DOD Registry conversion implementation
//!
//! This module contains the implementation for converting DOD data to domain models.

use crate::error::Result;
use crate::models::individual::Individual;
use crate::registry::ModelConversion;
use crate::registry::death::dod::DodRegister;
use arrow::record_batch::RecordBatch;

/// Enhance an individual with death date information
pub fn enhance_with_death_date(
    individual: &mut Individual,
    batch: &RecordBatch,
    row: usize,
) -> Result<bool> {
    use crate::utils::field_extractors::extract_date_from_string;
    
    // Extract death date using the field extractor
    if let Some(date) = extract_date_from_string(batch, row, "DODDATO", false)? {
        individual.death_date = Some(date);
        return Ok(true);
    }
    
    Ok(false)
}

/// Find an individual by PNR in a DOD batch and enhance it with death information
pub fn enhance_individuals_with_death_info(
    individuals: &mut [Individual],
    batch: &RecordBatch,
) -> Result<usize> {
    use crate::utils::field_extractors::extract_string;
    
    let mut count = 0;

    // Create a map of PNRs to row indices for fast lookup
    let mut pnr_row_map = std::collections::HashMap::new();
    for row in 0..batch.num_rows() {
        if let Some(pnr) = extract_string(batch, row, "PNR", false)? {
            pnr_row_map.insert(pnr, row);
        }
    }

    // Update individuals that exist in the death registry
    for individual in individuals.iter_mut() {
        if let Some(&row) = pnr_row_map.get(&individual.pnr) {
            if enhance_with_death_date(individual, batch, row)? {
                count += 1;
            }
        }
    }

    Ok(count)
}

// Implement ModelConversion for DOD registry
impl ModelConversion<Individual> for DodRegister {
    fn to_models(&self, _batch: &RecordBatch) -> Result<Vec<Individual>> {
        // DOD registry only contains death information, not complete individual info
        // It can only enhance existing individuals, not create new ones
        Ok(Vec::new())
    }

    fn from_models(&self, _models: &[Individual]) -> Result<RecordBatch> {
        unimplemented!("Conversion from Individual to DOD record batch not implemented")
    }

    fn transform_models(&self, _models: &mut [Individual]) -> Result<()> {
        // Load the DOD registry and enhance models with death information
        // This would need to be implemented in a way that accesses the actual data
        // Here we just provide a placeholder
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enhance_with_death_date() {
        // TODO: Add tests for death date enhancement
    }
}
