//! UDDF Registry conversion implementation
//!
//! This module contains the implementation for converting UDDF data to domain models.

use crate::error::Result;
use crate::models::individual::Individual;
use crate::models::types::EducationLevel;
use crate::registry::ModelConversion;
use crate::registry::uddf::UddfRegister;
use crate::utils::array_utils::{downcast_array, get_column};
use arrow::array::Array;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

// UDDF registry conversion implementation for the UddfRegistry trait
// This takes methods that were previously in individual.rs and moves them here
pub fn from_uddf_record(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
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

    // Create a basic Individual with minimal data
    let gender = crate::models::types::Gender::Unknown; // UDDF doesn't provide gender
    let birth_date = None; // UDDF doesn't provide birth date

    // Create Individual with the extracted data
    let mut individual = Individual::new(pnr, gender, birth_date);

    // Enhance with education data if available
    let _ = enhance_with_education_data(&mut individual, batch, row);

    Ok(Some(individual))
}

pub fn from_uddf_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    let mut individuals = Vec::new();
    for row in 0..batch.num_rows() {
        if let Some(individual) = from_uddf_record(batch, row)? {
            individuals.push(individual);
        }
    }
    Ok(individuals)
}

pub fn enhance_with_education_data(
    individual: &mut Individual,
    batch: &RecordBatch,
    row: usize,
) -> Result<bool> {
    // Extract HFAUDD column (education code)
    let hfaudd_array_opt = get_column(batch, "HFAUDD", &DataType::Utf8, true)?;
    let hfaudd_array = match &hfaudd_array_opt {
        Some(array) => downcast_array::<StringArray>(array, "HFAUDD", "String")?,
        None => return Ok(false), // No education data available
    };

    // Get education code if available
    if row < hfaudd_array.len() && !hfaudd_array.is_null(row) {
        let education_code = hfaudd_array.value(row);

        // Map education code to EducationLevel
        // This is a simplified mapping - real implementation would be more detailed
        individual.education_level = match education_code.chars().next() {
            Some('1') => EducationLevel::Low,
            Some('2') | Some('3') => EducationLevel::Medium,
            Some('4') | Some('5') | Some('6') | Some('7') | Some('8') => EducationLevel::High,
            _ => EducationLevel::Unknown,
        };

        return Ok(true);
    }

    Ok(false)
}

// Implement ModelConversion for UddfRegister using the functions above
impl ModelConversion<Individual> for UddfRegister {
    /// Convert UDDF registry data to Individual domain models
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Individual>> {
        // Use the functions defined in this module
        from_uddf_batch(batch)
    }

    /// Convert Individual models back to UDDF registry data
    fn from_models(&self, _models: &[Individual]) -> Result<RecordBatch> {
        // This is a placeholder implementation that would map Individual fields
        // back to the UDDF schema format
        unimplemented!("Conversion from Individual to UDDF record batch not implemented")
    }

    /// Apply additional transformations to models if needed
    fn transform_models(&self, _models: &mut [Individual]) -> Result<()> {
        // In a real implementation, we might enhance models with additional
        // education data from other batches if available
        Ok(())
    }
}
