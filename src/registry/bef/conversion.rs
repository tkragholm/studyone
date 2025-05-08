//! BEF registry model conversions
//!
//! This module implements registry-specific conversions for BEF registry data.
//! It provides trait implementations to convert from BEF registry format to domain models.
//! It also provides compatibility with the old `ModelConversion` interface.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::Family;
use crate::models::Individual;
use crate::models::traits::HealthStatus;
use crate::models::types::{FamilyType, Gender};
use crate::registry::{BefRegister, ModelConversion};
use arrow::array::Array;
use std::collections::HashMap;

// Implementation of BEF registry conversion functions
// These were moved from the individual model to separate concerns

/// Convert a BEF registry record to an Individual model
pub fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    use crate::utils::field_extractors::{extract_string, extract_date32, extract_int8_as_padded_string};
    use crate::models::types::Origin;
    
    // Extract PNR - required for identification
    let pnr = match extract_string(batch, row, "PNR", true)? {
        Some(pnr) => pnr,
        None => return Ok(None), // No PNR, can't create an Individual
    };

    // Extract gender
    let gender = match extract_string(batch, row, "KOEN", false)? {
        Some(gender_str) => match gender_str.as_str() {
            "M" => Gender::Male,
            "F" => Gender::Female,
            _ => Gender::Unknown,
        },
        None => Gender::Unknown,
    };

    // Extract birth date
    let birth_date = extract_date32(batch, row, "FOED_DAG", false)?;

    // Extract family ID
    let family_id = extract_string(batch, row, "FAMILIE_ID", false)?;

    // Extract mother's PNR
    let mother_pnr = extract_string(batch, row, "MOR_ID", false)?;

    // Extract father's PNR
    let father_pnr = extract_string(batch, row, "FAR_ID", false)?;

    // Extract origin information from OPR_LAND
    let origin = match extract_string(batch, row, "OPR_LAND", false)? {
        Some(origin_code) => {
            if origin_code == "5100" {
                Origin::Danish
            } else if origin_code.starts_with('5') {
                // Other Nordic countries
                Origin::Western
            } else if origin_code.len() >= 2 && "0123456789".contains(&origin_code[0..1]) {
                // Country codes starting with digits 0-9 are typically Western countries
                Origin::Western
            } else {
                Origin::NonWestern
            }
        },
        None => Origin::Unknown,
    };

    // Extract municipality code
    let municipality_code = extract_int8_as_padded_string(batch, row, "KOM", false, 3)?;

    // Create a new Individual with all extracted data
    let mut individual = Individual::new(pnr, gender, birth_date);
    individual.family_id = family_id;
    individual.mother_pnr = mother_pnr;
    individual.father_pnr = father_pnr;
    individual.origin = origin;
    individual.municipality_code = municipality_code;
    // Default other fields that we don't populate from BEF

    Ok(Some(individual))
}

/// Convert a BEF registry batch to a collection of Individual models
pub fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    let mut individuals = Vec::new();
    for row in 0..batch.num_rows() {
        if let Some(individual) = from_bef_record(batch, row)? {
            individuals.push(individual);
        }
    }
    Ok(individuals)
}

// Maintain compatibility with the old ModelConversion interface
impl ModelConversion<Individual> for BefRegister {
    /// Convert BEF registry data to Individual domain models
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Individual>> {
        // Use the functions defined in this module
        from_bef_batch(batch)
    }

    /// Convert Individual domain models back to BEF registry data
    fn from_models(&self, _models: &[Individual]) -> Result<RecordBatch> {
        unimplemented!("Conversion from Individual models to BEF registry data not yet implemented")
    }

    /// Apply additional transformations to Individual models
    fn transform_models(&self, _models: &mut [Individual]) -> Result<()> {
        Ok(())
    }
}

// Maintain compatibility with ModelConversion for Family
impl ModelConversion<Family> for BefRegister {
    /// Convert BEF registry data to Family domain models
    fn to_models(&self, batch: &RecordBatch) -> Result<Vec<Family>> {
        // First get individual data
        let individuals = from_bef_batch(batch)?;

        // Generate family models
        let mut families_map: HashMap<String, Vec<&Individual>> = HashMap::new();
        for individual in &individuals {
            if let Some(family_id) = &individual.family_id {
                families_map
                    .entry(family_id.clone())
                    .or_default()
                    .push(individual);
            }
        }

        // Create Family objects from grouped individuals
        let mut families = Vec::new();
        let current_date = chrono::Utc::now().naive_utc().date();

        for (family_id, members) in families_map {
            // Find parents and children in the family
            let mut mothers = Vec::new();
            let mut fathers = Vec::new();
            let mut children = Vec::new();

            for member in &members {
                // Simple heuristic: adults (18+) are potential parents, others are children
                if let Some(age) = member.age_at(&current_date) {
                    if age >= 18 {
                        match member.gender {
                            Gender::Female => mothers.push(member),
                            Gender::Male => fathers.push(member),
                            Gender::Unknown => {} // Skip individuals with unknown gender
                        }
                    } else {
                        children.push(member);
                    }
                }
            }

            // Determine family type
            let family_type = match (mothers.len(), fathers.len()) {
                (1.., 1..) => FamilyType::TwoParent,
                (1.., 0) => FamilyType::SingleMother,
                (0, 1..) => FamilyType::SingleFather,
                (0, 0) => FamilyType::NoParent,
            };

            // Create a new family
            // Since we don't have specific valid_from dates, we'll use a default
            let default_valid_from = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let family = Family::new(family_id, family_type, default_valid_from);

            families.push(family);
        }

        Ok(families)
    }

    /// Convert Family domain models back to BEF registry data
    fn from_models(&self, _models: &[Family]) -> Result<RecordBatch> {
        unimplemented!("Conversion from Family models to BEF registry data not yet implemented")
    }

    /// Apply additional transformations to Family models
    fn transform_models(&self, _models: &mut [Family]) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bef_conversion() {
        // TODO: Add tests for BEF conversion
    }
}
