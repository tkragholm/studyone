//! BEF Registry to Individual/Family Adapter
//!
//! This module contains the adapter that maps BEF registry data to Individual and Family domain models.
//! The BEF (Befolkning) registry contains population demographic information.

use super::RegistryAdapter;
use crate::error::{Error, Result};
use crate::models::family::{Family, FamilyType};
use crate::models::individual::{EducationLevel, Gender, Individual, Origin};
use arrow::array::{Array, Date32Array, Int8Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::Arc;

/// Adapter for converting BEF registry data to Individual models
pub struct BefIndividualAdapter;

impl RegistryAdapter<Individual> for BefIndividualAdapter {
    /// Convert a BEF `RecordBatch` to a vector of Individual objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
        // Get the column indices for required fields
        let pnr_idx = batch
            .schema()
            .index_of("PNR")
            .map_err(|_| Error::ColumnNotFound {
                column: "PNR".to_string(),
            })?;
        let birth_day_idx =
            batch
                .schema()
                .index_of("FOED_DAG")
                .map_err(|_| Error::ColumnNotFound {
                    column: "FOED_DAG".to_string(),
                })?;
        let far_id_idx = batch
            .schema()
            .index_of("FAR_ID")
            .map_err(|_| Error::ColumnNotFound {
                column: "FAR_ID".to_string(),
            })?;
        let mor_id_idx = batch
            .schema()
            .index_of("MOR_ID")
            .map_err(|_| Error::ColumnNotFound {
                column: "MOR_ID".to_string(),
            })?;
        let familie_id_idx =
            batch
                .schema()
                .index_of("FAMILIE_ID")
                .map_err(|_| Error::ColumnNotFound {
                    column: "FAMILIE_ID".to_string(),
                })?;
        let gender_idx = batch
            .schema()
            .index_of("KOEN")
            .map_err(|_| Error::ColumnNotFound {
                column: "KOEN".to_string(),
            })?;
        let municipality_idx =
            batch
                .schema()
                .index_of("KOM")
                .map_err(|_| Error::ColumnNotFound {
                    column: "KOM".to_string(),
                })?;
        let origin_idx =
            batch
                .schema()
                .index_of("OPR_LAND")
                .map_err(|_| Error::ColumnNotFound {
                    column: "OPR_LAND".to_string(),
                })?;

        // Cast columns to their appropriate types
        let pnr_array = batch
            .column(pnr_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "PNR".to_string(),
                expected: "String".to_string(),
            })?;

        let birth_day_array = batch
            .column(birth_day_idx)
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "FOED_DAG".to_string(),
                expected: "Date32".to_string(),
            })?;

        let far_id_array = batch
            .column(far_id_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "FAR_ID".to_string(),
                expected: "String".to_string(),
            })?;

        let mor_id_array = batch
            .column(mor_id_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "MOR_ID".to_string(),
                expected: "String".to_string(),
            })?;

        let familie_id_array = batch
            .column(familie_id_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "FAMILIE_ID".to_string(),
                expected: "String".to_string(),
            })?;

        let gender_array = batch
            .column(gender_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "KOEN".to_string(),
                expected: "String".to_string(),
            })?;

        let municipality_array = batch
            .column(municipality_idx)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "KOM".to_string(),
                expected: "Int8".to_string(),
            })?;

        let origin_array = batch
            .column(origin_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::InvalidDataType {
                column: "OPR_LAND".to_string(),
                expected: "String".to_string(),
            })?;

        // Create individual objects from the record batch
        let mut individuals = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let pnr = pnr_array.value(i).to_string();

            // Determine gender based on PNR or KOEN field
            let gender = if gender_array.is_null(i) {
                Gender::Unknown
            } else {
                match gender_array.value(i) {
                    "M" => Gender::Male,
                    "F" => Gender::Female,
                    _ => {
                        // Fallback to PNR-based gender (9th digit is odd for males, even for females)
                        if pnr.len() >= 10 {
                            if let Some(digit) = pnr.chars().nth(9) {
                                if let Some(d) = digit.to_digit(10) {
                                    if d % 2 == 1 {
                                        Gender::Male
                                    } else {
                                        Gender::Female
                                    }
                                } else {
                                    Gender::Unknown
                                }
                            } else {
                                Gender::Unknown
                            }
                        } else {
                            Gender::Unknown
                        }
                    }
                }
            };

            // Convert birth date
            let birth_date = if birth_day_array.is_null(i) {
                None
            } else {
                // Convert Date32 to NaiveDate (days since Unix epoch)
                Some(
                    NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .checked_add_days(chrono::Days::new(birth_day_array.value(i) as u64))
                        .unwrap(),
                )
            };

            // Parse origin based on country code
            let origin = if origin_array.is_null(i) {
                Origin::Unknown
            } else {
                match origin_array.value(i) {
                    "5100" => Origin::Danish, // Denmark
                    code => {
                        let code_num = code.parse::<i32>().unwrap_or(0);
                        if (5000..5999).contains(&code_num) {
                            // Western countries
                            Origin::Western
                        } else {
                            Origin::NonWestern
                        }
                    }
                }
            };

            // Create Individual object
            let mut individual = Individual::new(pnr, gender, birth_date);

            // Set additional fields
            individual.origin = origin;
            individual.education_level = EducationLevel::Unknown; // Not available in BEF, needs UDDA registry

            if !municipality_array.is_null(i) {
                individual.municipality_code = Some(municipality_array.value(i).to_string());
                // Municipalities with codes below 1000 are typically rural
                individual.is_rural = municipality_array.value(i) < 100;
            }

            if !far_id_array.is_null(i) {
                individual.father_pnr = Some(far_id_array.value(i).to_string());
            }

            if !mor_id_array.is_null(i) {
                individual.mother_pnr = Some(mor_id_array.value(i).to_string());
            }

            if !familie_id_array.is_null(i) {
                individual.family_id = Some(familie_id_array.value(i).to_string());
            }

            individuals.push(individual);
        }

        Ok(individuals)
    }

    /// Apply additional transformations to the Individual models
    fn transform(models: &mut [Individual]) -> Result<()> {
        // No additional transformations needed for individuals from BEF
        Ok(())
    }
}

/// Adapter for converting BEF registry data to Family models
pub struct BefFamilyAdapter;

impl RegistryAdapter<Family> for BefFamilyAdapter {
    /// Convert a BEF `RecordBatch` to a vector of Family objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Family>> {
        // Get individual data first
        let individuals = BefIndividualAdapter::from_record_batch(batch)?;

        // Group individuals by family_id
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
                _ => FamilyType::Unknown,
            };

            // Create a new family
            // Since we don't have specific valid_from dates, we'll use a default
            let default_valid_from = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let family = Family::new(family_id, family_type, default_valid_from);

            // We're not setting parent and child references here because:
            // 1. We need to convert parents to Parent objects first
            // 2. We need to convert children to Child objects first
            // 3. We need to establish proper Arc references
            //
            // These will be done in a separate harmonization step when combining multiple registries

            families.push(family);
        }

        Ok(families)
    }

    /// Apply additional transformations to the Family models
    fn transform(models: &mut [Family]) -> Result<()> {
        // No additional transformations needed for families from BEF
        Ok(())
    }
}

/// Helper function to create a lookup of Individual objects by PNR
#[must_use] pub fn create_individual_lookup(individuals: &[Individual]) -> HashMap<String, Arc<Individual>> {
    let mut lookup = HashMap::new();
    for individual in individuals {
        lookup.insert(individual.pnr.clone(), Arc::new(individual.clone()));
    }
    lookup
}

/// Combined adapter that processes BEF data and returns both Individuals and Families
pub struct BefCombinedAdapter;

impl BefCombinedAdapter {
    /// Process a BEF `RecordBatch` and return both Individuals and Families
    pub fn process_batch(batch: &RecordBatch) -> Result<(Vec<Individual>, Vec<Family>)> {
        let individuals = BefIndividualAdapter::from_record_batch(batch)?;
        let families = BefFamilyAdapter::from_record_batch(batch)?;
        Ok((individuals, families))
    }

    /// Extract unique family relationships from BEF data
    #[must_use] pub fn extract_relationships(
        individuals: &[Individual],
    ) -> HashMap<String, (Option<String>, Option<String>, Vec<String>)> {
        let mut relationships: HashMap<String, (Option<String>, Option<String>, Vec<String>)> =
            HashMap::new();

        // Group individuals by family ID
        let mut family_members: HashMap<String, Vec<&Individual>> = HashMap::new();
        for individual in individuals {
            if let Some(family_id) = &individual.family_id {
                family_members
                    .entry(family_id.clone())
                    .or_default()
                    .push(individual);
            }
        }

        // Process each family
        for (family_id, members) in family_members {
            let mut children = Vec::new();
            let mut mother_pnr = None;
            let mut father_pnr = None;

            for member in &members {
                // Check if this individual is a parent of any other individual in the family
                let is_parent = members.iter().any(|m| {
                    (m.mother_pnr
                        .as_ref() == Some(&member.pnr))
                        || (m.father_pnr
                            .as_ref() == Some(&member.pnr))
                });

                if is_parent {
                    // This is a parent
                    match member.gender {
                        Gender::Female => mother_pnr = Some(member.pnr.clone()),
                        Gender::Male => father_pnr = Some(member.pnr.clone()),
                        Gender::Unknown => {} // Skip individuals with unknown gender
                    }
                } else {
                    // This is likely a child
                    children.push(member.pnr.clone());
                }
            }

            relationships.insert(family_id, (mother_pnr, father_pnr, children));
        }

        relationships
    }
}
