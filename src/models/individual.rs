//! Individual entity model
//!
//! This module contains the core Individual entity structure which is central to the study design.
//! An Individual represents any person in the study, and can be associated with various roles
//! such as parent or child.

use crate::common::traits::{BefRegistry, RegistryAware};
use crate::error::Result;
use crate::models::traits::{ArrowSchema, EntityModel, HealthStatus, TemporalValidity};
use crate::models::types::{EducationLevel, Gender, Origin};
use crate::utils::array_utils::{downcast_array, get_column};
use arrow::array::{Array, BooleanArray, Date32Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use std::collections::HashMap;

/// Core Individual entity representing a person in the study
#[derive(Debug, Clone)]
pub struct Individual {
    /// Personal identification number (PNR)
    pub pnr: String,
    /// Gender of the individual
    pub gender: Gender,
    /// Birth date
    pub birth_date: Option<NaiveDate>,
    /// Death date, if applicable
    pub death_date: Option<NaiveDate>,
    /// Geographic origin category
    pub origin: Origin,
    /// Education level
    pub education_level: EducationLevel,
    /// Municipality code at index date
    pub municipality_code: Option<String>,
    /// Whether the individual lives in a rural area
    pub is_rural: bool,
    /// Mother's PNR, if known
    pub mother_pnr: Option<String>,
    /// Father's PNR, if known
    pub father_pnr: Option<String>,
    /// Family identifier
    pub family_id: Option<String>,
    /// Emigration date, if applicable
    pub emigration_date: Option<NaiveDate>,
    /// Immigration date, if applicable
    pub immigration_date: Option<NaiveDate>,
}

impl Individual {
    /// Create a new Individual with minimal required information
    #[must_use]
    pub const fn new(pnr: String, gender: Gender, birth_date: Option<NaiveDate>) -> Self {
        Self {
            pnr,
            gender,
            birth_date,
            death_date: None,
            origin: Origin::Unknown,
            education_level: EducationLevel::Unknown,
            municipality_code: None,
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: None,
            emigration_date: None,
            immigration_date: None,
        }
    }

    /// Create a lookup map from PNR to Individual
    #[must_use]
    pub fn create_pnr_lookup(individuals: &[Self]) -> HashMap<String, Self> {
        let mut lookup = HashMap::with_capacity(individuals.len());
        for individual in individuals {
            lookup.insert(individual.pnr.clone(), individual.clone());
        }
        lookup
    }
}

// Implement EntityModel trait
impl EntityModel for Individual {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.pnr
    }

    fn key(&self) -> String {
        self.pnr.clone()
    }
}

// Implement TemporalValidity trait
impl TemporalValidity for Individual {
    fn was_valid_at(&self, date: &NaiveDate) -> bool {
        self.was_alive_at(date)
    }

    fn valid_from(&self) -> NaiveDate {
        self.birth_date
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1900, 1, 1).unwrap())
    }

    fn valid_to(&self) -> Option<NaiveDate> {
        self.death_date
    }

    fn snapshot_at(&self, date: &NaiveDate) -> Option<Self> {
        if self.was_valid_at(date) {
            Some(self.clone())
        } else {
            None
        }
    }
}

// Implement HealthStatus trait
impl HealthStatus for Individual {
    /// Calculate age of the individual at a specific reference date
    fn age_at(&self, reference_date: &NaiveDate) -> Option<i32> {
        match self.birth_date {
            Some(birth_date) => {
                // Check if the individual was alive at the reference date
                if self.death_date.is_none_or(|d| d >= *reference_date) {
                    let years = reference_date.year() - birth_date.year();
                    // Adjust for birthday not yet reached in the reference year
                    if reference_date.month() < birth_date.month()
                        || (reference_date.month() == birth_date.month()
                            && reference_date.day() < birth_date.day())
                    {
                        Some(years - 1)
                    } else {
                        Some(years)
                    }
                } else {
                    // Individual was not alive at the reference date
                    None
                }
            }
            None => None,
        }
    }

    /// Check if the individual was alive at a specific date
    fn was_alive_at(&self, date: &NaiveDate) -> bool {
        // Check birth date (must be born before or on the date)
        if let Some(birth) = self.birth_date {
            if birth > *date {
                return false;
            }
        } else {
            // Unknown birth date, can't determine
            return false;
        }

        // Check death date (must not have died before the date)
        if let Some(death) = self.death_date {
            if death < *date {
                return false;
            }
        }

        true
    }

    /// Check if the individual was resident in Denmark at a specific date
    fn was_resident_at(&self, date: &NaiveDate) -> bool {
        // Must be alive to be resident
        if !self.was_alive_at(date) {
            return false;
        }

        // Check emigration status
        if let Some(emigration) = self.emigration_date {
            if emigration <= *date {
                // Check if they immigrated back after emigration
                if let Some(immigration) = self.immigration_date {
                    return immigration > emigration && immigration <= *date;
                }
                return false;
            }
        }

        // Either never emigrated or emigrated after the date
        true
    }
}

// Implement RegistryAware for Individual
impl RegistryAware for Individual {
    /// Get the registry name for this model
    fn registry_name() -> &'static str {
        "BEF" // Individual is primarily from BEF registry
    }

    /// Create a model from a registry-specific record
    fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Since registry-specific implementations have been moved out,
        // this just provides a basic implementation that works with the registry
        // model conversion layers
        let pnr_array_opt = get_column(batch, "PNR", &DataType::Utf8, false)?;
        if pnr_array_opt.is_none() {
            return Ok(None);
        }

        // Create a binding to avoid temporary value error
        let pnr_array_value = pnr_array_opt.unwrap();
        let pnr_array = downcast_array::<StringArray>(&pnr_array_value, "PNR", "String")?;

        if row >= pnr_array.len() || pnr_array.is_null(row) {
            return Ok(None);
        }

        let pnr = pnr_array.value(row).to_string();
        let gender = Gender::Unknown;
        let birth_date = None;

        Ok(Some(Self::new(pnr, gender, birth_date)))
    }

    /// Create models from an entire registry record batch
    fn from_registry_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let mut individuals = Vec::new();

        for row in 0..batch.num_rows() {
            if let Some(individual) = Self::from_registry_record(batch, row)? {
                individuals.push(individual);
            }
        }

        Ok(individuals)
    }
}

// Implement BefRegistry for Individual - delegate to conversion module
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

// Implement ArrowSchema trait
impl ArrowSchema for Individual {
    /// Get the Arrow schema for Individual records
    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("pnr", DataType::Utf8, false),
            Field::new("gender", DataType::Int32, false),
            Field::new("birth_date", DataType::Date32, true),
            Field::new("death_date", DataType::Date32, true),
            Field::new("origin", DataType::Int32, false),
            Field::new("education_level", DataType::Int32, false),
            Field::new("municipality_code", DataType::Utf8, true),
            Field::new("is_rural", DataType::Boolean, false),
            Field::new("mother_pnr", DataType::Utf8, true),
            Field::new("father_pnr", DataType::Utf8, true),
            Field::new("family_id", DataType::Utf8, true),
            Field::new("emigration_date", DataType::Date32, true),
            Field::new("immigration_date", DataType::Date32, true),
        ])
    }

    /// Convert a `RecordBatch` to a vector of Individual objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let pnr_array = batch
            .column(batch.schema().index_of("pnr")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let gender_array = batch
            .column(batch.schema().index_of("gender")?)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let birth_date_array = batch
            .column(batch.schema().index_of("birth_date")?)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let death_date_array = batch
            .column(batch.schema().index_of("death_date")?)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let origin_array = batch
            .column(batch.schema().index_of("origin")?)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let education_level_array = batch
            .column(batch.schema().index_of("education_level")?)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let municipality_code_array = batch
            .column(batch.schema().index_of("municipality_code")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let is_rural_array = batch
            .column(batch.schema().index_of("is_rural")?)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        let mother_pnr_array = batch
            .column(batch.schema().index_of("mother_pnr")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let father_pnr_array = batch
            .column(batch.schema().index_of("father_pnr")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let family_id_array = batch
            .column(batch.schema().index_of("family_id")?)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let emigration_date_array = batch
            .column(batch.schema().index_of("emigration_date")?)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let immigration_date_array = batch
            .column(batch.schema().index_of("immigration_date")?)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let mut individuals = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let individual = Self {
                pnr: pnr_array.value(i).to_string(),
                gender: Gender::from(gender_array.value(i)),
                birth_date: if birth_date_array.is_null(i) {
                    None
                } else {
                    // Convert Date32 to NaiveDate (days since Unix epoch)
                    Some(
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(chrono::Days::new(birth_date_array.value(i) as u64))
                            .unwrap(),
                    )
                },
                death_date: if death_date_array.is_null(i) {
                    None
                } else {
                    Some(
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(chrono::Days::new(death_date_array.value(i) as u64))
                            .unwrap(),
                    )
                },
                origin: Origin::from(origin_array.value(i)),
                education_level: EducationLevel::from(education_level_array.value(i)),
                municipality_code: if municipality_code_array.is_null(i) {
                    None
                } else {
                    Some(municipality_code_array.value(i).to_string())
                },
                is_rural: is_rural_array.value(i),
                mother_pnr: if mother_pnr_array.is_null(i) {
                    None
                } else {
                    Some(mother_pnr_array.value(i).to_string())
                },
                father_pnr: if father_pnr_array.is_null(i) {
                    None
                } else {
                    Some(father_pnr_array.value(i).to_string())
                },
                family_id: if family_id_array.is_null(i) {
                    None
                } else {
                    Some(family_id_array.value(i).to_string())
                },
                emigration_date: if emigration_date_array.is_null(i) {
                    None
                } else {
                    Some(
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(chrono::Days::new(
                                emigration_date_array.value(i) as u64
                            ))
                            .unwrap(),
                    )
                },
                immigration_date: if immigration_date_array.is_null(i) {
                    None
                } else {
                    Some(
                        NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .checked_add_days(chrono::Days::new(
                                immigration_date_array.value(i) as u64
                            ))
                            .unwrap(),
                    )
                },
            };

            individuals.push(individual);
        }

        Ok(individuals)
    }

    /// Convert a vector of Individual objects to a `RecordBatch`
    fn to_record_batch(_individuals: &[Self]) -> Result<RecordBatch> {
        // Implementation of conversion to RecordBatch
        // This would create Arrow arrays for each field and then combine them
        // For brevity, this is left as a placeholder
        unimplemented!("Conversion to RecordBatch not yet implemented")
    }
}
