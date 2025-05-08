//! Individual entity model
//!
//! This module contains the core Individual entity structure which is central to the study design.
//! An Individual represents any person in the study, and can be associated with various roles
//! such as parent or child.

use crate::common::traits::{BefRegistry, RegistryAware};
use crate::error::Result;
use crate::models::traits::{ArrowSchema, EntityModel, HealthStatus, TemporalValidity};
use crate::models::types::{
    CitizenshipStatus, EducationField, EducationLevel, Gender, HousingType,
    MaritalStatus, Origin, SocioeconomicStatus,
};
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
    
    // Employment and socioeconomic status
    /// Socioeconomic status classification
    pub socioeconomic_status: SocioeconomicStatus,
    /// Primary occupation code (DISCO-08)
    pub occupation_code: Option<String>,
    /// Industry code (DB07)
    pub industry_code: Option<String>,
    /// Primary workplace ID
    pub workplace_id: Option<String>,
    /// Employment start date
    pub employment_start_date: Option<NaiveDate>,
    /// Weekly working hours
    pub working_hours: Option<f64>,
    
    // Education details
    /// Primary field of education
    pub education_field: EducationField,
    /// Most recent education completion date
    pub education_completion_date: Option<NaiveDate>,
    /// Institution code for highest education
    pub education_institution: Option<String>,
    /// Educational program code (AUDD)
    pub education_program_code: Option<String>,
    
    // Income information
    /// Annual income (DKK)
    pub annual_income: Option<f64>,
    /// Disposable income after tax (DKK)
    pub disposable_income: Option<f64>,
    /// Income from employment (DKK)
    pub employment_income: Option<f64>,
    /// Income from self-employment (DKK)
    pub self_employment_income: Option<f64>,
    /// Capital income (DKK)
    pub capital_income: Option<f64>,
    /// Transfer income (social benefits, pensions, etc.) (DKK)
    pub transfer_income: Option<f64>,
    /// Income year
    pub income_year: Option<i32>,
    
    // Healthcare usage
    /// Number of hospital admissions in past year
    pub hospital_admissions_count: Option<i32>,
    /// Number of emergency room visits in past year
    pub emergency_visits_count: Option<i32>,
    /// Number of outpatient visits in past year
    pub outpatient_visits_count: Option<i32>,
    /// Number of GP contacts in past year
    pub gp_visits_count: Option<i32>,
    /// Date of most recent hospital admission
    pub last_hospital_admission_date: Option<NaiveDate>,
    /// Total hospitalization days in past year
    pub hospitalization_days: Option<i32>,
    
    // Additional demographic information
    /// Marital status
    pub marital_status: MaritalStatus,
    /// Citizenship status
    pub citizenship_status: CitizenshipStatus,
    /// Housing type
    pub housing_type: HousingType,
    /// Number of persons in household
    pub household_size: Option<i32>,
    /// Household type code
    pub household_type: Option<String>,
}

impl Individual {
    /// Create a new Individual with minimal required information
    #[must_use]
    pub fn new(pnr: String, gender: Gender, birth_date: Option<NaiveDate>) -> Self {
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
            
            // Initialize employment and socioeconomic status fields
            socioeconomic_status: SocioeconomicStatus::Unknown,
            occupation_code: None,
            industry_code: None,
            workplace_id: None,
            employment_start_date: None,
            working_hours: None,
            
            // Initialize education details fields
            education_field: EducationField::Unknown,
            education_completion_date: None,
            education_institution: None,
            education_program_code: None,
            
            // Initialize income information fields
            annual_income: None,
            disposable_income: None,
            employment_income: None,
            self_employment_income: None,
            capital_income: None,
            transfer_income: None,
            income_year: None,
            
            // Initialize healthcare usage fields
            hospital_admissions_count: None,
            emergency_visits_count: None,
            outpatient_visits_count: None,
            gp_visits_count: None,
            last_hospital_admission_date: None,
            hospitalization_days: None,
            
            // Initialize additional demographic information fields
            marital_status: MaritalStatus::Unknown,
            citizenship_status: CitizenshipStatus::Unknown,
            housing_type: HousingType::Unknown,
            household_size: None,
            household_type: None,
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
            // Core demographic fields
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
            
            // Employment and socioeconomic status fields
            Field::new("socioeconomic_status", DataType::Int32, false),
            Field::new("occupation_code", DataType::Utf8, true),
            Field::new("industry_code", DataType::Utf8, true),
            Field::new("workplace_id", DataType::Utf8, true),
            Field::new("employment_start_date", DataType::Date32, true),
            Field::new("working_hours", DataType::Float64, true),
            
            // Education details fields
            Field::new("education_field", DataType::Int32, false),
            Field::new("education_completion_date", DataType::Date32, true),
            Field::new("education_institution", DataType::Utf8, true),
            Field::new("education_program_code", DataType::Utf8, true),
            
            // Income information fields
            Field::new("annual_income", DataType::Float64, true),
            Field::new("disposable_income", DataType::Float64, true),
            Field::new("employment_income", DataType::Float64, true),
            Field::new("self_employment_income", DataType::Float64, true),
            Field::new("capital_income", DataType::Float64, true),
            Field::new("transfer_income", DataType::Float64, true),
            Field::new("income_year", DataType::Int32, true),
            
            // Healthcare usage fields
            Field::new("hospital_admissions_count", DataType::Int32, true),
            Field::new("emergency_visits_count", DataType::Int32, true),
            Field::new("outpatient_visits_count", DataType::Int32, true),
            Field::new("gp_visits_count", DataType::Int32, true),
            Field::new("last_hospital_admission_date", DataType::Date32, true),
            Field::new("hospitalization_days", DataType::Int32, true),
            
            // Additional demographic information fields
            Field::new("marital_status", DataType::Int32, false),
            Field::new("citizenship_status", DataType::Int32, false),
            Field::new("housing_type", DataType::Int32, false),
            Field::new("household_size", DataType::Int32, true),
            Field::new("household_type", DataType::Utf8, true),
        ])
    }

    /// Convert a `RecordBatch` to a vector of Individual objects
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Helper function to safely get an array from batch
        fn get_array<'a, T: arrow::array::Array + 'static>(
            batch: &'a RecordBatch,
            field_name: &str,
        ) -> Result<Option<&'a T>> {
            if let Ok(index) = batch.schema().index_of(field_name) {
                let array = batch.column(index);
                Ok(array.as_any().downcast_ref::<T>())
            } else {
                Ok(None)
            }
        }

        // Helper function to convert Date32 to NaiveDate
        fn date32_to_naive_date(days_since_epoch: i32) -> Option<NaiveDate> {
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_days(chrono::Days::new(days_since_epoch as u64))
        }

        // Get core arrays (required)
        let pnr_array = get_array::<StringArray>(batch, "pnr")?.ok_or_else(|| {
            anyhow::anyhow!("Required field 'pnr' not found in batch")
        })?;
        
        // Get gender array (required)
        let gender_array = get_array::<Int32Array>(batch, "gender")?.ok_or_else(|| {
            anyhow::anyhow!("Required field 'gender' not found in batch")
        })?;
        
        // Get all arrays with optional presence
        let birth_date_array = get_array::<Date32Array>(batch, "birth_date")?;
        let death_date_array = get_array::<Date32Array>(batch, "death_date")?;
        let origin_array = get_array::<Int32Array>(batch, "origin")?;
        let education_level_array = get_array::<Int32Array>(batch, "education_level")?;
        let municipality_code_array = get_array::<StringArray>(batch, "municipality_code")?;
        let is_rural_array = get_array::<BooleanArray>(batch, "is_rural")?;
        let mother_pnr_array = get_array::<StringArray>(batch, "mother_pnr")?;
        let father_pnr_array = get_array::<StringArray>(batch, "father_pnr")?;
        let family_id_array = get_array::<StringArray>(batch, "family_id")?;
        let emigration_date_array = get_array::<Date32Array>(batch, "emigration_date")?;
        let immigration_date_array = get_array::<Date32Array>(batch, "immigration_date")?;
        
        // Employment and socioeconomic status arrays
        let socioeconomic_status_array = get_array::<Int32Array>(batch, "socioeconomic_status")?;
        let occupation_code_array = get_array::<StringArray>(batch, "occupation_code")?;
        let industry_code_array = get_array::<StringArray>(batch, "industry_code")?;
        let workplace_id_array = get_array::<StringArray>(batch, "workplace_id")?;
        let employment_start_date_array = get_array::<Date32Array>(batch, "employment_start_date")?;
        let working_hours_array = get_array::<arrow::array::Float64Array>(batch, "working_hours")?;
        
        // Education details arrays
        let education_field_array = get_array::<Int32Array>(batch, "education_field")?;
        let education_completion_date_array = get_array::<Date32Array>(batch, "education_completion_date")?;
        let education_institution_array = get_array::<StringArray>(batch, "education_institution")?;
        let education_program_code_array = get_array::<StringArray>(batch, "education_program_code")?;
        
        // Income information arrays
        let annual_income_array = get_array::<arrow::array::Float64Array>(batch, "annual_income")?;
        let disposable_income_array = get_array::<arrow::array::Float64Array>(batch, "disposable_income")?;
        let employment_income_array = get_array::<arrow::array::Float64Array>(batch, "employment_income")?;
        let self_employment_income_array = get_array::<arrow::array::Float64Array>(batch, "self_employment_income")?;
        let capital_income_array = get_array::<arrow::array::Float64Array>(batch, "capital_income")?;
        let transfer_income_array = get_array::<arrow::array::Float64Array>(batch, "transfer_income")?;
        let income_year_array = get_array::<Int32Array>(batch, "income_year")?;
        
        // Healthcare usage arrays
        let hospital_admissions_count_array = get_array::<Int32Array>(batch, "hospital_admissions_count")?;
        let emergency_visits_count_array = get_array::<Int32Array>(batch, "emergency_visits_count")?;
        let outpatient_visits_count_array = get_array::<Int32Array>(batch, "outpatient_visits_count")?;
        let gp_visits_count_array = get_array::<Int32Array>(batch, "gp_visits_count")?;
        let last_hospital_admission_date_array = get_array::<Date32Array>(batch, "last_hospital_admission_date")?;
        let hospitalization_days_array = get_array::<Int32Array>(batch, "hospitalization_days")?;
        
        // Additional demographic information arrays
        let marital_status_array = get_array::<Int32Array>(batch, "marital_status")?;
        let citizenship_status_array = get_array::<Int32Array>(batch, "citizenship_status")?;
        let housing_type_array = get_array::<Int32Array>(batch, "housing_type")?;
        let household_size_array = get_array::<Int32Array>(batch, "household_size")?;
        let household_type_array = get_array::<StringArray>(batch, "household_type")?;

        let mut individuals = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let mut individual = Individual::new(
                pnr_array.value(i).to_string(),
                // Use gender from array or default to Unknown
                if i < gender_array.len() && !gender_array.is_null(i) {
                    Gender::from(gender_array.value(i))
                } else {
                    Gender::Unknown
                },
                // Birth date
                if let Some(array) = birth_date_array {
                    if i < array.len() && !array.is_null(i) {
                        date32_to_naive_date(array.value(i))
                    } else {
                        None
                    }
                } else {
                    None
                },
            );

            // Set death date if available
            if let Some(array) = death_date_array {
                if i < array.len() && !array.is_null(i) {
                    individual.death_date = date32_to_naive_date(array.value(i));
                }
            }

            // Set origin if available
            if let Some(array) = origin_array {
                if i < array.len() && !array.is_null(i) {
                    individual.origin = Origin::from(array.value(i));
                }
            }

            // Set education level if available
            if let Some(array) = education_level_array {
                if i < array.len() && !array.is_null(i) {
                    individual.education_level = EducationLevel::from(array.value(i));
                }
            }

            // Set municipality code if available
            if let Some(array) = municipality_code_array {
                if i < array.len() && !array.is_null(i) {
                    individual.municipality_code = Some(array.value(i).to_string());
                }
            }

            // Set is_rural if available
            if let Some(array) = is_rural_array {
                if i < array.len() && !array.is_null(i) {
                    individual.is_rural = array.value(i);
                }
            }

            // Set mother_pnr if available
            if let Some(array) = mother_pnr_array {
                if i < array.len() && !array.is_null(i) {
                    individual.mother_pnr = Some(array.value(i).to_string());
                }
            }

            // Set father_pnr if available
            if let Some(array) = father_pnr_array {
                if i < array.len() && !array.is_null(i) {
                    individual.father_pnr = Some(array.value(i).to_string());
                }
            }

            // Set family_id if available
            if let Some(array) = family_id_array {
                if i < array.len() && !array.is_null(i) {
                    individual.family_id = Some(array.value(i).to_string());
                }
            }

            // Set emigration_date if available
            if let Some(array) = emigration_date_array {
                if i < array.len() && !array.is_null(i) {
                    individual.emigration_date = date32_to_naive_date(array.value(i));
                }
            }

            // Set immigration_date if available
            if let Some(array) = immigration_date_array {
                if i < array.len() && !array.is_null(i) {
                    individual.immigration_date = date32_to_naive_date(array.value(i));
                }
            }
            
            // Set employment and socioeconomic status fields if available
            if let Some(array) = socioeconomic_status_array {
                if i < array.len() && !array.is_null(i) {
                    individual.socioeconomic_status = SocioeconomicStatus::from(array.value(i));
                }
            }
            
            if let Some(array) = occupation_code_array {
                if i < array.len() && !array.is_null(i) {
                    individual.occupation_code = Some(array.value(i).to_string());
                }
            }
            
            if let Some(array) = industry_code_array {
                if i < array.len() && !array.is_null(i) {
                    individual.industry_code = Some(array.value(i).to_string());
                }
            }
            
            if let Some(array) = workplace_id_array {
                if i < array.len() && !array.is_null(i) {
                    individual.workplace_id = Some(array.value(i).to_string());
                }
            }
            
            if let Some(array) = employment_start_date_array {
                if i < array.len() && !array.is_null(i) {
                    individual.employment_start_date = date32_to_naive_date(array.value(i));
                }
            }
            
            if let Some(array) = working_hours_array {
                if i < array.len() && !array.is_null(i) {
                    individual.working_hours = Some(array.value(i));
                }
            }
            
            // Set education details fields if available
            if let Some(array) = education_field_array {
                if i < array.len() && !array.is_null(i) {
                    individual.education_field = EducationField::from(array.value(i));
                }
            }
            
            if let Some(array) = education_completion_date_array {
                if i < array.len() && !array.is_null(i) {
                    individual.education_completion_date = date32_to_naive_date(array.value(i));
                }
            }
            
            if let Some(array) = education_institution_array {
                if i < array.len() && !array.is_null(i) {
                    individual.education_institution = Some(array.value(i).to_string());
                }
            }
            
            if let Some(array) = education_program_code_array {
                if i < array.len() && !array.is_null(i) {
                    individual.education_program_code = Some(array.value(i).to_string());
                }
            }
            
            // Set income information fields if available
            if let Some(array) = annual_income_array {
                if i < array.len() && !array.is_null(i) {
                    individual.annual_income = Some(array.value(i));
                }
            }
            
            if let Some(array) = disposable_income_array {
                if i < array.len() && !array.is_null(i) {
                    individual.disposable_income = Some(array.value(i));
                }
            }
            
            if let Some(array) = employment_income_array {
                if i < array.len() && !array.is_null(i) {
                    individual.employment_income = Some(array.value(i));
                }
            }
            
            if let Some(array) = self_employment_income_array {
                if i < array.len() && !array.is_null(i) {
                    individual.self_employment_income = Some(array.value(i));
                }
            }
            
            if let Some(array) = capital_income_array {
                if i < array.len() && !array.is_null(i) {
                    individual.capital_income = Some(array.value(i));
                }
            }
            
            if let Some(array) = transfer_income_array {
                if i < array.len() && !array.is_null(i) {
                    individual.transfer_income = Some(array.value(i));
                }
            }
            
            if let Some(array) = income_year_array {
                if i < array.len() && !array.is_null(i) {
                    individual.income_year = Some(array.value(i));
                }
            }
            
            // Set healthcare usage fields if available
            if let Some(array) = hospital_admissions_count_array {
                if i < array.len() && !array.is_null(i) {
                    individual.hospital_admissions_count = Some(array.value(i));
                }
            }
            
            if let Some(array) = emergency_visits_count_array {
                if i < array.len() && !array.is_null(i) {
                    individual.emergency_visits_count = Some(array.value(i));
                }
            }
            
            if let Some(array) = outpatient_visits_count_array {
                if i < array.len() && !array.is_null(i) {
                    individual.outpatient_visits_count = Some(array.value(i));
                }
            }
            
            if let Some(array) = gp_visits_count_array {
                if i < array.len() && !array.is_null(i) {
                    individual.gp_visits_count = Some(array.value(i));
                }
            }
            
            if let Some(array) = last_hospital_admission_date_array {
                if i < array.len() && !array.is_null(i) {
                    individual.last_hospital_admission_date = date32_to_naive_date(array.value(i));
                }
            }
            
            if let Some(array) = hospitalization_days_array {
                if i < array.len() && !array.is_null(i) {
                    individual.hospitalization_days = Some(array.value(i));
                }
            }
            
            // Set additional demographic information fields if available
            if let Some(array) = marital_status_array {
                if i < array.len() && !array.is_null(i) {
                    individual.marital_status = MaritalStatus::from(array.value(i));
                }
            }
            
            if let Some(array) = citizenship_status_array {
                if i < array.len() && !array.is_null(i) {
                    individual.citizenship_status = CitizenshipStatus::from(array.value(i));
                }
            }
            
            if let Some(array) = housing_type_array {
                if i < array.len() && !array.is_null(i) {
                    individual.housing_type = HousingType::from(array.value(i));
                }
            }
            
            if let Some(array) = household_size_array {
                if i < array.len() && !array.is_null(i) {
                    individual.household_size = Some(array.value(i));
                }
            }
            
            if let Some(array) = household_type_array {
                if i < array.len() && !array.is_null(i) {
                    individual.household_type = Some(array.value(i).to_string());
                }
            }

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
