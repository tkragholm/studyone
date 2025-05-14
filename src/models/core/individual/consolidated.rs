//! Core Individual entity definition
//!
//! This module contains the consolidated Individual struct definition with all functionality.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::registry_traits::{DodFields, IndFields, LprFields};
use crate::models::core::traits::{ArrowSchema, EntityModel, HealthStatus, TemporalValidity};

use arrow::datatypes::{Field, Schema};

use arrow::array::Array;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use std::default::Default;

/// Role of an individual in the study context
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Child role (subject of study)
    Child,
    /// Parent role (mother or father)
    Parent,
    /// Both child and parent roles
    ChildAndParent,
    /// Other role (relative, etc.)
    Other,
}

/// Core Individual entity representing a person in the study
///
/// A single unified struct that handles both storage and serde operations
/// All BEF derived dates are in the following format: DD/MM/YYYY
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Individual {
    // Identifiers
    /// Personal identification number (PNR)
    #[serde(alias = "PNR")]
    pub pnr: String,
    /// Mother's PNR, if known
    #[serde(alias = "MOR_ID")]
    pub mother_pnr: Option<String>,
    /// Father's PNR, if known
    #[serde(alias = "FAR_ID")]
    pub father_pnr: Option<String>,
    /// Family identifier
    #[serde(alias = "FAMILIE_ID")]
    pub family_id: Option<String>,
    /// Spouse's personal identification number
    #[serde(alias = "E_FAELLE_ID")]
    pub spouse_pnr: Option<String>,

    // Core characteristics
    /// Gender of the individual
    #[serde(alias = "KOEN")]
    pub gender: Option<String>,
    /// Birth date
    #[serde(alias = "FOED_DAG")]
    pub birth_date: Option<NaiveDate>,
    /// Death date, if applicable
    #[serde(alias = "DODDATO")]
    pub death_date: Option<NaiveDate>,
    /// Age
    #[serde(skip)]
    pub age: Option<i32>,

    // Background
    /// Geographic origin category
    #[serde(alias = "OPR_LAND")]
    pub origin: Option<String>,
    /// Citizenship status
    #[serde(alias = "STATSB")]
    pub citizenship_status: Option<String>,

    /// Immigration type
    /// 1: People of danish origin
    /// 2: Immigrants
    /// 3: Descendants
    #[serde(alias = "IE_TYPE")]
    pub immigration_type: Option<String>,

    /// Marital status
    /// D: Death
    /// E: Widow/Widower
    /// F: Sign
    /// G: Married (+ separated)
    /// L: Longest surviving of 2 partners
    /// O: Dissolved partnership
    /// P: Registered partnership
    /// U: Unmarried
    /// 9: Undisclosed marital status
    #[serde(alias = "CIVST")]
    pub marital_status: Option<String>,
    /// Marital date
    #[serde(alias = "CIV_VFRA")]
    pub marital_date: Option<NaiveDate>,

    // Basic demographic information
    /// Municipality code
    #[serde(alias = "KOM")]
    pub municipality_code: Option<String>,
    /// Regional code
    /// 0: Uoplyst
    /// 81: Nordjylland
    /// 82: Midtjylland
    /// 83: Syddanmark
    /// 84: Hovedstaden
    /// 85: Sjælland
    #[serde(alias = "REG")]
    pub regional_code: Option<String>,

    /// Whether the individual lives in a rural area
    #[serde(skip_deserializing, default)]
    pub is_rural: bool,

    /// Housing type
    /// 1: Single man
    /// 2: Single woman
    /// 3: Married couple
    /// 4: Other couple
    /// 5: Ikke hjemmeboende børn (under 18 år)
    /// 6: Andre husstande bestående af flere familier
    #[serde(alias = "HUSTYPE")]
    pub household_type: Option<i8>,

    /// Number of persons in household
    #[serde(alias = "ANTPERSF")]
    pub family_size: Option<i32>,

    /// Family size (number of persons in family)
    #[serde(alias = "ANTPERSH")]
    pub household_size: Option<i32>,

    // BEF registry specific fields
    /// Date of residence from
    /// Dato for tilflytning/indvandring
    #[serde(alias = "BOP_VFRA")]
    pub residence_from: Option<NaiveDate>,

    /// Position in family
    /// 1: Hovedperson
    /// 2: Ægtefælle/partner
    /// 3: Hjemmeboende barn
    #[serde(alias = "PLADS")]
    pub position_in_family: Option<i32>,

    /// Family type
    pub family_type: Option<i32>,

    // Migration information
    /// Event type, if applicable
    #[serde(alias = "INDUD_KODE")]
    pub event_type: Option<String>,
    /// Event date, if applicable
    #[serde(alias = "HAEND_DATO")]
    pub event_date: Option<NaiveDate>,

    // Education information
    /// Primary field of education (HFAUDD)
    #[serde(alias = "HFAUDD")]
    pub education_code: Option<u16>,
    /// Most recent education completion date
    #[serde(alias = "HF_VFRA")]
    pub education_valid_from: Option<NaiveDate>,
    /// Education start date
    #[serde(alias = "HF_VTIL")]
    pub education_valid_to: Option<NaiveDate>,
    /// Institution code for highest education
    #[serde(alias = "INSTNR")]
    pub education_institution: Option<i32>,
    /// Source of information
    #[serde(alias = "HF_KILDE")]
    pub education_source: Option<u8>,

    /// Education level
    #[serde(skip_deserializing, default)]
    pub education_level: i8,

    // Employment and socioeconomic status
    /// Socioeconomic status classification
    #[serde(alias = "SOCIO13")]
    pub socioeconomic_status: Option<i32>,

    // Income information
    /// Annual income (DKK)
    #[serde(alias = "PERINDKIALT_13")]
    pub annual_income: Option<f64>,
    /// Income from employment (DKK)
    #[serde(alias = "LOENMV_13")]
    pub employment_income: Option<f64>,
    /// Income year
    #[serde(alias = "AAR")]
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
    #[serde(alias = "D_INDDTO")]
    pub last_hospital_admission_date: Option<NaiveDate>,

    /// Total hospitalization days in past year
    #[serde(alias = "LIGGETID")]
    pub hospitalization_days: Option<i32>,

    /// Length of stay in days (for current/last admission)
    pub length_of_stay: Option<i32>,

    /// All diagnoses associated with this individual
    pub diagnoses: Option<Vec<String>>,

    /// All procedures performed on this individual
    pub procedures: Option<Vec<String>>,

    /// All hospital admission dates
    pub hospital_admissions: Option<Vec<NaiveDate>>,

    /// All hospital discharge dates
    pub discharge_dates: Option<Vec<NaiveDate>>,

    /// Death cause code
    pub death_cause: Option<String>,

    /// Underlying death cause
    pub underlying_death_cause: Option<String>,

    // MFR registry specific fields
    /// Birth weight in grams
    pub birth_weight: Option<i32>,

    /// Birth length in cm
    pub birth_length: Option<i32>,

    /// Gestational age in weeks
    pub gestational_age: Option<i32>,

    /// APGAR score at 5 minutes
    pub apgar_score: Option<i32>,

    /// Birth order for multiple births
    pub birth_order: Option<i32>,

    /// Plurality (number of fetuses in this pregnancy)
    pub plurality: Option<i32>,
}

// Implement Default for Individual
impl Default for Individual {
    fn default() -> Self {
        Self::default_impl()
    }
}

// Core Individual methods
impl Individual {
    /// Create a new Individual with minimal required information
    #[must_use]
    pub fn new(pnr: String, birth_date: Option<NaiveDate>) -> Self {
        // Start with default values for all fields
        let mut individual = Self::default();
        
        // Set the required fields
        individual.pnr = pnr;
        individual.birth_date = birth_date;
        
        individual
    }
    
    /// Create a default Individual instance
    fn default_impl() -> Self {
        Self {
            // Core identification
            pnr: String::new(),
            birth_date: None,
            gender: None,
            death_date: None,
            origin: None,
            age: None,

            // Basic demographic information
            municipality_code: None,
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: None,
            marital_status: None,
            marital_date: None,
            citizenship_status: None,
            regional_code: None,
            household_size: None,
            household_type: None,
            immigration_type: None,

            // Education information
            education_level: -1, // Unknown
            education_institution: None,
            education_code: None,
            education_source: None,
            education_valid_to: None,
            education_valid_from: None,

            // Employment and socioeconomic status
            socioeconomic_status: None,

            // Income information
            annual_income: None,
            employment_income: None,
            income_year: None,

            // Healthcare usage
            hospital_admissions_count: None,
            emergency_visits_count: None,
            outpatient_visits_count: None,
            gp_visits_count: None,
            last_hospital_admission_date: None,
            hospitalization_days: None,
            length_of_stay: None,
            diagnoses: None,
            procedures: None,
            hospital_admissions: None,
            discharge_dates: None,
            death_cause: None,
            underlying_death_cause: None,

            // Migration information
            event_date: None,
            event_type: None,

            // BEF registry specific fields
            spouse_pnr: None,
            family_size: None,
            residence_from: None,
            position_in_family: None,
            family_type: None,

            // MFR registry specific fields
            birth_weight: None,
            birth_length: None,
            gestational_age: None,
            apgar_score: None,
            birth_order: None,
            plurality: None,
        }
    }

    /// Set a property value by name
    ///
    /// This method is used by the registry deserializer to set property values
    /// dynamically, primarily for the procedural macro system.
    pub fn set_property(&mut self, property: &str, value: Box<dyn std::any::Any>) {
        // Handle common field types
        match property {
            "pnr" => {
                if let Some(v) = value.downcast_ref::<String>() {
                    self.pnr = v.clone();
                }
            }
            "gender" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.gender = v.clone();
                }
            }
            "birth_date" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    self.birth_date = *v;
                }
            }
            "death_date" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    self.death_date = *v;
                }
            }
            "mother_pnr" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.mother_pnr = v.clone();
                }
            }
            "father_pnr" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.father_pnr = v.clone();
                }
            }
            "event_type" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.event_type = v.clone();
                }
            }
            "event_date" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    self.event_date = *v;
                }
            }
            // LPR specific fields
            "action_diagnosis" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    if let Some(ref mut diagnoses) = self.diagnoses {
                        if let Some(diagnosis) = v {
                            diagnoses.push(diagnosis.clone());
                        }
                    } else if let Some(diagnosis) = v {
                        self.diagnoses = Some(vec![diagnosis.clone()]);
                    }
                }
            }
            "diagnosis_code" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    if let Some(ref mut diagnoses) = self.diagnoses {
                        if let Some(diagnosis) = v {
                            diagnoses.push(diagnosis.clone());
                        }
                    } else if let Some(diagnosis) = v {
                        self.diagnoses = Some(vec![diagnosis.clone()]);
                    }
                }
            }
            "diagnosis_type" => {
                // Store as a property if needed in the future
                // Currently we don't have a dedicated field for this in Individual
            }
            "record_number" => {
                // Store as a property if needed in the future
                // Currently we don't have a dedicated field for this in Individual
            }
            "department_code" => {
                // Store as a property if needed in the future
            }
            "municipality_code" => {
                if let Some(v) = value.downcast_ref::<Option<String>>() {
                    self.municipality_code = v.clone();
                }
            }
            "admission_date" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    if let Some(date) = *v {
                        if let Some(ref mut admissions) = self.hospital_admissions {
                            admissions.push(date);
                        } else {
                            self.hospital_admissions = Some(vec![date]);
                        }
                        // Also update last admission date if newer
                        if let Some(last_date) = self.last_hospital_admission_date {
                            if date > last_date {
                                self.last_hospital_admission_date = Some(date);
                            }
                        } else {
                            self.last_hospital_admission_date = Some(date);
                        }
                    }
                }
            }
            "discharge_date" => {
                if let Some(v) = value.downcast_ref::<Option<NaiveDate>>() {
                    if let Some(date) = *v {
                        if let Some(ref mut discharges) = self.discharge_dates {
                            discharges.push(date);
                        } else {
                            self.discharge_dates = Some(vec![date]);
                        }
                    }
                }
            }
            "age" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.age = *v;
                }
            }
            "length_of_stay" => {
                if let Some(v) = value.downcast_ref::<Option<i32>>() {
                    self.length_of_stay = *v;
                }
            }
            // Add more field mappings as needed
            _ => {
                // For fields not explicitly handled, log a debug message
                eprintln!("Property not handled: {property}");
            }
        }
    }

    /// Compute rural status from municipality code
    pub fn compute_rural_status(&mut self) {
        if let Some(code) = &self.municipality_code {
            let code_num = code.parse::<i32>().unwrap_or(0);
            // Rural areas often have municipality codes in specific ranges
            self.is_rural = !(400..=600).contains(&code_num);
        }
    }

    /// Convert directly from a `RecordBatch` using `serde_arrow`
    pub fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        match serde_arrow::from_record_batch::<Vec<Self>>(batch) {
            Ok(mut individuals) => {
                // Compute any derived fields if needed
                for individual in &mut individuals {
                    individual.compute_rural_status();
                }
                Ok(individuals)
            }
            Err(e) => Err(anyhow::anyhow!("Failed to deserialize: {}", e)),
        }
    }

    /// Calculate the age of the individual at a given reference date
    pub fn calculate_age(&mut self, reference_date: NaiveDate) {
        if let Some(birth_date) = self.birth_date {
            // Simple calculation based on year difference
            let birth_year = birth_date.year();
            let reference_year = reference_date.year();

            // Basic age calculation
            let mut age = reference_year - birth_year;

            // Adjust if birthday hasn't occurred yet this year
            let birth_month_day = (birth_date.month(), birth_date.day());
            let reference_month_day = (reference_date.month(), reference_date.day());

            if reference_month_day < birth_month_day {
                age -= 1;
            }

            self.age = Some(age);
        }
    }

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
    // pub fn enhance_from_registry(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
    //     // First check if the PNR matches
    //     if !self.pnr_matches_record(batch, row)? {
    //         return Ok(false);
    //     }

    //     // Deserialize a new Individual from the registry record
    //     if let Some(enhanced_individual) =
    //         crate::registry::trait_deserializer::deserialize_row(batch, row)?
    //     {
    //         // Merge fields from the enhanced individual into self, but only if they're not already set
    //         self.merge_fields(&enhanced_individual);
    //         Ok(true)
    //     } else {
    //         Ok(false)
    //     }
    // }

    /// Check if this Individual's PNR matches the PNR in a registry record
    pub fn pnr_matches_record(&self, batch: &RecordBatch, row: usize) -> Result<bool> {
        use crate::utils::array_utils::{downcast_array, get_column};

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
    #[allow(dead_code)]
    fn merge_fields(&mut self, source: &Self) {
        // Only copy fields if they're not already set
        if self.gender.is_none() {
            self.gender = source.gender.clone();
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

        // Copy all other fields using the same pattern
        if self.origin.is_none() {
            self.origin = source.origin.clone();
        }

        if self.municipality_code.is_none() {
            self.municipality_code = source.municipality_code.clone();
            // Recompute rural status if municipality code was updated
            self.compute_rural_status();
        }

        if self.household_size.is_none() {
            self.household_size = source.household_size;
        }

        if self.household_type.is_none() {
            self.household_type = source.household_type;
        }

        if self.marital_status.is_none() {
            self.marital_status = source.marital_status.clone();
        }

        if self.citizenship_status.is_none() {
            self.citizenship_status = source.citizenship_status.clone();
        }

        if self.education_level == -1 {
            self.education_level = source.education_level;
        }

        if self.education_code.is_none() {
            self.education_code = source.education_code;
        }

        if self.education_valid_from.is_none() {
            self.education_valid_from = source.education_valid_from;
        }

        if self.education_valid_to.is_none() {
            self.education_valid_to = source.education_valid_to;
        }

        if self.education_institution.is_none() {
            self.education_institution = source.education_institution;
        }

        if self.education_source.is_none() {
            self.education_source = source.education_source;
        }

        // Employment fields
        if self.socioeconomic_status.is_none() {
            self.socioeconomic_status = source.socioeconomic_status;
        }

        // Income fields
        if self.annual_income.is_none() {
            self.annual_income = source.annual_income;
        }

        if self.employment_income.is_none() {
            self.employment_income = source.employment_income;
        }

        if self.income_year.is_none() {
            self.income_year = source.income_year;
        }

        // Healthcare fields
        if self.hospital_admissions_count.is_none() {
            self.hospital_admissions_count = source.hospital_admissions_count;
        }

        if self.emergency_visits_count.is_none() {
            self.emergency_visits_count = source.emergency_visits_count;
        }

        if self.outpatient_visits_count.is_none() {
            self.outpatient_visits_count = source.outpatient_visits_count;
        }

        if self.gp_visits_count.is_none() {
            self.gp_visits_count = source.gp_visits_count;
        }

        if self.last_hospital_admission_date.is_none() {
            self.last_hospital_admission_date = source.last_hospital_admission_date;
        }

        if self.hospitalization_days.is_none() {
            self.hospitalization_days = source.hospitalization_days;
        }

        if self.length_of_stay.is_none() {
            self.length_of_stay = source.length_of_stay;
        }

        if self.diagnoses.is_none() {
            self.diagnoses = source.diagnoses.clone();
        }

        if self.procedures.is_none() {
            self.procedures = source.procedures.clone();
        }

        if self.hospital_admissions.is_none() {
            self.hospital_admissions = source.hospital_admissions.clone();
        }

        if self.discharge_dates.is_none() {
            self.discharge_dates = source.discharge_dates.clone();
        }

        if self.death_cause.is_none() {
            self.death_cause = source.death_cause.clone();
        }

        if self.underlying_death_cause.is_none() {
            self.underlying_death_cause = source.underlying_death_cause.clone();
        }

        // Migration fields
        if self.event_date.is_none() {
            self.event_date = source.event_date;
        }

        if self.event_type.is_none() {
            self.event_type = source.event_type.clone();
        }

        // BEF fields
        if self.spouse_pnr.is_none() {
            self.spouse_pnr = source.spouse_pnr.clone();
        }

        if self.family_size.is_none() {
            self.family_size = source.family_size;
        }

        if self.residence_from.is_none() {
            self.residence_from = source.residence_from;
        }

        if self.position_in_family.is_none() {
            self.position_in_family = source.position_in_family;
        }

        if self.family_type.is_none() {
            self.family_type = source.family_type;
        }

        // MFR fields
        if self.birth_weight.is_none() {
            self.birth_weight = source.birth_weight;
        }

        if self.birth_length.is_none() {
            self.birth_length = source.birth_length;
        }

        if self.gestational_age.is_none() {
            self.gestational_age = source.gestational_age;
        }

        if self.apgar_score.is_none() {
            self.apgar_score = source.apgar_score;
        }

        if self.birth_order.is_none() {
            self.birth_order = source.birth_order;
        }

        if self.plurality.is_none() {
            self.plurality = source.plurality;
        }
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

// The Default trait is already implemented at the top of the file
// using the default_impl() method.

// Implement family relationship methods
impl Individual {
    /// Check if this Individual has a mother in the dataset
    #[must_use]
    pub const fn has_mother(&self) -> bool {
        self.mother_pnr.is_some()
    }

    /// Check if this Individual has a father in the dataset
    #[must_use]
    pub const fn has_father(&self) -> bool {
        self.father_pnr.is_some()
    }

    /// Check if this Individual has both parents in the dataset
    #[must_use]
    pub const fn has_both_parents(&self) -> bool {
        self.has_mother() && self.has_father()
    }

    /// Check if this Individual has either parent in the dataset
    #[must_use]
    pub const fn has_any_parent(&self) -> bool {
        self.has_mother() || self.has_father()
    }
}

// Implement the DodFields trait for Individual
impl DodFields for Individual {
    fn death_date(&self) -> Option<NaiveDate> {
        self.death_date
    }

    fn set_death_date(&mut self, value: Option<NaiveDate>) {
        self.death_date = value;
    }

    fn death_cause(&self) -> Option<&str> {
        self.death_cause.as_deref()
    }

    fn set_death_cause(&mut self, value: Option<String>) {
        self.death_cause = value;
    }

    fn underlying_death_cause(&self) -> Option<&str> {
        self.underlying_death_cause.as_deref()
    }

    fn set_underlying_death_cause(&mut self, value: Option<String>) {
        self.underlying_death_cause = value;
    }
}

// Implement ArrowSchema for Individual
impl ArrowSchema for Individual {
    /// Get the Arrow schema for this model
    fn schema() -> Schema {
        // Create a simplified schema with the most important fields
        let fields = vec![
            Field::new("pnr", DataType::Utf8, false),
            Field::new("birth_date", DataType::Date32, true),
            Field::new("death_date", DataType::Date32, true),
            Field::new("gender", DataType::Utf8, true),
            Field::new("mother_pnr", DataType::Utf8, true),
            Field::new("father_pnr", DataType::Utf8, true),
            // Add more fields as needed
        ];

        Schema::new(fields)
    }

    /// Convert a `RecordBatch` to a vector of Individual models
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // This is a placeholder implementation - a full implementation would
        // extract all individual fields from the batch
        let mut individuals = Vec::with_capacity(batch.num_rows());

        // Extract the PNR column
        if let Some(pnr_column) = batch.column_by_name("pnr") {
            if let Some(pnr_array) = pnr_column.as_any().downcast_ref::<StringArray>() {
                for i in 0..batch.num_rows() {
                    if !pnr_array.is_null(i) {
                        let pnr = pnr_array.value(i).to_string();
                        let individual = Self::new(pnr, None);
                        individuals.push(individual);
                    }
                }
            }
        }

        Ok(individuals)
    }

    /// Convert a vector of Individual models to a `RecordBatch`
    fn to_record_batch(_models: &[Self]) -> Result<RecordBatch> {
        // This is a placeholder - a full implementation would convert
        // all fields to Arrow arrays
        Err(anyhow::anyhow!("Not implemented yet"))
    }
}

// Implement the IndFields trait for Individual
impl IndFields for Individual {
    fn annual_income(&self) -> Option<f64> {
        self.annual_income
    }

    fn set_annual_income(&mut self, value: Option<f64>) {
        self.annual_income = value;
    }

    fn disposable_income(&self) -> Option<f64> {
        None // Not implemented in the Individual struct yet
    }

    fn set_disposable_income(&mut self, _value: Option<f64>) {
        // Not implemented in the Individual struct yet
    }

    fn employment_income(&self) -> Option<f64> {
        self.employment_income
    }

    fn set_employment_income(&mut self, value: Option<f64>) {
        self.employment_income = value;
    }

    fn self_employment_income(&self) -> Option<f64> {
        None // Not implemented in the Individual struct yet
    }

    fn set_self_employment_income(&mut self, _value: Option<f64>) {
        // Not implemented in the Individual struct yet
    }

    fn capital_income(&self) -> Option<f64> {
        None // Not implemented in the Individual struct yet
    }

    fn set_capital_income(&mut self, _value: Option<f64>) {
        // Not implemented in the Individual struct yet
    }

    fn transfer_income(&self) -> Option<f64> {
        None // Not implemented in the Individual struct yet
    }

    fn set_transfer_income(&mut self, _value: Option<f64>) {
        // Not implemented in the Individual struct yet
    }

    fn income_year(&self) -> Option<i32> {
        self.income_year
    }

    fn set_income_year(&mut self, value: Option<i32>) {
        self.income_year = value;
    }
}

// Implement TemporalValidity trait for Individual
impl TemporalValidity for Individual {
    /// Check if this entity was valid at a specific date
    fn was_valid_at(&self, date: &NaiveDate) -> bool {
        // For individuals, we consider them valid if they were born before or on the date
        // and either they haven't died yet or they died after the date
        match self.birth_date {
            Some(birth) => {
                birth <= *date
                    && match self.death_date {
                        Some(death) => death >= *date,
                        None => true, // No death date means still alive
                    }
            }
            None => false, // No birth date means we can't determine validity
        }
    }

    /// Get the start date of validity (birth date)
    fn valid_from(&self) -> NaiveDate {
        // Return birth date or a default date if not available
        self.birth_date
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1900, 1, 1).unwrap())
    }

    /// Get the end date of validity (death date if any)
    fn valid_to(&self) -> Option<NaiveDate> {
        self.death_date
    }

    /// Create a snapshot of this entity at a specific point in time
    fn snapshot_at(&self, date: &NaiveDate) -> Option<Self> {
        if self.was_valid_at(date) {
            Some(self.clone())
        } else {
            None
        }
    }
}

// Implement HealthStatus trait for Individual
impl HealthStatus for Individual {
    /// Check if the individual was alive at a specific date
    fn was_alive_at(&self, date: &NaiveDate) -> bool {
        // Same logic as was_valid_at
        match self.birth_date {
            Some(birth) => {
                birth <= *date
                    && match self.death_date {
                        Some(death) => death >= *date,
                        None => true, // No death date means still alive
                    }
            }
            None => false, // No birth date means we can't determine if alive
        }
    }

    /// Check if the individual was resident in Denmark at a specific date
    fn was_resident_at(&self, date: &NaiveDate) -> bool {
        // For simplicity, we assume residency if the individual was alive
        // A more accurate implementation would check migration events
        self.was_alive_at(date)
    }

    /// Calculate age at a specific reference date
    fn age_at(&self, reference_date: &NaiveDate) -> Option<i32> {
        match self.birth_date {
            Some(birth_date) => {
                if self.was_alive_at(reference_date) {
                    // Calculate years between birth date and reference date
                    let years = reference_date.year() - birth_date.year();

                    // Adjust for month and day (if birthday hasn't occurred yet this year)
                    let adjustment = if reference_date.month() < birth_date.month()
                        || (reference_date.month() == birth_date.month()
                            && reference_date.day() < birth_date.day())
                    {
                        1
                    } else {
                        0
                    };

                    Some(years - adjustment)
                } else {
                    None // Not alive at the reference date
                }
            }
            None => None, // No birth date available
        }
    }
}

// Implement the LprFields trait for Individual
impl LprFields for Individual {
    fn diagnoses(&self) -> Option<&[String]> {
        self.diagnoses.as_deref()
    }

    fn set_diagnoses(&mut self, value: Option<Vec<String>>) {
        self.diagnoses = value;
    }

    fn add_diagnosis(&mut self, diagnosis: String) {
        if let Some(diagnoses) = &mut self.diagnoses {
            diagnoses.push(diagnosis);
        } else {
            self.diagnoses = Some(vec![diagnosis]);
        }
    }

    fn procedures(&self) -> Option<&[String]> {
        self.procedures.as_deref()
    }

    fn set_procedures(&mut self, value: Option<Vec<String>>) {
        self.procedures = value;
    }

    fn add_procedure(&mut self, procedure: String) {
        if let Some(procedures) = &mut self.procedures {
            procedures.push(procedure);
        } else {
            self.procedures = Some(vec![procedure]);
        }
    }

    fn hospital_admissions(&self) -> Option<&[NaiveDate]> {
        self.hospital_admissions.as_deref()
    }

    fn set_hospital_admissions(&mut self, value: Option<Vec<NaiveDate>>) {
        self.hospital_admissions = value;
    }

    fn add_hospital_admission(&mut self, date: NaiveDate) {
        if let Some(admissions) = &mut self.hospital_admissions {
            admissions.push(date);
        } else {
            self.hospital_admissions = Some(vec![date]);
        }
    }

    fn discharge_dates(&self) -> Option<&[NaiveDate]> {
        self.discharge_dates.as_deref()
    }

    fn set_discharge_dates(&mut self, value: Option<Vec<NaiveDate>>) {
        self.discharge_dates = value;
    }

    fn add_discharge_date(&mut self, date: NaiveDate) {
        if let Some(dates) = &mut self.discharge_dates {
            dates.push(date);
        } else {
            self.discharge_dates = Some(vec![date]);
        }
    }

    fn length_of_stay(&self) -> Option<i32> {
        self.length_of_stay
    }

    fn set_length_of_stay(&mut self, value: Option<i32>) {
        self.length_of_stay = value;
    }
}
