//! Core Individual entity definition
//!
//! This module contains the consolidated Individual struct definition with all functionality.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::individual::temporal::TimePeriod;
use crate::models::core::traits::EntityModel;
use macros::PropertyField;

use arrow::array::Array;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
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
#[derive(Debug, Serialize, Deserialize, PropertyField)]
pub struct Individual {
    // Identifiers
    /// Personal identification number (PNR)
    #[serde(alias = "PNR")]
    #[property(name = "pnr")]
    pub pnr: String,
    /// Mother's PNR, if known
    #[serde(alias = "MOR_ID")]
    #[property(name = "mother_pnr")]
    pub mother_pnr: Option<String>,
    /// Father's PNR, if known
    #[serde(alias = "FAR_ID")]
    #[property(name = "father_pnr")]
    pub father_pnr: Option<String>,
    /// Family identifier
    #[serde(alias = "FAMILIE_ID")]
    #[property(name = "family_id")]
    pub family_id: Option<String>,
    /// Spouse's personal identification number
    #[serde(alias = "E_FAELLE_ID")]
    #[property(name = "spouse_pnr")]
    pub spouse_pnr: Option<String>,

    /// Additional properties that don't have explicit fields
    #[serde(skip)]
    pub properties: Option<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>,

    // Core characteristics
    /// Gender of the individual
    #[serde(alias = "KOEN")]
    #[property(name = "gender")]
    pub gender: Option<String>,
    /// Birth date
    #[serde(alias = "FOED_DAG")]
    #[property(name = "birth_date")]
    pub birth_date: Option<NaiveDate>,
    /// Death date, if applicable
    #[serde(alias = "DODDATO")]
    #[property(name = "death_date")]
    pub death_date: Option<NaiveDate>,
    /// Age
    #[serde(skip)]
    #[property(name = "age")]
    pub age: Option<i32>,

    // Background
    /// Geographic origin category
    #[serde(alias = "OPR_LAND")]
    #[property(name = "origin")]
    pub origin: Option<String>,
    /// Citizenship status
    #[serde(alias = "STATSB")]
    #[property(name = "citizenship_status")]
    pub citizenship_status: Option<String>,

    /// Immigration type
    /// 1: People of danish origin
    /// 2: Immigrants
    /// 3: Descendants
    #[serde(alias = "IE_TYPE")]
    #[property(name = "immigration_type")]
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
    #[property(name = "marital_status")]
    pub marital_status: Option<String>,
    /// Marital date
    #[serde(alias = "CIV_VFRA")]
    #[property(name = "marital_date")]
    pub marital_date: Option<NaiveDate>,

    // Basic demographic information
    /// Municipality code
    #[serde(alias = "KOM")]
    #[property(name = "municipality_code")]
    pub municipality_code: Option<String>,
    /// Regional code
    /// 0: Uoplyst
    /// 81: Nordjylland
    /// 82: Midtjylland
    /// 83: Syddanmark
    /// 84: Hovedstaden
    /// 85: Sjælland
    #[serde(alias = "REG")]
    #[property(name = "regional_code")]
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
    #[property(name = "family_size")]
    pub family_size: Option<i32>,

    /// Family size (number of persons in family)
    #[serde(alias = "ANTPERSH")]
    #[property(name = "household_size")]
    pub household_size: Option<i32>,

    // BEF registry specific fields
    /// Date of residence from
    /// Dato for tilflytning/indvandring
    #[serde(alias = "BOP_VFRA")]
    #[property(name = "residence_from")]
    pub residence_from: Option<NaiveDate>,

    /// Position in family
    /// 1: Hovedperson
    /// 2: Ægtefælle/partner
    /// 3: Hjemmeboende barn
    #[serde(alias = "PLADS")]
    #[property(name = "position_in_family")]
    pub position_in_family: Option<i32>,

    /// Family type
    #[property(name = "family_type")]
    pub family_type: Option<i32>,

    // Migration information
    /// Event type, if applicable
    #[serde(alias = "INDUD_KODE")]
    #[property(name = "event_type")]
    pub event_type: Option<String>,
    /// Event date, if applicable
    #[serde(alias = "HAEND_DATO")]
    #[property(name = "event_date")]
    pub event_date: Option<NaiveDate>,

    // Education information
    /// Primary field of education (HFAUDD)
    #[serde(alias = "HFAUDD")]
    #[property(name = "education_code")]
    pub education_code: Option<u16>,
    /// Most recent education completion date
    #[serde(alias = "HF_VFRA")]
    #[property(name = "education_valid_from")]
    pub education_valid_from: Option<NaiveDate>,
    /// Education start date
    #[serde(alias = "HF_VTIL")]
    #[property(name = "education_valid_to")]
    pub education_valid_to: Option<NaiveDate>,
    /// Institution code for highest education
    #[serde(alias = "INSTNR")]
    #[property(name = "education_institution")]
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
    #[property(name = "socioeconomic_status")]
    pub socioeconomic_status: Option<i32>,

    // Income information
    /// Annual income (DKK)
    #[serde(alias = "PERINDKIALT_13")]
    #[property(name = "annual_income")]
    pub annual_income: Option<f64>,
    /// Income from employment (DKK)
    #[serde(alias = "LOENMV_13")]
    #[property(name = "employment_income")]
    pub employment_income: Option<f64>,
    /// Income year
    #[serde(alias = "AAR")]
    #[property(name = "income_year")]
    pub income_year: Option<i32>,

    /// Time period information for each registry
    /// Maps registry name to a sorted map of time periods with their associated data
    #[serde(skip)]
    pub time_periods: HashMap<String, BTreeMap<TimePeriod, String>>,

    /// Current time period being processed - used during loading
    #[serde(skip)]
    pub current_time_period: Option<(String, TimePeriod)>,

    // Healthcare usage
    /// Number of hospital admissions in past year
    #[property(name = "hospital_admissions_count")]
    pub hospital_admissions_count: Option<i32>,

    /// Number of emergency room visits in past year
    #[property(name = "emergency_visits_count")]
    pub emergency_visits_count: Option<i32>,

    /// Number of outpatient visits in past year
    #[property(name = "outpatient_visits_count")]
    pub outpatient_visits_count: Option<i32>,

    /// Number of GP contacts in past year
    #[property(name = "gp_visits_count")]
    pub gp_visits_count: Option<i32>,

    /// Date of most recent hospital admission
    #[serde(alias = "D_INDDTO")]
    #[property(name = "last_hospital_admission_date")]
    pub last_hospital_admission_date: Option<NaiveDate>,

    /// Total hospitalization days in past year
    #[serde(alias = "LIGGETID")]
    #[property(name = "hospitalization_days")]
    pub hospitalization_days: Option<i32>,

    /// Length of stay in days (for current/last admission)
    #[property(name = "length_of_stay")]
    pub length_of_stay: Option<i32>,

    /// All diagnoses associated with this individual
    #[property(name = "diagnoses")]
    pub diagnoses: Option<Vec<String>>,

    /// All procedures performed on this individual
    #[property(name = "procedures")]
    pub procedures: Option<Vec<String>>,

    /// All hospital admission dates
    #[property(name = "hospital_admissions")]
    pub hospital_admissions: Option<Vec<NaiveDate>>,

    /// All hospital discharge dates
    #[property(name = "discharge_dates")]
    pub discharge_dates: Option<Vec<NaiveDate>>,

    /// Death cause code
    #[property(name = "death_cause")]
    pub death_cause: Option<String>,

    /// Underlying death cause
    #[property(name = "underlying_death_cause")]
    pub underlying_death_cause: Option<String>,

    // MFR registry specific fields
    /// Birth weight in grams
    #[property(name = "birth_weight")]
    pub birth_weight: Option<i32>,

    /// Birth length in cm
    #[property(name = "birth_length")]
    pub birth_length: Option<i32>,

    /// Gestational age in weeks
    #[property(name = "gestational_age")]
    pub gestational_age: Option<i32>,

    /// APGAR score at 5 minutes
    #[property(name = "apgar_score")]
    pub apgar_score: Option<i32>,

    /// Birth order for multiple births
    #[property(name = "birth_order")]
    pub birth_order: Option<i32>,

    /// Plurality (number of fetuses in this pregnancy)
    #[property(name = "plurality")]
    pub plurality: Option<i32>,
}

// Implement Default for Individual
impl Default for Individual {
    fn default() -> Self {
        Self::default_impl()
    }
}

// Implement Clone for Individual
impl Clone for Individual {
    fn clone(&self) -> Self {
        // Create a new default instance
        let mut cloned = Self::default();

        // Copy all fields except properties
        cloned.pnr = self.pnr.clone();
        cloned.mother_pnr = self.mother_pnr.clone();
        cloned.father_pnr = self.father_pnr.clone();
        cloned.family_id = self.family_id.clone();
        cloned.spouse_pnr = self.spouse_pnr.clone();

        cloned.gender = self.gender.clone();
        cloned.birth_date = self.birth_date;
        cloned.death_date = self.death_date;
        cloned.age = self.age;

        cloned.origin = self.origin.clone();
        cloned.citizenship_status = self.citizenship_status.clone();
        cloned.immigration_type = self.immigration_type.clone();
        cloned.marital_status = self.marital_status.clone();
        cloned.marital_date = self.marital_date;

        cloned.municipality_code = self.municipality_code.clone();
        cloned.regional_code = self.regional_code.clone();
        cloned.is_rural = self.is_rural;
        cloned.household_type = self.household_type;
        cloned.family_size = self.family_size;
        cloned.household_size = self.household_size;

        cloned.residence_from = self.residence_from;
        cloned.position_in_family = self.position_in_family;
        cloned.family_type = self.family_type;

        cloned.event_type = self.event_type.clone();
        cloned.event_date = self.event_date;

        cloned.education_code = self.education_code;
        cloned.education_valid_from = self.education_valid_from;
        cloned.education_valid_to = self.education_valid_to;
        cloned.education_institution = self.education_institution;
        cloned.education_source = self.education_source;
        cloned.education_level = self.education_level;

        cloned.socioeconomic_status = self.socioeconomic_status;

        cloned.annual_income = self.annual_income;
        cloned.employment_income = self.employment_income;
        cloned.income_year = self.income_year;

        cloned.hospital_admissions_count = self.hospital_admissions_count;
        cloned.emergency_visits_count = self.emergency_visits_count;
        cloned.outpatient_visits_count = self.outpatient_visits_count;
        cloned.gp_visits_count = self.gp_visits_count;
        cloned.last_hospital_admission_date = self.last_hospital_admission_date;
        cloned.hospitalization_days = self.hospitalization_days;
        cloned.length_of_stay = self.length_of_stay;

        cloned.diagnoses = self.diagnoses.clone();
        cloned.procedures = self.procedures.clone();
        cloned.hospital_admissions = self.hospital_admissions.clone();
        cloned.discharge_dates = self.discharge_dates.clone();
        cloned.death_cause = self.death_cause.clone();
        cloned.underlying_death_cause = self.underlying_death_cause.clone();

        cloned.birth_weight = self.birth_weight;
        cloned.birth_length = self.birth_length;
        cloned.gestational_age = self.gestational_age;
        cloned.apgar_score = self.apgar_score;
        cloned.birth_order = self.birth_order;
        cloned.plurality = self.plurality;

        // Clone time periods
        for (registry, periods) in &self.time_periods {
            cloned
                .time_periods
                .insert(registry.clone(), periods.clone());
        }
        cloned.current_time_period = self.current_time_period.clone();

        // Don't copy properties - they can't be cloned
        // This is acceptable behavior since properties are just for temporary storage

        cloned
    }
}

// Time period related methods for Individual
impl Individual {
    /// Set the current time period for a registry
    pub fn set_current_time_period(&mut self, registry: String, period: TimePeriod) {
        self.current_time_period = Some((registry, period));
    }

    /// Add a registry time period entry
    pub fn add_time_period(&mut self, registry: String, period: TimePeriod, data_source: String) {
        self.time_periods
            .entry(registry)
            .or_insert_with(BTreeMap::new)
            .insert(period, data_source);
    }

    /// Get all time periods for a registry
    pub fn get_time_periods(&self, registry: &str) -> Option<&BTreeMap<TimePeriod, String>> {
        self.time_periods.get(registry)
    }

    /// Get the earliest time period data for a registry
    pub fn get_earliest_time_period(&self, registry: &str) -> Option<TimePeriod> {
        self.time_periods
            .get(registry)
            .and_then(|periods| periods.keys().next().copied())
    }

    /// Get the latest time period data for a registry
    pub fn get_latest_time_period(&self, registry: &str) -> Option<TimePeriod> {
        self.time_periods
            .get(registry)
            .and_then(|periods| periods.keys().rev().next().copied())
    }

    /// Get all data sources for a specific time period across all registries
    pub fn get_data_sources_for_date(&self, date: NaiveDate) -> HashMap<String, Vec<TimePeriod>> {
        let mut result = HashMap::new();

        for (registry, periods) in &self.time_periods {
            let matching_periods: Vec<TimePeriod> = periods
                .keys()
                .filter(|period| period.contains(date))
                .copied()
                .collect();

            if !matching_periods.is_empty() {
                result.insert(registry.clone(), matching_periods);
            }
        }

        result
    }

    /// Filter data to a specific time range
    pub fn filter_to_time_range(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> HashMap<String, Vec<TimePeriod>> {
        let mut result = HashMap::new();

        for (registry, periods) in &self.time_periods {
            let matching_periods: Vec<TimePeriod> = periods
                .keys()
                .filter(|period| {
                    let period_start = period.start_date();
                    let period_end = period.end_date();
                    // Check for any overlap between the period and the requested range
                    period_start <= end_date && period_end >= start_date
                })
                .copied()
                .collect();

            if !matching_periods.is_empty() {
                result.insert(registry.clone(), matching_periods);
            }
        }

        result
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

            // Properties map for dynamic fields
            properties: None,

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

            // Temporal data
            time_periods: HashMap::new(),
            current_time_period: None,
        }
    }

    /// Set a property value by name
    ///
    /// This method is used by the registry deserializer to set property values
    /// dynamically, primarily for the procedural macro system.
    pub fn set_property(&mut self, property: &str, value: Box<dyn std::any::Any + Send + Sync>) {
        use crate::models::core::individual::implementations::property_reflection::PropertyReflection;

        // Use the PropertyReflection trait to handle both dedicated fields and the properties map
        self.set_reflected_property(property, value);
    }

    /// Get access to the properties map
    #[must_use]
    pub fn properties(&self) -> Option<&HashMap<String, Box<dyn std::any::Any + Send + Sync>>> {
        self.properties.as_ref()
    }

    /// Store a property in the properties map
    /// This is kept for compatibility with existing code
    pub fn store_property(&mut self, property: &str, value: Box<dyn std::any::Any + Send + Sync>) {
        use crate::models::core::individual::implementations::property_reflection::PropertyReflection;
        PropertyReflection::store_property(self, property, value);
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
    
    /// Convert from a `RecordBatch` with time period information extracted from the file path
    ///
    /// This version of from_batch extracts time period information from the file path
    /// and sets it on the resulting Individual objects.
    ///
    /// # Arguments
    ///
    /// * `batch` - The record batch to convert
    /// * `file_path` - The path to the file containing the data
    /// * `registry_name` - The name of the registry the data is from
    ///
    /// # Returns
    ///
    /// A Result containing a vector of Individual objects with time period information
    pub fn from_batch_with_time_period(
        batch: &RecordBatch,
        file_path: &std::path::Path,
        registry_name: &str,
    ) -> Result<Vec<Self>> {
        // First convert to individuals
        let mut individuals = Self::from_batch(batch)?;
        
        // Extract time period from filename
        if let Some(time_period) = 
            crate::models::core::individual::temporal::extract_time_period_from_filename(file_path) {
            
            // Set time period information on each individual
            for individual in &mut individuals {
                // Set the current time period for this registry
                individual.set_current_time_period(registry_name.to_string(), time_period);
                
                // Generate a source identifier for this data point
                let source = format!("{}_{}_{}", registry_name, time_period.to_string(), batch.num_rows());
                
                // Add the time period to the individual's time periods map
                individual.add_time_period(registry_name.to_string(), time_period, source);
            }
        }
        
        Ok(individuals)
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
    /// * `registry_name` - The name of the registry
    /// * `time_period` - Optional time period information for this data
    /// * `file_path` - Optional path to the file containing the data
    ///
    /// # Returns
    ///
    /// `true` if any data was added to the Individual, `false` otherwise
    pub fn enhance_from_registry(
        &mut self,
        batch: &RecordBatch,
        row: usize,
        registry_name: &str,
        time_period: Option<TimePeriod>,
        file_path: Option<&std::path::Path>,
    ) -> Result<bool> {
        // First check if the PNR matches
        if !self.pnr_matches_record(batch, row)? {
            return Ok(false);
        }

        // Try to get time period information
        let period = match time_period {
            // If time period is provided directly, use it
            Some(p) => Some(p),
            // Otherwise, try to extract it from the file path if provided
            None => file_path.and_then(|path| 
                crate::models::core::individual::temporal::extract_time_period_from_filename(path)),
        };

        // Store time period information if available
        if let Some(period) = period {
            // Set as current time period for this enhancement operation
            self.set_current_time_period(registry_name.to_string(), period);

            // Generate a source identifier for this data point
            let source = format!("{}_row_{}", registry_name, row);

            // Store this time period in the individual's history
            self.add_time_period(registry_name.to_string(), period, source);
        }

        // Use the direct method for enhancement since this is a direct deserialization registry
        // Create a temporary individual from this registry row
        use crate::registry::trait_deserializer::deserialize_row;
        if let Some(enhanced_individual) = deserialize_row(registry_name, batch, row)? {
            // Merge fields from the enhanced individual into self, but only if they're not already set
            self.merge_fields(&enhanced_individual);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Check if this Individual's PNR matches the PNR in a registry record
    pub fn pnr_matches_record(&self, batch: &RecordBatch, row: usize) -> Result<bool> {
        use crate::utils::arrow::array_utils::{downcast_array, get_column};

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
    pub fn merge_fields(&mut self, source: &Self) {
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
