//! Core Individual entity definition
//!
//! This module contains the consolidated Individual struct definition with all functionality.

use crate::RecordBatch;
use crate::error::Result;
use crate::models::core::traits::EntityModel;
use crate::models::core::types::{
    CitizenshipStatus, EducationLevel, Gender, HousingType, MaritalStatus, Origin,
    SocioeconomicStatus,
};
use arrow::array::Array;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Deserializer, Serialize};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Individual {
    // Core identification
    /// Personal identification number (PNR)
    #[serde(alias = "PNR")]
    pub pnr: String,

    /// Gender of the individual
    #[serde(
        alias = "KOEN",
        deserialize_with = "deserialize_gender",
        default = "Gender::default"
    )]
    pub gender: Gender,

    /// Birth date
    #[serde(alias = "FOED_DAG")]
    pub birth_date: Option<NaiveDate>,

    /// Death date, if applicable
    #[serde(alias = "DODDATO")]
    pub death_date: Option<NaiveDate>,

    /// Geographic origin category
    #[serde(
        alias = "OPR_LAND",
        deserialize_with = "deserialize_origin",
        default = "Origin::default"
    )]
    pub origin: Origin,

    /// Age
    #[serde(skip)]
    pub age: Option<i32>,

    // Basic demographic information
    /// Municipality code at index date
    #[serde(
        alias = "KOM",
        deserialize_with = "deserialize_municipality_code",
        default
    )]
    pub municipality_code: Option<String>,

    /// Whether the individual lives in a rural area
    #[serde(skip_deserializing, default)]
    pub is_rural: bool,

    /// Mother's PNR, if known
    #[serde(alias = "MOR_ID")]
    pub mother_pnr: Option<String>,

    /// Father's PNR, if known
    #[serde(alias = "FAR_ID")]
    pub father_pnr: Option<String>,

    /// Family identifier
    #[serde(alias = "FAMILIE_ID")]
    pub family_id: Option<String>,

    /// Marital status
    #[serde(
        alias = "CIVST",
        deserialize_with = "deserialize_marital_status",
        default = "MaritalStatus::default"
    )]
    pub marital_status: MaritalStatus,

    /// Citizenship status
    #[serde(
        alias = "STATSB",
        deserialize_with = "deserialize_citizenship_status",
        default = "CitizenshipStatus::default"
    )]
    pub citizenship_status: CitizenshipStatus,

    /// Housing type
    #[serde(
        alias = "HUSTYPE",
        deserialize_with = "deserialize_housing_type",
        default = "HousingType::default"
    )]
    pub housing_type: HousingType,

    /// Number of persons in household
    #[serde(alias = "ANTPERSF")]
    pub household_size: Option<i32>,

    /// Household type code
    pub household_type: Option<String>,

    // BEF registry specific fields
    /// Spouse's personal identification number
    pub spouse_pnr: Option<String>,

    /// Family size (number of persons in family)
    pub family_size: Option<i32>,

    /// Date of residence from
    pub residence_from: Option<NaiveDate>,

    /// Migration type (in/out)
    pub migration_type: Option<String>,

    /// Position in family
    pub position_in_family: Option<i32>,

    /// Family type
    pub family_type: Option<i32>,

    // Migration information
    /// Emigration date, if applicable
    #[serde(alias = "UDRDTO")]
    pub emigration_date: Option<NaiveDate>,

    /// Immigration date, if applicable
    #[serde(alias = "INDRDTO")]
    pub immigration_date: Option<NaiveDate>,

    // Education information
    /// Education level
    #[serde(default = "EducationLevel::default")]
    pub education_level: EducationLevel,

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

    /// Education start date
    #[serde(alias = "HF_KILDE")]
    pub education_source: Option<u8>,

    // Employment and socioeconomic status
    /// Socioeconomic status classification
    #[serde(
        alias = "SOCIO",
        deserialize_with = "deserialize_socioeconomic_status",
        default = "SocioeconomicStatus::default"
    )]
    pub socioeconomic_status: SocioeconomicStatus,

    /// Primary workplace ID
    #[serde(alias = "ARB_STED_ID")]
    pub workplace_id: Option<String>,

    /// Primary occupation code (DISCO-08)
    #[serde(alias = "DISCO")]
    pub occupation_code: Option<String>,

    /// Industry code (DB07)
    #[serde(alias = "BRANCHE")]
    pub industry_code: Option<String>,

    /// Employment start date
    pub employment_start_date: Option<NaiveDate>,

    /// Weekly working hours
    #[serde(alias = "HELTID")]
    pub working_hours: Option<f64>,

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

// Core Individual methods
impl Individual {
    /// Create a new Individual with minimal required information
    #[must_use]
    pub fn new(pnr: String, gender: Gender, birth_date: Option<NaiveDate>) -> Self {
        Self {
            // Core identification
            pnr,
            gender,
            birth_date,
            death_date: None,
            origin: Origin::Unknown,
            age: None,

            // Basic demographic information
            municipality_code: None,
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: None,
            marital_status: MaritalStatus::Unknown,
            citizenship_status: CitizenshipStatus::Unknown,
            housing_type: HousingType::Unknown,
            household_size: None,
            household_type: None,

            // Education information
            education_level: EducationLevel::Unknown,
            education_institution: None,
            education_code: None,
            education_source: None,
            education_valid_to: None,
            education_valid_from: None,

            // Employment and socioeconomic status
            socioeconomic_status: SocioeconomicStatus::Unknown,
            workplace_id: None,
            occupation_code: None,
            industry_code: None,
            employment_start_date: None,
            working_hours: None,

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
            emigration_date: None,
            immigration_date: None,

            // BEF registry specific fields
            spouse_pnr: None,
            family_size: None,
            residence_from: None,
            migration_type: None,
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
    fn merge_fields(&mut self, source: &Self) {
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

        // Copy all other fields using the same pattern
        if self.origin == Origin::Unknown {
            self.origin = source.origin;
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
            self.household_type = source.household_type.clone();
        }

        if self.marital_status == MaritalStatus::Unknown {
            self.marital_status = source.marital_status;
        }

        if self.citizenship_status == CitizenshipStatus::Unknown {
            self.citizenship_status = source.citizenship_status;
        }

        if self.housing_type == HousingType::Unknown {
            self.housing_type = source.housing_type;
        }

        // Education fields
        if self.education_level == EducationLevel::Unknown {
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
        if self.socioeconomic_status == SocioeconomicStatus::Unknown {
            self.socioeconomic_status = source.socioeconomic_status;
        }

        if self.workplace_id.is_none() {
            self.workplace_id = source.workplace_id.clone();
        }

        if self.occupation_code.is_none() {
            self.occupation_code = source.occupation_code.clone();
        }

        if self.industry_code.is_none() {
            self.industry_code = source.industry_code.clone();
        }

        if self.employment_start_date.is_none() {
            self.employment_start_date = source.employment_start_date;
        }

        if self.working_hours.is_none() {
            self.working_hours = source.working_hours;
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
        if self.emigration_date.is_none() {
            self.emigration_date = source.emigration_date;
        }

        if self.immigration_date.is_none() {
            self.immigration_date = source.immigration_date;
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

        if self.migration_type.is_none() {
            self.migration_type = source.migration_type.clone();
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
    pub fn has_mother(&self) -> bool {
        self.mother_pnr.is_some()
    }

    /// Check if this Individual has a father in the dataset
    #[must_use]
    pub fn has_father(&self) -> bool {
        self.father_pnr.is_some()
    }

    /// Check if this Individual has both parents in the dataset
    #[must_use]
    pub fn has_both_parents(&self) -> bool {
        self.has_mother() && self.has_father()
    }

    /// Check if this Individual has either parent in the dataset
    #[must_use]
    pub fn has_any_parent(&self) -> bool {
        self.has_mother() || self.has_father()
    }
}

// Custom deserializers

/// Custom deserializer for gender from KOEN field
fn deserialize_gender<'de, D>(deserializer: D) -> std::result::Result<Gender, D::Error>
where
    D: Deserializer<'de>,
{
    let gender_code = String::deserialize(deserializer)?;
    Ok(Gender::from(gender_code.as_str()))
}

/// Custom deserializer for Origin from `OPR_LAND` field
fn deserialize_origin<'de, D>(deserializer: D) -> std::result::Result<Origin, D::Error>
where
    D: Deserializer<'de>,
{
    let origin_code = String::deserialize(deserializer)?;

    Ok(if origin_code == "5100" {
        Origin::Danish
    } else if origin_code.starts_with('5') {
        // Other Nordic countries
        Origin::Western
    } else if origin_code.len() >= 2 && "0123456789".contains(&origin_code[0..1]) {
        // Country codes starting with digits 0-9 are typically Western countries
        Origin::Western
    } else {
        Origin::NonWestern
    })
}

/// Custom deserializer for `MaritalStatus` from CIVST field
fn deserialize_marital_status<'de, D>(
    deserializer: D,
) -> std::result::Result<MaritalStatus, D::Error>
where
    D: Deserializer<'de>,
{
    let status_code = String::deserialize(deserializer)?;
    Ok(MaritalStatus::from(status_code.as_str()))
}

/// Custom deserializer for `CitizenshipStatus` from STATSB field
fn deserialize_citizenship_status<'de, D>(
    deserializer: D,
) -> std::result::Result<CitizenshipStatus, D::Error>
where
    D: Deserializer<'de>,
{
    // Use a more flexible approach to handle different types
    // Use serde's untagged enum visitor pattern
    struct FlexibleCitizenshipVisitor;

    impl serde::de::Visitor<'_> for FlexibleCitizenshipVisitor {
        type Value = CitizenshipStatus;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an integer or a string representing citizenship status")
        }

        // Handle integer values
        fn visit_i32<E>(self, value: i32) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.process_code(value)
        }

        fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.process_code(value as i32)
        }

        // Handle string values
        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            match value.parse::<i32>() {
                Ok(code) => self.process_code(code),
                Err(_) => Err(E::custom(format!("Invalid citizenship code: {value}"))),
            }
        }
    }

    impl FlexibleCitizenshipVisitor {
        fn process_code<E>(&self, code: i32) -> std::result::Result<CitizenshipStatus, E>
        where
            E: serde::de::Error,
        {
            Ok(if code == 5100 {
                CitizenshipStatus::Danish
            } else if (5001..=5999).contains(&code) {
                CitizenshipStatus::EuropeanUnion
            } else {
                CitizenshipStatus::NonEUWithResidence
            })
        }
    }

    deserializer.deserialize_any(FlexibleCitizenshipVisitor)
}

/// Custom deserializer for `HousingType` from HUSTYPE field
fn deserialize_housing_type<'de, D>(deserializer: D) -> std::result::Result<HousingType, D::Error>
where
    D: Deserializer<'de>,
{
    let code: i32 = Deserialize::deserialize(deserializer)?;
    Ok(HousingType::from(code))
}

/// Custom deserializer for `municipality_code` from KOM field
/// This converts from integer to string
fn deserialize_municipality_code<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    // Try to deserialize as i32
    let result = i32::deserialize(deserializer);

    match result {
        Ok(code) => Ok(Some(code.to_string())),
        Err(_) => {
            // If deserialization fails, return None (which is valid for Option<String>)
            Ok(None)
        }
    }
}

/// Custom deserializer for `SocioeconomicStatus` from SOCIO field
fn deserialize_socioeconomic_status<'de, D>(
    deserializer: D,
) -> std::result::Result<SocioeconomicStatus, D::Error>
where
    D: Deserializer<'de>,
{
    let status_code: i32 = Deserialize::deserialize(deserializer)?;

    // This uses a registry-specific mapping that doesn't match the standard i32 conversion,
    // so we keep the custom matching here
    Ok(match status_code {
        110 | 120 | 130 => SocioeconomicStatus::SelfEmployedWithEmployees,
        210 | 220 => SocioeconomicStatus::TopLevelEmployee,
        310 | 320 | 330 | 340 | 350 => SocioeconomicStatus::MediumLevelEmployee,
        360 | 370 | 380 => SocioeconomicStatus::BasicLevelEmployee,
        410 | 420 | 430 => SocioeconomicStatus::OtherEmployee,
        500 => SocioeconomicStatus::Student,
        600 => SocioeconomicStatus::Pensioner,
        700 => SocioeconomicStatus::Unemployed,
        800 => SocioeconomicStatus::OtherInactive,
        900 => SocioeconomicStatus::Unknown,
        _ => SocioeconomicStatus::Unknown,
    })
}
