//! Serde-enhanced Individual model
//!
//! This module provides an enhanced version of the Individual model
//! with direct serde attributes for registry data conversion.

use crate::Individual;
use crate::error::Result;
use crate::models::core::types::{
    CitizenshipStatus, EducationField, EducationLevel, Gender, HousingType, MaritalStatus, Origin,
    SocioeconomicStatus,
};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use serde::{Deserialize, Deserializer, Serialize};

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
                Err(_) => Err(E::custom(format!("Invalid citizenship code: {}", value))),
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

/// Custom deserializer for municipality_code from KOM field
/// This converts from integer to string
fn deserialize_municipality_code<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let code = i32::deserialize(deserializer).map_err(serde::de::Error::custom)?;
    Ok(Some(code.to_string()))
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

/// Compute `is_rural` from `municipality_code`
#[allow(dead_code)]
fn compute_is_rural(municipality_code: &Option<String>) -> bool {
    if let Some(code) = municipality_code {
        let code_num = code.parse::<i32>().unwrap_or(0);
        // Rural areas often have municipality codes in specific ranges
        return !(400..=600).contains(&code_num);
    }
    false
}

/// Wrapper struct with serde attributes for direct registry deserialization
///
/// This wrapper adds serde attributes to the base Individual model
/// to enable direct conversion from registry data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerdeIndividual {
    /// The inner Individual that holds the actual data
    #[serde(flatten, with = "IndividualDef")]
    inner: Individual,
}

/// Field attribute mappings for `SerdeIndividual`
///
/// This struct defines the serde mapping attributes to allow direct
/// deserialization from registry field names to Individual fields
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(remote = "Individual")]
struct IndividualDef {
    /// Personal identification number (PNR)
    #[serde(alias = "PNR")]
    pnr: String,

    /// Gender of the individual
    #[serde(alias = "KOEN", deserialize_with = "deserialize_gender", default = "Gender::default")]
    gender: Gender,

    /// Birth date
    #[serde(alias = "FOED_DAG")]
    birth_date: Option<NaiveDate>,

    /// Death date, if applicable
    #[serde(alias = "DODDATO")]
    death_date: Option<NaiveDate>,

    /// Geographic origin category
    #[serde(alias = "OPR_LAND", deserialize_with = "deserialize_origin", default = "Origin::default")]
    origin: Origin,

    /// Education level
    #[serde(default = "EducationLevel::default")]
    education_level: EducationLevel,

    /// Municipality code at index date
    #[serde(alias = "KOM", deserialize_with = "deserialize_municipality_code")]
    municipality_code: Option<String>,

    /// Whether the individual lives in a rural area
    #[serde(skip_deserializing, default)]
    is_rural: bool,

    /// Mother's PNR, if known
    #[serde(alias = "MOR_ID")]
    mother_pnr: Option<String>,

    /// Father's PNR, if known
    #[serde(alias = "FAR_ID")]
    father_pnr: Option<String>,

    /// Family identifier
    #[serde(alias = "FAMILIE_ID")]
    family_id: Option<String>,

    /// Emigration date, if applicable
    #[serde(alias = "UDRDTO")]
    emigration_date: Option<NaiveDate>,

    /// Immigration date, if applicable
    #[serde(alias = "INDRDTO")]
    immigration_date: Option<NaiveDate>,

    // Employment and socioeconomic status
    /// Socioeconomic status classification
    #[serde(alias = "SOCIO", deserialize_with = "deserialize_socioeconomic_status", default = "SocioeconomicStatus::default")]
    socioeconomic_status: SocioeconomicStatus,

    /// Primary occupation code (DISCO-08)
    #[serde(alias = "DISCO")]
    occupation_code: Option<String>,

    /// Industry code (DB07)
    #[serde(alias = "BRANCHE")]
    industry_code: Option<String>,

    /// Primary workplace ID
    #[serde(alias = "ARB_STED_ID")]
    workplace_id: Option<String>,

    /// Employment start date
    employment_start_date: Option<NaiveDate>,

    /// Weekly working hours
    #[serde(alias = "HELTID")]
    working_hours: Option<f64>,

    // Education details
    /// Primary field of education
    #[serde(default = "EducationField::default")]
    education_field: EducationField,

    /// Most recent education completion date
    #[serde(alias = "AFSLUTNINGSDATO")]
    education_completion_date: Option<NaiveDate>,

    /// Institution code for highest education
    #[serde(alias = "UDD_INST")]
    education_institution: Option<String>,

    /// Educational program code (AUDD)
    #[serde(alias = "AUDD")]
    education_program_code: Option<String>,

    // Income information
    /// Annual income (DKK)
    #[serde(alias = "PERINDKIALT")]
    annual_income: Option<f64>,

    /// Disposable income after tax (DKK)
    #[serde(alias = "DISPON_NY")]
    disposable_income: Option<f64>,

    /// Income from employment (DKK)
    #[serde(alias = "LOENMV")]
    employment_income: Option<f64>,

    /// Income from self-employment (DKK)
    #[serde(alias = "NETOVSKUD")]
    self_employment_income: Option<f64>,

    /// Capital income (DKK)
    #[serde(alias = "KPITALIND")]
    capital_income: Option<f64>,

    /// Transfer income (social benefits, pensions, etc.) (DKK)
    #[serde(alias = "OFFHJ")]
    transfer_income: Option<f64>,

    /// Income year
    #[serde(alias = "AAR")]
    income_year: Option<i32>,

    // Healthcare usage
    /// Number of hospital admissions in past year
    hospital_admissions_count: Option<i32>,

    /// Number of emergency room visits in past year
    emergency_visits_count: Option<i32>,

    /// Number of outpatient visits in past year
    outpatient_visits_count: Option<i32>,

    /// Number of GP contacts in past year
    gp_visits_count: Option<i32>,

    /// Date of most recent hospital admission
    #[serde(alias = "D_INDDTO")]
    last_hospital_admission_date: Option<NaiveDate>,

    /// Total hospitalization days in past year
    #[serde(alias = "LIGGETID")]
    hospitalization_days: Option<i32>,

    // Additional demographic information
    /// Marital status
    #[serde(alias = "CIVST", deserialize_with = "deserialize_marital_status", default = "MaritalStatus::default")]
    marital_status: MaritalStatus,

    /// Citizenship status
    #[serde(alias = "STATSB", deserialize_with = "deserialize_citizenship_status", default = "CitizenshipStatus::default")]
    citizenship_status: CitizenshipStatus,

    /// Housing type
    #[serde(alias = "HUSTYPE", deserialize_with = "deserialize_housing_type", default = "HousingType::default")]
    housing_type: HousingType,

    /// Number of persons in household
    #[serde(alias = "ANTPERSF")]
    household_size: Option<i32>,

    /// Household type code
    household_type: Option<String>,
}

impl SerdeIndividual {
    /// Create a new `SerdeIndividual` with minimal required information
    #[must_use]
    pub fn new(pnr: String, gender: Gender, birth_date: Option<NaiveDate>) -> Self {
        let mut individual = Individual::new(pnr, gender, birth_date);
        individual.compute_rural_status();

        Self { inner: individual }
    }

    /// Get reference to the underlying Individual
    #[must_use]
    pub const fn inner(&self) -> &Individual {
        &self.inner
    }

    /// Get mutable reference to the underlying Individual
    pub const fn inner_mut(&mut self) -> &mut Individual {
        &mut self.inner
    }

    /// Convert into the inner Individual
    #[must_use]
    pub fn into_inner(self) -> Individual {
        self.inner
    }

    /// Convert directly from a `RecordBatch` using `serde_arrow`
    pub fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        match serde_arrow::from_record_batch::<Vec<Self>>(batch) {
            Ok(mut individuals) => {
                // Compute any derived fields if needed
                for individual in &mut individuals {
                    individual.post_deserialize();
                }
                Ok(individuals)
            }
            Err(e) => Err(anyhow::anyhow!("Failed to deserialize: {}", e)),
        }
    }

    /// Convert from the standard Individual model
    #[must_use]
    pub fn from_standard(standard: &Individual) -> Self {
        Self {
            inner: standard.clone(),
        }
    }
}

// Implementation for the serde deserialization of Individual through the SerdeIndividual
// We now use the derive(Deserialize) instead of manually implementing it
impl SerdeIndividual {
    // This custom deserialize hook runs after deserialization
    // to compute derived fields
    pub fn post_deserialize(&mut self) {
        self.inner.compute_rural_status();
    }
}
