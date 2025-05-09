//! Core Individual entity definition
//!
//! This module contains the base Individual struct definition and core methods.

use crate::models::core::traits::EntityModel;
use crate::models::core::types::{
    CitizenshipStatus, EducationField, EducationLevel, Gender, HousingType, MaritalStatus, Origin,
    SocioeconomicStatus,
};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Compute rural status from municipality code
    pub fn compute_rural_status(&mut self) {
        if let Some(code) = &self.municipality_code {
            let code_num = code.parse::<i32>().unwrap_or(0);
            // Rural areas often have municipality codes in specific ranges
            self.is_rural = !(400..=600).contains(&code_num);
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
