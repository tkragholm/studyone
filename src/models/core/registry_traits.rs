//! Registry-specific field interface traits
//!
//! This module defines traits for accessing and manipulating fields that are specific
//! to different registry data sources. These traits provide a type-safe interface for
//! working with registry data in the unified schema system.
//!
//! Each registry trait defines getter and setter methods for fields that are present in
//! that registry's data. This provides a unified interface for field mappings and ensures
//! consistent access patterns across the codebase.

use chrono::NaiveDate;

/// Trait for fields from the BEF (Befolkning) registry
pub trait BefFields {
    /// Get spouse's personal identification number
    fn spouse_pnr(&self) -> Option<&str>;
    
    /// Set spouse's personal identification number
    fn set_spouse_pnr(&mut self, value: Option<String>);
    
    /// Get family size (number of persons in family)
    fn family_size(&self) -> Option<i32>;
    
    /// Set family size 
    fn set_family_size(&mut self, value: Option<i32>);
    
    /// Get date of residence from
    fn residence_from(&self) -> Option<NaiveDate>;
    
    /// Set date of residence from
    fn set_residence_from(&mut self, value: Option<NaiveDate>);
    
    /// Get migration type (in/out)
    fn migration_type(&self) -> Option<&str>;
    
    /// Set migration type
    fn set_migration_type(&mut self, value: Option<String>);
    
    /// Get position in family
    fn position_in_family(&self) -> Option<i32>;
    
    /// Set position in family
    fn set_position_in_family(&mut self, value: Option<i32>);
    
    /// Get family type
    fn family_type(&self) -> Option<i32>;
    
    /// Set family type
    fn set_family_type(&mut self, value: Option<i32>);
}

/// Trait for fields from the LPR (Landspatientregistret) registry
pub trait LprFields {
    /// Get all diagnoses
    fn diagnoses(&self) -> Option<&[String]>;
    
    /// Set diagnoses
    fn set_diagnoses(&mut self, value: Option<Vec<String>>);
    
    /// Add a single diagnosis
    fn add_diagnosis(&mut self, diagnosis: String);
    
    /// Get all procedures
    fn procedures(&self) -> Option<&[String]>;
    
    /// Set procedures
    fn set_procedures(&mut self, value: Option<Vec<String>>);
    
    /// Add a single procedure
    fn add_procedure(&mut self, procedure: String);
    
    /// Get hospital admission dates
    fn hospital_admissions(&self) -> Option<&[NaiveDate]>;
    
    /// Set hospital admission dates
    fn set_hospital_admissions(&mut self, value: Option<Vec<NaiveDate>>);
    
    /// Add a single hospital admission date
    fn add_hospital_admission(&mut self, date: NaiveDate);
    
    /// Get discharge dates
    fn discharge_dates(&self) -> Option<&[NaiveDate]>;
    
    /// Set discharge dates
    fn set_discharge_dates(&mut self, value: Option<Vec<NaiveDate>>);
    
    /// Add a single discharge date
    fn add_discharge_date(&mut self, date: NaiveDate);
    
    /// Get length of stay in days
    fn length_of_stay(&self) -> Option<i32>;
    
    /// Set length of stay in days
    fn set_length_of_stay(&mut self, value: Option<i32>);
}

/// Trait for fields from the MFR (Medical Birth Registry) registry
pub trait MfrFields {
    /// Get birth weight in grams
    fn birth_weight(&self) -> Option<i32>;
    
    /// Set birth weight in grams
    fn set_birth_weight(&mut self, value: Option<i32>);
    
    /// Get birth length in cm
    fn birth_length(&self) -> Option<i32>;
    
    /// Set birth length in cm
    fn set_birth_length(&mut self, value: Option<i32>);
    
    /// Get gestational age in weeks
    fn gestational_age(&self) -> Option<i32>;
    
    /// Set gestational age in weeks
    fn set_gestational_age(&mut self, value: Option<i32>);
    
    /// Get APGAR score at 5 minutes
    fn apgar_score(&self) -> Option<i32>;
    
    /// Set APGAR score at 5 minutes
    fn set_apgar_score(&mut self, value: Option<i32>);
    
    /// Get birth order for multiple births
    fn birth_order(&self) -> Option<i32>;
    
    /// Set birth order for multiple births
    fn set_birth_order(&mut self, value: Option<i32>);
    
    /// Get plurality (number of fetuses in this pregnancy)
    fn plurality(&self) -> Option<i32>;
    
    /// Set plurality
    fn set_plurality(&mut self, value: Option<i32>);
}

/// Trait for fields from the UDDF (Education) registry
pub trait UddfFields {
    /// Get education institution code
    fn education_institution(&self) -> Option<&str>;
    
    /// Set education institution code
    fn set_education_institution(&mut self, value: Option<String>);
    
    /// Get education start date
    fn education_start_date(&self) -> Option<NaiveDate>;
    
    /// Set education start date
    fn set_education_start_date(&mut self, value: Option<NaiveDate>);
    
    /// Get education completion date
    fn education_completion_date(&self) -> Option<NaiveDate>;
    
    /// Set education completion date
    fn set_education_completion_date(&mut self, value: Option<NaiveDate>);
    
    /// Get education program code
    fn education_program_code(&self) -> Option<&str>;
    
    /// Set education program code
    fn set_education_program_code(&mut self, value: Option<String>);
}

/// Trait for fields from the IND (Income) registry
pub trait IndFields {
    /// Get annual income
    fn annual_income(&self) -> Option<f64>;
    
    /// Set annual income
    fn set_annual_income(&mut self, value: Option<f64>);
    
    /// Get disposable income
    fn disposable_income(&self) -> Option<f64>;
    
    /// Set disposable income
    fn set_disposable_income(&mut self, value: Option<f64>);
    
    /// Get employment income
    fn employment_income(&self) -> Option<f64>;
    
    /// Set employment income
    fn set_employment_income(&mut self, value: Option<f64>);
    
    /// Get self-employment income
    fn self_employment_income(&self) -> Option<f64>;
    
    /// Set self-employment income
    fn set_self_employment_income(&mut self, value: Option<f64>);
    
    /// Get capital income
    fn capital_income(&self) -> Option<f64>;
    
    /// Set capital income
    fn set_capital_income(&mut self, value: Option<f64>);
    
    /// Get transfer income (social benefits, pensions)
    fn transfer_income(&self) -> Option<f64>;
    
    /// Set transfer income
    fn set_transfer_income(&mut self, value: Option<f64>);
    
    /// Get income year
    fn income_year(&self) -> Option<i32>;
    
    /// Set income year
    fn set_income_year(&mut self, value: Option<i32>);
}

/// Trait for fields from the AKM (Labour Market) registry
pub trait AkmFields {
    /// Get occupation code (DISCO-08)
    fn occupation_code(&self) -> Option<&str>;
    
    /// Set occupation code
    fn set_occupation_code(&mut self, value: Option<String>);
    
    /// Get industry code (DB07)
    fn industry_code(&self) -> Option<&str>;
    
    /// Set industry code
    fn set_industry_code(&mut self, value: Option<String>);
    
    /// Get employment start date
    fn employment_start_date(&self) -> Option<NaiveDate>;
    
    /// Set employment start date
    fn set_employment_start_date(&mut self, value: Option<NaiveDate>);
    
    /// Get employment end date
    fn employment_end_date(&self) -> Option<NaiveDate>;
    
    /// Set employment end date
    fn set_employment_end_date(&mut self, value: Option<NaiveDate>);
    
    /// Get workplace ID
    fn workplace_id(&self) -> Option<&str>;
    
    /// Set workplace ID
    fn set_workplace_id(&mut self, value: Option<String>);
    
    /// Get weekly working hours
    fn working_hours(&self) -> Option<f64>;
    
    /// Set weekly working hours
    fn set_working_hours(&mut self, value: Option<f64>);
}

/// Trait for fields from the VNDS (Migration) registry
pub trait VndsFields {
    /// Get emigration date
    fn emigration_date(&self) -> Option<NaiveDate>;
    
    /// Set emigration date
    fn set_emigration_date(&mut self, value: Option<NaiveDate>);
    
    /// Get immigration date
    fn immigration_date(&self) -> Option<NaiveDate>;
    
    /// Set immigration date
    fn set_immigration_date(&mut self, value: Option<NaiveDate>);
    
    /// Get emigration country code
    fn emigration_country(&self) -> Option<&str>;
    
    /// Set emigration country code
    fn set_emigration_country(&mut self, value: Option<String>);
    
    /// Get immigration country code
    fn immigration_country(&self) -> Option<&str>;
    
    /// Set immigration country code
    fn set_immigration_country(&mut self, value: Option<String>);
}

/// Trait for fields from the DOD (Death) registry
pub trait DodFields {
    /// Get death date
    fn death_date(&self) -> Option<NaiveDate>;
    
    /// Set death date
    fn set_death_date(&mut self, value: Option<NaiveDate>);
    
    /// Get death cause code
    fn death_cause(&self) -> Option<&str>;
    
    /// Set death cause code
    fn set_death_cause(&mut self, value: Option<String>);
    
    /// Get underlying death cause
    fn underlying_death_cause(&self) -> Option<&str>;
    
    /// Set underlying death cause
    fn set_underlying_death_cause(&mut self, value: Option<String>);
}