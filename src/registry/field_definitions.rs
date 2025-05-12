//! Centralized registry field definitions
//!
//! This module provides a single source of truth for registry field definitions.
//! It contains standardized field definitions that can be used across different
//! registry schemas.

use crate::schema::{FieldDefinition, FieldType};
use crate::models::core::types::{
    CitizenshipStatus, Gender, MaritalStatus, Origin, SocioeconomicStatus, HousingType
};
use crate::schema::field_def::{FieldMapping, ModelSetters, Extractors};

/// Common field definitions used across registries
#[derive(Debug)]
pub struct CommonFields;

impl CommonFields {
    // Basic identification fields
    
    /// Personal identification number (PNR)
    #[must_use]
    pub fn pnr() -> FieldDefinition {
        FieldDefinition::new(
            "PNR",
            "Personal identification number",
            FieldType::PNR,
            false,
        )
    }
    
    /// Gender field
    #[must_use]
    pub fn gender() -> FieldDefinition {
        FieldDefinition::new(
            "KOEN",
            "Gender",
            FieldType::Category,
            true,
        )
    }
    
    /// Birth date field
    #[must_use]
    pub fn birth_date() -> FieldDefinition {
        FieldDefinition::new(
            "FOED_DAG",
            "Birth date",
            FieldType::Date,
            true,
        )
    }
    
    /// Death date field
    #[must_use]
    pub fn death_date() -> FieldDefinition {
        FieldDefinition::new(
            "DODDATO",
            "Death date",
            FieldType::Date,
            true,
        )
    }
    
    // Family and relationship fields
    
    /// Mother's PNR field
    #[must_use]
    pub fn mother_pnr() -> FieldDefinition {
        FieldDefinition::new(
            "MOR_ID",
            "Mother's personal identification number",
            FieldType::PNR,
            true,
        )
    }
    
    /// Father's PNR field
    #[must_use]
    pub fn father_pnr() -> FieldDefinition {
        FieldDefinition::new(
            "FAR_ID",
            "Father's personal identification number",
            FieldType::PNR,
            true,
        )
    }
    
    /// Family ID field
    #[must_use]
    pub fn family_id() -> FieldDefinition {
        FieldDefinition::new(
            "FAMILIE_ID",
            "Family identifier",
            FieldType::String,
            true,
        )
    }
    
    // Demographic and status fields
    
    /// Origin field
    #[must_use]
    pub fn origin() -> FieldDefinition {
        FieldDefinition::new(
            "OPR_LAND",
            "Geographic origin category",
            FieldType::Category,
            true,
        )
    }
    
    /// Municipality code field
    #[must_use]
    pub fn municipality_code() -> FieldDefinition {
        FieldDefinition::new(
            "KOM",
            "Municipality code at index date",
            FieldType::Category,
            true,
        )
    }
    
    /// Marital status field
    #[must_use]
    pub fn marital_status() -> FieldDefinition {
        FieldDefinition::new(
            "CIVST",
            "Marital status",
            FieldType::Category,
            true,
        )
    }
    
    /// Citizenship status field
    #[must_use]
    pub fn citizenship_status() -> FieldDefinition {
        FieldDefinition::new(
            "STATSB",
            "Citizenship status",
            FieldType::Category,
            true,
        )
    }
    
    /// Housing type field
    #[must_use]
    pub fn housing_type() -> FieldDefinition {
        FieldDefinition::new(
            "HUSTYPE",
            "Housing type",
            FieldType::Category,
            true,
        )
    }
    
    /// Household size field
    #[must_use]
    pub fn household_size() -> FieldDefinition {
        FieldDefinition::new(
            "ANTPERSF",
            "Number of persons in household",
            FieldType::Integer,
            true,
        )
    }
    
    // Employment and socioeconomic fields
    
    /// Socioeconomic status field
    #[must_use]
    pub fn socioeconomic_status() -> FieldDefinition {
        FieldDefinition::new(
            "SOCIO",
            "Socioeconomic status classification",
            FieldType::Category,
            true,
        )
    }
    
    /// Occupation code field
    #[must_use]
    pub fn occupation_code() -> FieldDefinition {
        FieldDefinition::new(
            "DISCO",
            "Primary occupation code (DISCO-08)",
            FieldType::String,
            true,
        )
    }
    
    /// Industry code field
    #[must_use]
    pub fn industry_code() -> FieldDefinition {
        FieldDefinition::new(
            "BRANCHE",
            "Industry code (DB07)",
            FieldType::String,
            true,
        )
    }
    
    /// Workplace ID field
    #[must_use]
    pub fn workplace_id() -> FieldDefinition {
        FieldDefinition::new(
            "ARB_STED_ID",
            "Primary workplace ID",
            FieldType::String,
            true,
        )
    }
    
    /// Working hours field
    #[must_use]
    pub fn working_hours() -> FieldDefinition {
        FieldDefinition::new(
            "HELTID",
            "Weekly working hours",
            FieldType::Decimal,
            true,
        )
    }
    
    // Income fields
    
    /// Annual income field
    #[must_use]
    pub fn annual_income() -> FieldDefinition {
        FieldDefinition::new(
            "PERINDKIALT",
            "Annual income (DKK)",
            FieldType::Decimal,
            true,
        )
    }
    
    /// Disposable income field
    #[must_use]
    pub fn disposable_income() -> FieldDefinition {
        FieldDefinition::new(
            "DISPON_NY",
            "Disposable income after tax (DKK)",
            FieldType::Decimal,
            true,
        )
    }
    
    /// Employment income field
    #[must_use]
    pub fn employment_income() -> FieldDefinition {
        FieldDefinition::new(
            "LOENMV",
            "Income from employment (DKK)",
            FieldType::Decimal,
            true,
        )
    }
    
    /// Year field
    #[must_use]
    pub fn year() -> FieldDefinition {
        FieldDefinition::new(
            "AAR",
            "Year",
            FieldType::Integer,
            true,
        )
    }
}

/// Common field mappings to the Individual model
#[derive(Debug)]
pub struct CommonMappings;

impl CommonMappings {
    /// Create a PNR field mapping
    #[must_use]
    pub fn pnr() -> FieldMapping {
        FieldMapping::new(
            CommonFields::pnr(),
            Extractors::string("PNR"),
            ModelSetters::string_setter(|individual, value| {
                individual.pnr = value;
            }),
        )
    }
    
    /// Create a gender field mapping
    #[must_use]
    pub fn gender() -> FieldMapping {
        FieldMapping::new(
            CommonFields::gender(),
            Extractors::string("KOEN"),
            ModelSetters::string_setter(|individual, value| {
                individual.gender = Gender::from(value.as_str());
            }),
        )
    }
    
    /// Create a birth date field mapping
    #[must_use]
    pub fn birth_date() -> FieldMapping {
        FieldMapping::new(
            CommonFields::birth_date(),
            Extractors::date("FOED_DAG"),
            ModelSetters::date_setter(|individual, value| {
                individual.birth_date = Some(value);
            }),
        )
    }
    
    /// Create a death date field mapping
    #[must_use]
    pub fn death_date() -> FieldMapping {
        FieldMapping::new(
            CommonFields::death_date(),
            Extractors::date("DODDATO"),
            ModelSetters::date_setter(|individual, value| {
                individual.death_date = Some(value);
            }),
        )
    }
    
    /// Create a mother PNR field mapping
    #[must_use]
    pub fn mother_pnr() -> FieldMapping {
        FieldMapping::new(
            CommonFields::mother_pnr(),
            Extractors::string("MOR_ID"),
            ModelSetters::string_setter(|individual, value| {
                individual.mother_pnr = Some(value);
            }),
        )
    }
    
    /// Create a father PNR field mapping
    #[must_use]
    pub fn father_pnr() -> FieldMapping {
        FieldMapping::new(
            CommonFields::father_pnr(),
            Extractors::string("FAR_ID"),
            ModelSetters::string_setter(|individual, value| {
                individual.father_pnr = Some(value);
            }),
        )
    }
    
    /// Create a family ID field mapping
    #[must_use]
    pub fn family_id() -> FieldMapping {
        FieldMapping::new(
            CommonFields::family_id(),
            Extractors::string("FAMILIE_ID"),
            ModelSetters::string_setter(|individual, value| {
                individual.family_id = Some(value);
            }),
        )
    }
    
    /// Create an origin field mapping
    #[must_use]
    pub fn origin() -> FieldMapping {
        FieldMapping::new(
            CommonFields::origin(),
            Extractors::string("OPR_LAND"),
            ModelSetters::string_setter(|individual, value| {
                let origin = if value == "5100" {
                    Origin::Danish
                } else if value.starts_with('5') {
                    // Other Nordic countries
                    Origin::Western
                } else if value.len() >= 2 && "0123456789".contains(&value[0..1]) {
                    // Country codes starting with digits 0-9 are typically Western countries
                    Origin::Western
                } else {
                    Origin::NonWestern
                };
                individual.origin = origin;
            }),
        )
    }
    
    /// Create a municipality code field mapping
    #[must_use]
    pub fn municipality_code() -> FieldMapping {
        FieldMapping::new(
            CommonFields::municipality_code(),
            Extractors::string("KOM"),
            ModelSetters::string_setter(|individual, value| {
                individual.municipality_code = Some(value);
            }),
        )
    }
    
    /// Create a socioeconomic status field mapping
    #[must_use]
    pub fn socioeconomic_status() -> FieldMapping {
        FieldMapping::new(
            CommonFields::socioeconomic_status(),
            Extractors::integer("SOCIO"),
            ModelSetters::i32_setter(|individual, value| {
                let status = match value {
                    110..=129 => SocioeconomicStatus::SelfEmployedWithEmployees,
                    210 | 220 => SocioeconomicStatus::TopLevelEmployee,
                    310..=359 => SocioeconomicStatus::MediumLevelEmployee,
                    360..=389 => SocioeconomicStatus::BasicLevelEmployee,
                    410..=439 => SocioeconomicStatus::OtherEmployee,
                    500 => SocioeconomicStatus::Student,
                    600 => SocioeconomicStatus::Pensioner,
                    700 => SocioeconomicStatus::Unemployed,
                    800 => SocioeconomicStatus::OtherInactive,
                    _ => SocioeconomicStatus::Unknown,
                };
                individual.socioeconomic_status = status;
            }),
        )
    }
    
    /// Create a marital status field mapping
    #[must_use]
    pub fn marital_status() -> FieldMapping {
        FieldMapping::new(
            CommonFields::marital_status(),
            Extractors::string("CIVST"),
            ModelSetters::string_setter(|individual, value| {
                individual.marital_status = MaritalStatus::from(value.as_str());
            }),
        )
    }
    
    /// Create a citizenship status field mapping
    #[must_use]
    pub fn citizenship_status() -> FieldMapping {
        FieldMapping::new(
            CommonFields::citizenship_status(),
            Extractors::integer("STATSB"),
            ModelSetters::i32_setter(|individual, value| {
                let status = if value == 5100 {
                    CitizenshipStatus::Danish
                } else if (5001..=5999).contains(&value) {
                    CitizenshipStatus::EuropeanUnion
                } else {
                    CitizenshipStatus::NonEUWithResidence
                };
                individual.citizenship_status = status;
            }),
        )
    }
    
    /// Create a housing type field mapping
    #[must_use]
    pub fn housing_type() -> FieldMapping {
        FieldMapping::new(
            CommonFields::housing_type(),
            Extractors::integer("HUSTYPE"),
            ModelSetters::i32_setter(|individual, value| {
                individual.housing_type = HousingType::from(value);
            }),
        )
    }
}