//! Common domain type definitions
//!
//! This module contains common enum types and data structures used across
//! domain models to ensure consistency and facilitate code reuse.

use serde::{Deserialize, Serialize};

/// Gender of an individual
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Gender {
    /// Male gender
    Male,
    /// Female gender
    Female,
    /// Unknown or not specified
    Unknown,
}

impl From<&str> for Gender {
    fn from(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "m" | "male" | "1" => Self::Male,
            "f" | "female" | "2" => Self::Female,
            _ => Self::Unknown,
        }
    }
}

impl From<i32> for Gender {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::Male,
            2 => Self::Female,
            _ => Self::Unknown,
        }
    }
}

/// Geographic origin category
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Origin {
    /// Danish origin
    Danish,
    /// Western immigrant or descendant
    Western,
    /// Non-western immigrant or descendant
    NonWestern,
    /// Unknown origin
    Unknown,
}

impl From<&str> for Origin {
    fn from(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "danish" | "danmark" | "dk" | "1" => Self::Danish,
            "western" | "west" | "2" => Self::Western,
            "non-western" | "nonwestern" | "3" => Self::NonWestern,
            _ => Self::Unknown,
        }
    }
}

impl From<i32> for Origin {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::Danish,
            2 => Self::Western,
            3 => Self::NonWestern,
            _ => Self::Unknown,
        }
    }
}

/// Education level using ISCED classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EducationLevel {
    /// ISCED 0-2: Low education
    Low,
    /// ISCED 3-4: Medium education
    Medium,
    /// ISCED 5-8: High education
    High,
    /// Unknown education level
    Unknown,
}

impl From<&str> for EducationLevel {
    fn from(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "low" | "1" => Self::Low,
            "medium" | "2" => Self::Medium,
            "high" | "3" => Self::High,
            _ => Self::Unknown,
        }
    }
}

impl From<i32> for EducationLevel {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::Low,
            2 => Self::Medium,
            3 => Self::High,
            _ => Self::Unknown,
        }
    }
}

/// Type of diagnosis (primary or secondary)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagnosisType {
    /// Primary (main) diagnosis
    Primary,
    /// Secondary diagnosis
    Secondary,
    /// Other or unknown type
    Other,
}

impl From<&str> for DiagnosisType {
    fn from(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "primary" | "main" | "a" => Self::Primary,
            "secondary" | "b" => Self::Secondary,
            _ => Self::Other,
        }
    }
}

impl From<i32> for DiagnosisType {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::Primary,
            2 => Self::Secondary,
            _ => Self::Other,
        }
    }
}

/// Severe Chronic Disease category
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScdCategory {
    /// Blood disorders
    BloodDisorder,
    /// Immune system disorders
    ImmuneDisorder,
    /// Endocrine disorders
    EndocrineDisorder,
    /// Neurological disorders
    NeurologicalDisorder,
    /// Cardiovascular disorders
    CardiovascularDisorder,
    /// Respiratory disorders
    RespiratoryDisorder,
    /// Gastrointestinal disorders
    GastrointestinalDisorder,
    /// Musculoskeletal disorders
    MusculoskeletalDisorder,
    /// Renal disorders
    RenalDisorder,
    /// Congenital disorders
    CongenitalDisorder,
    /// No SCD category
    None,
}

impl From<i32> for ScdCategory {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::BloodDisorder,
            2 => Self::ImmuneDisorder,
            3 => Self::EndocrineDisorder,
            4 => Self::NeurologicalDisorder,
            5 => Self::CardiovascularDisorder,
            6 => Self::RespiratoryDisorder,
            7 => Self::GastrointestinalDisorder,
            8 => Self::MusculoskeletalDisorder,
            9 => Self::RenalDisorder,
            10 => Self::CongenitalDisorder,
            _ => Self::None,
        }
    }
}

/// Disease severity classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiseaseSeverity {
    /// Mild conditions (e.g., asthma)
    Mild,
    /// Moderate conditions (most SCD algorithm conditions)
    Moderate,
    /// Severe conditions (e.g., cancer, organ transplantation)
    Severe,
    /// No disease or unknown severity
    None,
}

impl From<i32> for DiseaseSeverity {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::Mild,
            2 => Self::Moderate,
            3 => Self::Severe,
            _ => Self::None,
        }
    }
}

/// Origin of the disease
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiseaseOrigin {
    /// Congenital disease (present at birth)
    Congenital,
    /// Acquired disease (developed after birth)
    Acquired,
    /// No disease or unknown origin
    None,
}

impl From<i32> for DiseaseOrigin {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::Congenital,
            2 => Self::Acquired,
            _ => Self::None,
        }
    }
}

/// Job situation category
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobSituation {
    /// Employed full-time
    EmployedFullTime,
    /// Employed part-time
    EmployedPartTime,
    /// Self-employed
    SelfEmployed,
    /// Unemployed
    Unemployed,
    /// Student
    Student,
    /// Retired
    Retired,
    /// On leave (e.g., parental leave, sick leave)
    OnLeave,
    /// Other or unknown job situation
    Other,
}

impl From<i32> for JobSituation {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::EmployedFullTime,
            2 => Self::EmployedPartTime,
            3 => Self::SelfEmployed,
            4 => Self::Unemployed,
            5 => Self::Student,
            6 => Self::Retired,
            7 => Self::OnLeave,
            _ => Self::Other,
        }
    }
}

/// Type of family based on composition
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FamilyType {
    /// Two-parent family with both parents present
    TwoParent,
    /// Single-parent family with only a mother
    SingleMother,
    /// Single-parent family with only a father
    SingleFather,
    /// No parents present (e.g., children living with other relatives)
    NoParent,
    /// Unknown family type
    Unknown,
}

impl From<i32> for FamilyType {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::TwoParent,
            2 => Self::SingleMother,
            3 => Self::SingleFather,
            4 => Self::NoParent,
            _ => Self::Unknown,
        }
    }
}

/// Marital status according to Danish registries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaritalStatus {
    /// Unmarried
    Unmarried,
    /// Married
    Married,
    /// Divorced
    Divorced,
    /// Widowed
    Widowed,
    /// Registered partnership (similar to marriage)
    RegisteredPartnership,
    /// Dissolved partnership
    DissolvedPartnership,
    /// Longest living partner (widow/widower from registered partnership)
    LongestLivingPartner,
    /// Unknown or not specified
    Unknown,
}

impl From<&str> for MaritalStatus {
    fn from(s: &str) -> Self {
        match s.trim().to_uppercase().as_str() {
            "U" => Self::Unmarried,
            "G" => Self::Married,
            "F" => Self::Divorced,
            "E" => Self::Widowed,
            "P" => Self::RegisteredPartnership,
            "O" => Self::DissolvedPartnership,
            "L" => Self::LongestLivingPartner,
            _ => Self::Unknown,
        }
    }
}

impl From<i32> for MaritalStatus {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::Unmarried,
            2 => Self::Married,
            3 => Self::Divorced,
            4 => Self::Widowed,
            5 => Self::RegisteredPartnership,
            6 => Self::DissolvedPartnership,
            7 => Self::LongestLivingPartner,
            _ => Self::Unknown,
        }
    }
}

/// Citizenship status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CitizenshipStatus {
    /// Danish citizen
    Danish,
    /// EU/EEA citizen
    EuropeanUnion,
    /// Non-EU foreigner with residence permit
    NonEUWithResidence,
    /// Foreign with temporary permit
    TemporaryPermit,
    /// Stateless
    Stateless,
    /// Unknown or not specified
    Unknown,
}

impl From<i32> for CitizenshipStatus {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::Danish,
            2 => Self::EuropeanUnion,
            3 => Self::NonEUWithResidence,
            4 => Self::TemporaryPermit,
            5 => Self::Stateless,
            _ => Self::Unknown,
        }
    }
}

/// Housing type categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HousingType {
    /// Single-family house
    SingleFamilyHouse,
    /// Apartment
    Apartment,
    /// Terraced house or townhouse
    TerracedHouse,
    /// Dormitory or student housing
    Dormitory,
    /// Institution (care home, etc.)
    Institution,
    /// Other or unspecified
    Other,
    /// Unknown
    Unknown,
}

impl From<i32> for HousingType {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::SingleFamilyHouse,
            2 => Self::Apartment,
            3 => Self::TerracedHouse,
            4 => Self::Dormitory,
            5 => Self::Institution,
            6 => Self::Other,
            _ => Self::Unknown,
        }
    }
}

/// Socioeconomic status classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SocioeconomicStatus {
    /// Self-employed with employees
    SelfEmployedWithEmployees,
    /// Self-employed without employees
    SelfEmployedWithoutEmployees,
    /// Top-level employee (management)
    TopLevelEmployee,
    /// Medium-level employee (professionals)
    MediumLevelEmployee,
    /// Basic-level employee (clerical, service, etc.)
    BasicLevelEmployee,
    /// Other employees
    OtherEmployee,
    /// Unemployed
    Unemployed,
    /// Student
    Student,
    /// Pensioner
    Pensioner,
    /// Other not economically active
    OtherInactive,
    /// Unknown
    Unknown,
}

impl From<i32> for SocioeconomicStatus {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::SelfEmployedWithEmployees,
            2 => Self::SelfEmployedWithoutEmployees,
            3 => Self::TopLevelEmployee,
            4 => Self::MediumLevelEmployee,
            5 => Self::BasicLevelEmployee,
            6 => Self::OtherEmployee,
            7 => Self::Unemployed,
            8 => Self::Student,
            9 => Self::Pensioner,
            10 => Self::OtherInactive,
            _ => Self::Unknown,
        }
    }
}

/// Primary field of education
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EducationField {
    /// General education (non-specialized)
    General,
    /// Education (teaching)
    Education,
    /// Humanities and arts
    HumanitiesArts,
    /// Social sciences, business, law
    SocialScienceBusinessLaw,
    /// Science, mathematics, computing
    ScienceMathematicsComputing,
    /// Engineering, manufacturing, construction
    EngineeringManufacturingConstruction,
    /// Agriculture and veterinary
    AgricultureVeterinary,
    /// Health and welfare
    HealthWelfare,
    /// Services
    Services,
    /// Unknown or not specified
    Unknown,
}

impl From<i32> for EducationField {
    fn from(value: i32) -> Self {
        match value {
            0 => Self::General,
            1 => Self::Education,
            2 => Self::HumanitiesArts,
            3 => Self::SocialScienceBusinessLaw,
            4 => Self::ScienceMathematicsComputing,
            5 => Self::EngineeringManufacturingConstruction,
            6 => Self::AgricultureVeterinary,
            7 => Self::HealthWelfare,
            8 => Self::Services,
            _ => Self::Unknown,
        }
    }
}