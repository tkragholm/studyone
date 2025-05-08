//! Common domain type definitions
//!
//! This module contains common enum types and data structures used across
//! domain models to ensure consistency and facilitate code reuse.

/// Gender of an individual
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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