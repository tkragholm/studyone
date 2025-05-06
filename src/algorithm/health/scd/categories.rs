//! Disease categories for SCD algorithm
//!
//! This module defines the disease categories used in the Severe Chronic Disease (SCD) algorithm.

use std::fmt;

/// SCD disease categories
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScdCategory {
    /// Blood disorders (e.g., anemias, coagulation defects)
    BloodDisorder = 1,
    /// Immune system disorders (e.g., immunodeficiencies)
    ImmuneDisorder = 2,
    /// Endocrine disorders (e.g., metabolic disorders)
    EndocrineDisorder = 3,
    /// Neurological disorders (e.g., epilepsy, cerebral palsy)
    NeurologicalDisorder = 4,
    /// Cardiovascular disorders (e.g., congenital heart defects)
    CardiovascularDisorder = 5,
    /// Respiratory disorders (e.g., cystic fibrosis, asthma)
    RespiratoryDisorder = 6,
    /// Gastrointestinal disorders (e.g., Crohn's disease)
    GastrointestinalDisorder = 7,
    /// Musculoskeletal disorders (e.g., muscular dystrophy)
    MusculoskeletalDisorder = 8,
    /// Renal disorders (e.g., chronic kidney disease)
    RenalDisorder = 9,
    /// Other congenital disorders not covered by other categories
    CongenitalDisorder = 10,
    /// No SCD category assigned
    None = 0,
}

impl ScdCategory {
    /// Convert a numeric category ID to ScdCategory
    #[must_use]
    pub const fn from_u8(id: u8) -> Self {
        match id {
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
    
    /// Get the display name for this category
    #[must_use]
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::BloodDisorder => "Blood Disorder",
            Self::ImmuneDisorder => "Immune System Disorder",
            Self::EndocrineDisorder => "Endocrine Disorder",
            Self::NeurologicalDisorder => "Neurological Disorder",
            Self::CardiovascularDisorder => "Cardiovascular Disorder",
            Self::RespiratoryDisorder => "Respiratory Disorder",
            Self::GastrointestinalDisorder => "Gastrointestinal Disorder",
            Self::MusculoskeletalDisorder => "Musculoskeletal Disorder",
            Self::RenalDisorder => "Renal Disorder",
            Self::CongenitalDisorder => "Other Congenital Disorder",
            Self::None => "No SCD Category",
        }
    }
    
    /// Get all valid SCD categories
    #[must_use]
    pub fn all_categories() -> Vec<Self> {
        vec![
            Self::BloodDisorder,
            Self::ImmuneDisorder,
            Self::EndocrineDisorder,
            Self::NeurologicalDisorder,
            Self::CardiovascularDisorder,
            Self::RespiratoryDisorder,
            Self::GastrointestinalDisorder,
            Self::MusculoskeletalDisorder,
            Self::RenalDisorder,
            Self::CongenitalDisorder,
        ]
    }
    
    /// Check if this is a valid SCD category (not None)
    #[must_use]
    pub fn is_valid(self) -> bool {
        self != Self::None
    }
}

impl fmt::Display for ScdCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name())
    }
}