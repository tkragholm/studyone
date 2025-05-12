//! Registry field definitions
//!
//! This module provides a centralized source of truth for registry field definitions,
//! including field names, types, descriptions, and other metadata.

use crate::schema::{FieldDefinition, FieldType};

/// Field definitions for the BEF (Befolkning) registry
pub struct BefFields;

impl BefFields {
    /// Personal identification number (PNR)
    pub fn pnr() -> FieldDefinition {
        FieldDefinition::new("PNR", "Personal identification number", FieldType::PNR, false)
    }
    
    /// Gender code
    pub fn gender() -> FieldDefinition {
        FieldDefinition::new("KOEN", "Gender code (1=male, 2=female)", FieldType::Integer, false)
    }
    
    /// Birth date
    pub fn birth_date() -> FieldDefinition {
        FieldDefinition::new("FOED_DAG", "Birth date", FieldType::Date, false)
    }
    
    /// Municipality code
    pub fn municipality() -> FieldDefinition {
        FieldDefinition::new("KOM", "Municipality code", FieldType::Integer, true)
    }
    
    /// Marital status
    pub fn marital_status() -> FieldDefinition {
        FieldDefinition::new("CIVST", "Marital status code", FieldType::String, true)
    }
    
    /// Country of origin code
    pub fn origin() -> FieldDefinition {
        FieldDefinition::new("OPR_LAND", "Country of origin code", FieldType::String, true)
    }
    
    /// Citizenship code
    pub fn citizenship() -> FieldDefinition {
        FieldDefinition::new("STATSB", "Citizenship code", FieldType::String, true)
    }
    
    /// Housing type
    pub fn housing_type() -> FieldDefinition {
        FieldDefinition::new("HUSTYPE", "Housing type code", FieldType::Integer, true)
    }
    
    /// Household size
    pub fn household_size() -> FieldDefinition {
        FieldDefinition::new("ANTPERSH", "Number of persons in household", FieldType::Integer, true)
    }
    
    /// Family identifier
    pub fn family_id() -> FieldDefinition {
        FieldDefinition::new("FAMILIE_ID", "Family identifier", FieldType::String, true)
    }
    
    /// Mother's personal identification number
    pub fn mother_pnr() -> FieldDefinition {
        FieldDefinition::new("MOR_ID", "Mother's personal identification number", FieldType::PNR, true)
    }
    
    /// Father's personal identification number
    pub fn father_pnr() -> FieldDefinition {
        FieldDefinition::new("FAR_ID", "Father's personal identification number", FieldType::PNR, true)
    }
    
    /// Spouse's personal identification number
    pub fn spouse_pnr() -> FieldDefinition {
        FieldDefinition::new("AEGTE_ID", "Spouse's personal identification number", FieldType::PNR, true)
    }
    
    /// Family size
    pub fn family_size() -> FieldDefinition {
        FieldDefinition::new("ANTPERSF", "Number of persons in family", FieldType::Integer, true)
    }
    
    /// Date of residence from
    pub fn residence_from() -> FieldDefinition {
        FieldDefinition::new("BOP_VFRA", "Date of residence from", FieldType::Date, true)
    }
    
    /// Position in family
    pub fn position_in_family() -> FieldDefinition {
        FieldDefinition::new("PLADS", "Position in family", FieldType::Integer, true)
    }
    
    /// Family type
    pub fn family_type() -> FieldDefinition {
        FieldDefinition::new("FAMILIE_TYPE", "Family type", FieldType::Integer, true)
    }
    
    /// Immigration/emigration type
    pub fn migration_type() -> FieldDefinition {
        FieldDefinition::new("IE_TYPE", "Immigration/emigration type", FieldType::String, true)
    }
}

/// Field definitions for the LPR (Landspatientregistret) registry
pub struct LprFields;

impl LprFields {
    /// Personal identification number (PNR)
    pub fn pnr() -> FieldDefinition {
        FieldDefinition::new("PNR", "Personal identification number", FieldType::PNR, false)
    }
    
    /// Diagnosis code (LPR v2)
    pub fn diagnosis_v2() -> FieldDefinition {
        FieldDefinition::new("DIAG", "Diagnosis code (ICD-10)", FieldType::String, true)
    }
    
    /// Diagnosis code (LPR v3)
    pub fn diagnosis_v3() -> FieldDefinition {
        FieldDefinition::new("C_DIAG", "Diagnosis code (ICD-10)", FieldType::String, true)
    }
    
    /// Diagnosis type (LPR v2)
    pub fn diagnosis_type_v2() -> FieldDefinition {
        FieldDefinition::new("DIAGTYPE", "Diagnosis type (A=primary, B=secondary)", FieldType::String, true)
    }
    
    /// Diagnosis type (LPR v3)
    pub fn diagnosis_type_v3() -> FieldDefinition {
        FieldDefinition::new("C_DIAGTYPE", "Diagnosis type (1=primary, 2=secondary)", FieldType::Integer, true)
    }
    
    /// Procedure code (LPR v2)
    pub fn procedure_v2() -> FieldDefinition {
        FieldDefinition::new("OPR", "Procedure code", FieldType::String, true)
    }
    
    /// Procedure code (LPR v3)
    pub fn procedure_v3() -> FieldDefinition {
        FieldDefinition::new("C_OPR", "Procedure code", FieldType::String, true)
    }
    
    /// Admission date (LPR v2)
    pub fn admission_date_v2() -> FieldDefinition {
        FieldDefinition::new("INDDTO", "Admission date", FieldType::Date, true)
    }
    
    /// Admission date (LPR v3)
    pub fn admission_date_v3() -> FieldDefinition {
        FieldDefinition::new("D_INDDTO", "Admission date", FieldType::Date, true)
    }
    
    /// Discharge date (LPR v2)
    pub fn discharge_date_v2() -> FieldDefinition {
        FieldDefinition::new("UDDTO", "Discharge date", FieldType::Date, true)
    }
    
    /// Discharge date (LPR v3)
    pub fn discharge_date_v3() -> FieldDefinition {
        FieldDefinition::new("D_UDDTO", "Discharge date", FieldType::Date, true)
    }
    
    /// Length of stay in days
    pub fn length_of_stay() -> FieldDefinition {
        FieldDefinition::new("LIGGETID", "Length of stay in days", FieldType::Integer, true)
    }
}

/// Field definitions for the MFR (Medical Birth Registry) registry
pub struct MfrFields;

impl MfrFields {
    /// Child's personal identification number
    pub fn child_pnr() -> FieldDefinition {
        FieldDefinition::new("CPR_BARN", "Child's personal identification number", FieldType::PNR, false)
    }
    
    /// Mother's personal identification number
    pub fn mother_pnr() -> FieldDefinition {
        FieldDefinition::new("CPR_MODER", "Mother's personal identification number", FieldType::PNR, true)
    }
    
    /// Father's personal identification number
    pub fn father_pnr() -> FieldDefinition {
        FieldDefinition::new("CPR_FADER", "Father's personal identification number", FieldType::PNR, true)
    }
    
    /// Birth date
    pub fn birth_date() -> FieldDefinition {
        FieldDefinition::new("FOEDSELSDATO", "Birth date", FieldType::Date, false)
    }
    
    /// Birth weight in grams
    pub fn birth_weight() -> FieldDefinition {
        FieldDefinition::new("VAEGT", "Birth weight in grams", FieldType::Integer, true)
    }
    
    /// Birth length in cm
    pub fn birth_length() -> FieldDefinition {
        FieldDefinition::new("LAENGDE", "Birth length in cm", FieldType::Integer, true)
    }
    
    /// Gestational age in weeks
    pub fn gestational_age() -> FieldDefinition {
        FieldDefinition::new("SVLNGD", "Gestational age in weeks", FieldType::Integer, true)
    }
    
    /// APGAR score at 5 minutes
    pub fn apgar_score() -> FieldDefinition {
        FieldDefinition::new("APGAR5", "APGAR score at 5 minutes", FieldType::Integer, true)
    }
    
    /// Birth order for multiple births
    pub fn birth_order() -> FieldDefinition {
        FieldDefinition::new("FLERFOLD", "Birth order for multiple births", FieldType::Integer, true)
    }
    
    /// Plurality (number of fetuses in this pregnancy)
    pub fn plurality() -> FieldDefinition {
        FieldDefinition::new("PLURALITY", "Number of fetuses in this pregnancy", FieldType::Integer, true)
    }
}

/// Field definitions for the UDDF (Education) registry
pub struct UddfFields;

impl UddfFields {
    /// Personal identification number (PNR)
    pub fn pnr() -> FieldDefinition {
        FieldDefinition::new("PNR", "Personal identification number", FieldType::PNR, false)
    }
    
    /// Education institution code
    pub fn institution() -> FieldDefinition {
        FieldDefinition::new("UDD_INST", "Education institution code", FieldType::String, true)
    }
    
    /// Education start date
    pub fn start_date() -> FieldDefinition {
        FieldDefinition::new("STARTDATO", "Education start date", FieldType::Date, true)
    }
    
    /// Education completion date
    pub fn completion_date() -> FieldDefinition {
        FieldDefinition::new("AFSLUTNINGSDATO", "Education completion date", FieldType::Date, true)
    }
    
    /// Education program code
    pub fn program_code() -> FieldDefinition {
        FieldDefinition::new("AUDD", "Education program code", FieldType::String, true)
    }
}

/// Field definitions for the IND (Income) registry
pub struct IndFields;

impl IndFields {
    /// Personal identification number (PNR)
    pub fn pnr() -> FieldDefinition {
        FieldDefinition::new("PNR", "Personal identification number", FieldType::PNR, false)
    }
    
    /// Annual income
    pub fn annual_income() -> FieldDefinition {
        FieldDefinition::new("PERINDKIALT", "Annual income (DKK)", FieldType::Float, true)
    }
    
    /// Disposable income
    pub fn disposable_income() -> FieldDefinition {
        FieldDefinition::new("DISPON_NY", "Disposable income after tax (DKK)", FieldType::Float, true)
    }
    
    /// Employment income
    pub fn employment_income() -> FieldDefinition {
        FieldDefinition::new("LOENMV", "Income from employment (DKK)", FieldType::Float, true)
    }
    
    /// Self-employment income
    pub fn self_employment_income() -> FieldDefinition {
        FieldDefinition::new("NETOVSKUD", "Income from self-employment (DKK)", FieldType::Float, true)
    }
    
    /// Capital income
    pub fn capital_income() -> FieldDefinition {
        FieldDefinition::new("KPITALIND", "Capital income (DKK)", FieldType::Float, true)
    }
    
    /// Transfer income
    pub fn transfer_income() -> FieldDefinition {
        FieldDefinition::new("OFFHJ", "Transfer income (social benefits, pensions, etc.) (DKK)", FieldType::Float, true)
    }
    
    /// Income year
    pub fn income_year() -> FieldDefinition {
        FieldDefinition::new("AAR", "Income year", FieldType::Integer, true)
    }
}

/// Common field definitions that appear in multiple registries
pub struct CommonFields;

impl CommonFields {
    /// Personal identification number (PNR)
    pub fn pnr() -> FieldDefinition {
        FieldDefinition::new("PNR", "Personal identification number", FieldType::PNR, false)
    }
    
    /// Birth date
    pub fn birth_date() -> FieldDefinition {
        FieldDefinition::new("FOED_DAG", "Birth date", FieldType::Date, false)
    }
    
    /// Gender
    pub fn gender() -> FieldDefinition {
        FieldDefinition::new("KOEN", "Gender code (1=male, 2=female)", FieldType::Integer, false)
    }
}