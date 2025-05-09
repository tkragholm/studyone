//! Registry data enhancement functionality
//!
//! This module provides methods for enhancing Individual models with registry data.

use crate::error::Result;
use crate::models::core::individual::Individual;
use crate::models::core::types::{
    CitizenshipStatus, Gender, HousingType, MaritalStatus, Origin, SocioeconomicStatus,
};
use crate::registry::registry_aware_models::detect_registry_type;
use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

impl Individual {
    /// Enhance this Individual with data from a registry record
    ///
    /// This method detects the registry type and applies the appropriate enhancement.
    /// It returns true if any data was added to the Individual, false otherwise.
    pub fn enhance_from_registry(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // Detect registry type
        let registry_type = detect_registry_type(batch);
        log::debug!("Enhancing Individual from registry type: {}", registry_type);

        // Skip if PNR doesn't match
        if !self.pnr_matches_record(batch, row)? {
            return Ok(false);
        }

        // Apply appropriate enhancement based on registry type
        match registry_type {
            "BEF" => self.enhance_from_bef_registry(batch, row),
            "IND" => self.enhance_from_ind_registry(batch, row),
            "LPR" => self.enhance_from_lpr_registry(batch, row),
            "MFR" => self.enhance_from_mfr_registry(batch, row),
            "DOD" => self.enhance_with_death_data(batch, row),
            "AKM" => self.enhance_with_employment_data(batch, row),
            "UDDF" => self.enhance_with_education_data(batch, row),
            "VNDS" => self.enhance_with_migration_data(batch, row),
            _ => {
                log::warn!(
                    "Unsupported registry type for enhancement: {}",
                    registry_type
                );
                Ok(false)
            }
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

    /// Enhance this Individual with data from a BEF registry record
    pub fn enhance_from_bef_registry(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        use crate::utils::field_extractors::{
            extract_date32, extract_int8_as_padded_string, extract_int32, extract_string,
        };

        let mut enhanced = false;

        // Extract gender if not already set
        if self.gender == Gender::Unknown {
            if let Ok(Some(gender_str)) = extract_string(batch, row, "KOEN", false) {
                self.gender = match gender_str.as_str() {
                    "M" => Gender::Male,
                    "F" => Gender::Female,
                    _ => Gender::Unknown,
                };
                enhanced = true;
            }
        }

        // Extract birth date if not already set
        if self.birth_date.is_none() {
            if let Ok(Some(birth_date)) = extract_date32(batch, row, "FOED_DAG", false) {
                self.birth_date = Some(birth_date);
                enhanced = true;
            }
        }

        // Extract family ID if not already set
        if self.family_id.is_none() {
            if let Ok(Some(family_id)) = extract_string(batch, row, "FAMILIE_ID", false) {
                self.family_id = Some(family_id);
                enhanced = true;
            }
        }

        // Extract parent PNRs if not already set
        if self.mother_pnr.is_none() {
            if let Ok(Some(mother_pnr)) = extract_string(batch, row, "MOR_ID", false) {
                self.mother_pnr = Some(mother_pnr);
                enhanced = true;
            }
        }

        if self.father_pnr.is_none() {
            if let Ok(Some(father_pnr)) = extract_string(batch, row, "FAR_ID", false) {
                self.father_pnr = Some(father_pnr);
                enhanced = true;
            }
        }

        // Extract origin information if not already set
        if self.origin == Origin::Unknown {
            if let Ok(Some(origin_code)) = extract_string(batch, row, "OPR_LAND", false) {
                self.origin = if origin_code == "5100" {
                    Origin::Danish
                } else if origin_code.starts_with('5') {
                    // Other Nordic countries
                    Origin::Western
                } else if origin_code.len() >= 2 && "0123456789".contains(&origin_code[0..1]) {
                    // Country codes starting with digits 0-9 are typically Western countries
                    Origin::Western
                } else {
                    Origin::NonWestern
                };
                enhanced = true;
            }
        }

        // Extract municipality code if not already set
        if self.municipality_code.is_none() {
            if let Ok(Some(municipality_code)) =
                extract_int8_as_padded_string(batch, row, "KOM", false, 3)
            {
                self.municipality_code = Some(municipality_code.clone());

                // Set is_rural field based on municipality code
                // This is a simplified approximation - in a real implementation,
                // this would use a proper lookup table of rural municipalities
                let code_num = municipality_code.parse::<i32>().unwrap_or(0);
                // Rural areas often have municipality codes in specific ranges
                self.is_rural = !(400..=600).contains(&code_num);

                enhanced = true;
            }
        }

        // Extract marital status if not already set
        if self.marital_status == MaritalStatus::Unknown {
            if let Ok(Some(status_code)) = extract_string(batch, row, "CIVST", false) {
                self.marital_status = MaritalStatus::from(status_code.as_str());
                enhanced = true;
            }
        }

        // Extract citizenship status if not already set
        if self.citizenship_status == CitizenshipStatus::Unknown {
            if let Ok(Some(code)) = extract_int32(batch, row, "STATSB", false) {
                self.citizenship_status = if code == 5100 {
                    CitizenshipStatus::Danish
                } else if (5001..=5999).contains(&code) {
                    CitizenshipStatus::EuropeanUnion
                } else {
                    CitizenshipStatus::NonEUWithResidence
                };
                enhanced = true;
            }
        }

        // Extract housing type if not already set
        if self.housing_type == HousingType::Unknown {
            if let Ok(Some(code)) = extract_int32(batch, row, "HUSTYPE", false) {
                self.housing_type = match code {
                    1 => HousingType::SingleFamilyHouse,
                    2 => HousingType::Apartment,
                    3 => HousingType::TerracedHouse,
                    4 => HousingType::Dormitory,
                    5 => HousingType::Institution,
                    _ => HousingType::Other,
                };
                enhanced = true;
            }
        }

        // Extract household size if not already set
        if self.household_size.is_none() {
            if let Ok(Some(family_size)) = extract_int32(batch, row, "ANTPERSF", false) {
                self.household_size = Some(family_size);
                enhanced = true;
            } else if let Ok(Some(household_size)) = extract_int32(batch, row, "ANTPERSH", false) {
                self.household_size = Some(household_size);
                enhanced = true;
            }
        }

        Ok(enhanced)
    }

    /// Enhance this Individual with data from an IND registry record
    pub fn enhance_from_ind_registry(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        use crate::utils::field_extractors::{extract_float64, extract_int32};

        let mut enhanced = false;

        // Extract income data
        if self.annual_income.is_none() {
            if let Ok(Some(income)) = extract_float64(batch, row, "PERINDKIALT", false) {
                self.annual_income = Some(income);
                enhanced = true;
            }
        }

        if self.disposable_income.is_none() {
            if let Ok(Some(income)) = extract_float64(batch, row, "DISPON_NY", false) {
                self.disposable_income = Some(income);
                enhanced = true;
            }
        }

        if self.employment_income.is_none() {
            if let Ok(Some(income)) = extract_float64(batch, row, "LOENMV", false) {
                self.employment_income = Some(income);
                enhanced = true;
            }
        }

        if self.self_employment_income.is_none() {
            if let Ok(Some(income)) = extract_float64(batch, row, "NETOVSKUD", false) {
                self.self_employment_income = Some(income);
                enhanced = true;
            }
        }

        if self.capital_income.is_none() {
            if let Ok(Some(income)) = extract_float64(batch, row, "KPITALIND", false) {
                self.capital_income = Some(income);
                enhanced = true;
            }
        }

        if self.transfer_income.is_none() {
            if let Ok(Some(income)) = extract_float64(batch, row, "OFFHJ", false) {
                self.transfer_income = Some(income);
                enhanced = true;
            }
        }

        // Extract income year
        if self.income_year.is_none() {
            if let Ok(Some(year)) = extract_int32(batch, row, "AAR", false) {
                self.income_year = Some(year);
                enhanced = true;
            }
        }

        // Extract socioeconomic status
        if self.socioeconomic_status == SocioeconomicStatus::Unknown {
            if let Ok(Some(status_code)) = extract_int32(batch, row, "SOCIO", false) {
                self.socioeconomic_status = match status_code {
                    110 | 120 | 130 => SocioeconomicStatus::SelfEmployed,
                    210 | 220 => SocioeconomicStatus::TopManagement,
                    310 | 320 | 330 | 340 | 350 => SocioeconomicStatus::HigherEducation,
                    360 | 370 | 380 => SocioeconomicStatus::MiddleEducation,
                    410 | 420 | 430 => SocioeconomicStatus::BasicEducation,
                    500 => SocioeconomicStatus::Student,
                    600 => SocioeconomicStatus::Retired,
                    700 => SocioeconomicStatus::Unemployed,
                    800 => SocioeconomicStatus::OutsideWorkforce,
                    900 => SocioeconomicStatus::Unknown,
                    _ => SocioeconomicStatus::Other,
                };
                enhanced = true;
            }
        }

        Ok(enhanced)
    }

    /// Enhance this Individual with data from an LPR registry record
    pub fn enhance_from_lpr_registry(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // The LPR enhancement would be more complex, as LPR data is primarily about diagnoses
        // For this implementation, we'll focus on tracking healthcare usage metrics
        use crate::utils::field_extractors::{extract_date32, extract_int32};

        let mut enhanced = false;

        // Extract admission date as the last hospital admission date
        if let Ok(Some(admission_date)) = extract_date32(batch, row, "D_INDDTO", false) {
            self.last_hospital_admission_date = Some(admission_date);
            enhanced = true;
        }

        // Extract hospitalization days if present
        if self.hospitalization_days.is_none() {
            if let Ok(Some(days)) = extract_int32(batch, row, "LIGGETID", false) {
                self.hospitalization_days = Some(days);
                enhanced = true;
            }
        }

        // Increment hospital admissions count
        if let Ok(Some(record_type)) = extract_int32(batch, row, "PATTYPE", false) {
            // PATTYPE of 0 indicates an inpatient admission
            if record_type == 0 {
                let current_count = self.hospital_admissions_count.unwrap_or(0);
                self.hospital_admissions_count = Some(current_count + 1);
                enhanced = true;
            } else if record_type == 2 {
                // PATTYPE of 2 typically indicates emergency
                let current_count = self.emergency_visits_count.unwrap_or(0);
                self.emergency_visits_count = Some(current_count + 1);
                enhanced = true;
            } else if record_type == 1 {
                // PATTYPE of 1 typically indicates outpatient
                let current_count = self.outpatient_visits_count.unwrap_or(0);
                self.outpatient_visits_count = Some(current_count + 1);
                enhanced = true;
            }
        }

        Ok(enhanced)
    }

    /// Enhance this Individual with data from an MFR registry record
    pub fn enhance_from_mfr_registry(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        use crate::utils::field_extractors::{extract_date32, extract_int32, extract_string};

        let mut enhanced = false;

        // MFR is primarily about birth records, so we're mainly interested in:
        // 1. Birth date for the child
        // 2. Parental relationships

        // Extract birth date if not already set
        if self.birth_date.is_none() {
            if let Ok(Some(birth_date)) = extract_date32(batch, row, "FOED_DAG", false) {
                self.birth_date = Some(birth_date);
                enhanced = true;
            }
        }

        // Extract mother's PNR if not already set
        if self.mother_pnr.is_none() {
            if let Ok(Some(mother_pnr)) = extract_string(batch, row, "MOR_CPR", false) {
                self.mother_pnr = Some(mother_pnr);
                enhanced = true;
            }
        }

        // Extract father's PNR if not already set
        if self.father_pnr.is_none() {
            if let Ok(Some(father_pnr)) = extract_string(batch, row, "FAR_CPR", false) {
                self.father_pnr = Some(father_pnr);
                enhanced = true;
            }
        }

        // Set gender if not already set - there's typically a KOEN field in MFR
        if self.gender == Gender::Unknown {
            if let Ok(Some(gender_code)) = extract_int32(batch, row, "KOEN", false) {
                self.gender = match gender_code {
                    1 => Gender::Male,
                    2 => Gender::Female,
                    _ => Gender::Unknown,
                };
                enhanced = true;
            }
        }

        Ok(enhanced)
    }

    /// Enhance with death data
    pub fn enhance_with_death_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        use crate::utils::field_extractors::extract_date32;

        let mut enhanced = false;

        // Extract death date if not already set
        if self.death_date.is_none() {
            if let Ok(Some(death_date)) = extract_date32(batch, row, "DODDATO", false) {
                self.death_date = Some(death_date);
                enhanced = true;
            }
        }

        Ok(enhanced)
    }

    /// Enhance with employment data
    pub fn enhance_with_employment_data(
        &mut self,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<bool> {
        use crate::utils::field_extractors::{extract_float64, extract_string};

        let mut enhanced = false;

        // Extract employment-related fields
        if self.occupation_code.is_none() {
            if let Ok(Some(occupation_code)) = extract_string(batch, row, "DISCO", false) {
                self.occupation_code = Some(occupation_code);
                enhanced = true;
            }
        }

        if self.industry_code.is_none() {
            if let Ok(Some(industry_code)) = extract_string(batch, row, "BRANCHE", false) {
                self.industry_code = Some(industry_code);
                enhanced = true;
            }
        }

        if self.workplace_id.is_none() {
            if let Ok(Some(workplace_id)) = extract_string(batch, row, "ARB_STED_ID", false) {
                self.workplace_id = Some(workplace_id);
                enhanced = true;
            }
        }

        if self.working_hours.is_none() {
            if let Ok(Some(hours)) = extract_float64(batch, row, "HELTID", false) {
                self.working_hours = Some(hours);
                enhanced = true;
            }
        }

        Ok(enhanced)
    }

    /// Enhance with education data
    pub fn enhance_with_education_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        use crate::models::core::types::EducationField;
        use crate::models::core::types::EducationLevel;
        use crate::utils::field_extractors::{extract_date32, extract_int32, extract_string};

        let mut enhanced = false;

        // Extract education level from UDD_H
        if self.education_level == EducationLevel::Unknown {
            if let Ok(Some(level_code)) = extract_int32(batch, row, "UDD_H", false) {
                self.education_level = match level_code {
                    10 => EducationLevel::Basic,
                    20 | 30 => EducationLevel::Secondary,
                    40 | 50 => EducationLevel::Higher,
                    60 => EducationLevel::Postgraduate,
                    _ => EducationLevel::Unknown,
                };
                enhanced = true;
            }
        }

        // Extract education field from AUDD
        if self.education_field == EducationField::Unknown {
            if let Ok(Some(field_code)) = extract_string(batch, row, "AUDD", false) {
                self.education_field = match &field_code[0..1] {
                    "0" => EducationField::General,
                    "1" => EducationField::Education,
                    "2" => EducationField::HumanitiesAndArts,
                    "3" => EducationField::SocialSciences,
                    "4" => EducationField::Science,
                    "5" => EducationField::Engineering,
                    "6" => EducationField::Agriculture,
                    "7" => EducationField::Health,
                    "8" => EducationField::Services,
                    _ => EducationField::Unknown,
                };

                self.education_program_code = Some(field_code);
                enhanced = true;
            }
        }

        // Extract education institution
        if self.education_institution.is_none() {
            if let Ok(Some(institution)) = extract_string(batch, row, "UDD_INST", false) {
                self.education_institution = Some(institution);
                enhanced = true;
            }
        }

        // Extract education completion date
        if self.education_completion_date.is_none() {
            if let Ok(Some(completion_date)) = extract_date32(batch, row, "AFSLUTNINGSDATO", false)
            {
                self.education_completion_date = Some(completion_date);
                enhanced = true;
            }
        }

        Ok(enhanced)
    }

    /// Enhance with migration data
    pub fn enhance_with_migration_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        use crate::utils::field_extractors::extract_date32;

        let mut enhanced = false;

        // Extract migration-related fields
        if self.emigration_date.is_none() {
            if let Ok(Some(emigration_date)) = extract_date32(batch, row, "UDRDTO", false) {
                self.emigration_date = Some(emigration_date);
                enhanced = true;
            }
        }

        if self.immigration_date.is_none() {
            if let Ok(Some(immigration_date)) = extract_date32(batch, row, "INDRDTO", false) {
                self.immigration_date = Some(immigration_date);
                enhanced = true;
            }
        }

        Ok(enhanced)
    }
}
