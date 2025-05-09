//! Registry-aware model implementations
//!
//! This module provides implementations of the RegistryAware trait for domain models,
//! centralizing the registry-specific behavior for model creation.

use crate::RecordBatch;
use crate::common::traits::{
    AkmRegistry, BefRegistry, IndRegistry, LprRegistry, MfrRegistry, RegistryAware, UddfRegistry,
    VndsRegistry,
};
use crate::error::Result;
use crate::models::SocioeconomicStatus;
use crate::models::core::Individual;
use crate::models::core::types::Gender;
use crate::models::derived::Child;
use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use log::{debug, warn};
use std::collections::HashMap;
use std::sync::Arc;

/// Utility function to detect registry type from batch schema
pub fn detect_registry_type(batch: &RecordBatch) -> &'static str {
    if batch.schema().field_with_name("RECNUM").is_ok() {
        "LPR"
    } else if batch.schema().field_with_name("PERINDKIALT").is_ok() {
        "IND"
    } else if batch.schema().field_with_name("BARSELNR").is_ok() {
        "MFR"
    } else if batch.schema().field_with_name("VEJ_KODE").is_ok() {
        "VNDS"
    } else if batch.schema().field_with_name("DODDATO").is_ok() {
        "DOD"
    } else if batch.schema().field_with_name("HELTID").is_ok() {
        "AKM"
    } else if batch.schema().field_with_name("UDD_H").is_ok() {
        "UDDF"
    } else {
        // Default to BEF registry format
        "BEF"
    }
}

// Implement RegistryAware for Individual
impl RegistryAware for Individual {
    /// Get the registry name for this model
    fn registry_name() -> &'static str {
        "BEF" // Primary registry for Individuals
    }

    /// Create a model from a registry-specific record
    fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Detect registry type and route to the appropriate implementation
        let registry_type = detect_registry_type(batch);
        debug!("Detected registry type: {}", registry_type);

        match registry_type {
            "BEF" => Self::from_bef_record(batch, row),
            "IND" => Self::from_ind_record(batch, row),
            "LPR" => Self::from_lpr_record(batch, row),
            "MFR" => Self::from_mfr_record(batch, row),
            "AKM" => Self::from_akm_record(batch, row),
            "UDDF" => Self::from_uddf_record(batch, row),
            "VNDS" => Self::from_vnds_record(batch, row),
            _ => {
                warn!("Unsupported registry type: {}", registry_type);
                Ok(None)
            }
        }
    }

    /// Create models from an entire registry record batch
    fn from_registry_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Try serde_arrow conversion first for efficiency
        match Self::from_registry_batch_with_serde_arrow(batch) {
            Ok(individuals) => {
                debug!("Successfully used serde_arrow for batch deserialization");
                return Ok(individuals);
            }
            Err(err) => {
                debug!(
                    "Serde Arrow conversion failed, falling back to registry-specific conversion: {}",
                    err
                );
                // Fallback to registry-specific implementations
            }
        }

        // Detect registry type and route to the appropriate implementation
        let registry_type = detect_registry_type(batch);
        debug!("Detected registry type for batch: {}", registry_type);

        match registry_type {
            "BEF" => Self::from_bef_batch(batch),
            "IND" => Self::from_ind_batch(batch),
            "LPR" => Self::from_lpr_batch(batch),
            "MFR" => Self::from_mfr_batch(batch),
            "AKM" => Self::from_akm_batch(batch),
            "UDDF" => Self::from_uddf_batch(batch),
            "VNDS" => Self::from_vnds_batch(batch),
            _ => {
                // Fallback to row-by-row processing for unsupported registry types
                let mut individuals = Vec::new();
                for row in 0..batch.num_rows() {
                    if let Some(individual) = Self::from_registry_record(batch, row)? {
                        individuals.push(individual);
                    }
                }
                Ok(individuals)
            }
        }
    }
}

// Implement BefRegistry for Individual
impl BefRegistry for Individual {
    fn from_bef_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        use crate::registry::bef::conversion;
        conversion::from_bef_record(batch, row)
    }

    fn from_bef_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // Try to use serde_arrow first for more efficient deserialization
        match Self::from_registry_batch_with_serde_arrow(batch) {
            Ok(individuals) => Ok(individuals),
            Err(err) => {
                log::warn!(
                    "Serde Arrow conversion failed, falling back to row-by-row conversion: {}",
                    err
                );
                // Fall back to the traditional conversion method
                use crate::registry::bef::conversion;
                conversion::from_bef_batch(batch)
            }
        }
    }
}

// Implement IndRegistry for Individual
impl IndRegistry for Individual {
    fn from_ind_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Extract PNR - required for identification
        let pnr_col = crate::utils::array_utils::get_column(batch, "PNR", &DataType::Utf8, false)?;
        let pnr = if let Some(array) = pnr_col {
            let string_array =
                crate::utils::array_utils::downcast_array::<StringArray>(&array, "PNR", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                string_array.value(row).to_string()
            } else {
                return Ok(None); // No valid PNR
            }
        } else {
            return Ok(None); // No PNR column
        };

        // Create a basic individual with just the PNR (to be enhanced later)
        let individual = Individual::new(
            pnr,
            Gender::Unknown, // Would be determined from data
            None,            // Birth date would come from other registries
        );

        Ok(Some(individual))
    }

    fn from_ind_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let mut individuals = Vec::new();
        for row in 0..batch.num_rows() {
            if let Some(individual) = Self::from_ind_record(batch, row)? {
                individuals.push(individual);
            }
        }
        Ok(individuals)
    }
}

// Implement LprRegistry for Individual
impl LprRegistry for Individual {
    fn from_lpr_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Extract PNR from LPR registry data
        let pnr_col = crate::utils::array_utils::get_column(batch, "PNR", &DataType::Utf8, false)?;
        let pnr = if let Some(array) = pnr_col {
            let string_array =
                crate::utils::array_utils::downcast_array::<StringArray>(&array, "PNR", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                string_array.value(row).to_string()
            } else {
                return Ok(None); // No valid PNR
            }
        } else {
            return Ok(None); // No PNR column
        };

        // Create a basic individual with just the PNR
        let individual = Individual::new(pnr, Gender::Unknown, None);

        Ok(Some(individual))
    }

    fn from_lpr_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let mut individuals = Vec::new();
        for row in 0..batch.num_rows() {
            if let Some(individual) = Self::from_lpr_record(batch, row)? {
                individuals.push(individual);
            }
        }
        Ok(individuals)
    }
}

// Implement MfrRegistry for Individual
impl MfrRegistry for Individual {
    fn from_mfr_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Extract PNR from MFR registry data (child's PNR)
        let pnr_col = crate::utils::array_utils::get_column(batch, "PNR", &DataType::Utf8, false)?;
        let pnr = if let Some(array) = pnr_col {
            let string_array =
                crate::utils::array_utils::downcast_array::<StringArray>(&array, "PNR", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                string_array.value(row).to_string()
            } else {
                return Ok(None); // No valid PNR
            }
        } else {
            return Ok(None); // No PNR column
        };

        // Create a basic individual with just the PNR
        let mut individual = Individual::new(
            pnr,
            Gender::Unknown, // We know it's a child, but gender might need to be extracted
            None,
        );

        // MFR registry might have birth date
        if let Ok(Some(birth_date)) =
            crate::utils::field_extractors::extract_date32(batch, row, "FOED_DAG", false)
        {
            individual.birth_date = Some(birth_date);
        }

        Ok(Some(individual))
    }

    fn from_mfr_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let mut individuals = Vec::new();
        for row in 0..batch.num_rows() {
            if let Some(individual) = Self::from_mfr_record(batch, row)? {
                individuals.push(individual);
            }
        }
        Ok(individuals)
    }
}

// Implement AkmRegistry for Individual
impl AkmRegistry for Individual {
    fn from_akm_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Extract PNR from AKM registry data
        let pnr_col = crate::utils::array_utils::get_column(batch, "PNR", &DataType::Utf8, false)?;
        let pnr = if let Some(array) = pnr_col {
            let string_array =
                crate::utils::array_utils::downcast_array::<StringArray>(&array, "PNR", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                string_array.value(row).to_string()
            } else {
                return Ok(None); // No valid PNR
            }
        } else {
            return Ok(None); // No PNR column
        };

        // Create a basic individual with just the PNR
        let individual = Individual::new(pnr, Gender::Unknown, None);

        Ok(Some(individual))
    }

    fn from_akm_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let mut individuals = Vec::new();
        for row in 0..batch.num_rows() {
            if let Some(individual) = Self::from_akm_record(batch, row)? {
                individuals.push(individual);
            }
        }
        Ok(individuals)
    }

    fn enhance_with_employment_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // Extract employment-related fields
        if let Ok(Some(occupation_code)) =
            crate::utils::field_extractors::extract_string(batch, row, "DISCO", false)
        {
            self.occupation_code = Some(occupation_code);
        }

        if let Ok(Some(industry_code)) =
            crate::utils::field_extractors::extract_string(batch, row, "BRANCHE", false)
        {
            self.industry_code = Some(industry_code);
        }

        if let Ok(Some(workplace_id)) =
            crate::utils::field_extractors::extract_string(batch, row, "ARB_STED_ID", false)
        {
            self.workplace_id = Some(workplace_id);
        }

        if let Ok(Some(hours)) =
            crate::utils::field_extractors::extract_float64(batch, row, "HELTID", false)
        {
            self.working_hours = Some(hours);
        }

        Ok(true)
    }
}

// Implement UddfRegistry for Individual
impl UddfRegistry for Individual {
    fn from_uddf_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Extract PNR from UDDF registry data
        let pnr_col = crate::utils::array_utils::get_column(batch, "PNR", &DataType::Utf8, false)?;
        let pnr = if let Some(array) = pnr_col {
            let string_array =
                crate::utils::array_utils::downcast_array::<StringArray>(&array, "PNR", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                string_array.value(row).to_string()
            } else {
                return Ok(None); // No valid PNR
            }
        } else {
            return Ok(None); // No PNR column
        };

        // Create a basic individual with just the PNR
        let individual = Individual::new(pnr, Gender::Unknown, None);

        Ok(Some(individual))
    }

    fn from_uddf_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let mut individuals = Vec::new();
        for row in 0..batch.num_rows() {
            if let Some(individual) = Self::from_uddf_record(batch, row)? {
                individuals.push(individual);
            }
        }
        Ok(individuals)
    }

    fn enhance_with_education_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // Implement education data enhancement
        use crate::models::core::types::EducationField;
        use crate::models::core::types::EducationLevel;

        // Extract education level from UDD_H
        if let Ok(Some(level_code)) =
            crate::utils::field_extractors::extract_int32(batch, row, "UDD_H", false)
        {
            self.education_level = match level_code {
                10 => EducationLevel::Basic,
                20 | 30 => EducationLevel::Secondary,
                40 | 50 => EducationLevel::Higher,
                60 => EducationLevel::Postgraduate,
                _ => EducationLevel::Unknown,
            };
        }

        // Extract education field from AUDD
        if let Ok(Some(field_code)) =
            crate::utils::field_extractors::extract_string(batch, row, "AUDD", false)
        {
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
        }

        // Extract education institution
        if let Ok(Some(institution)) =
            crate::utils::field_extractors::extract_string(batch, row, "UDD_INST", false)
        {
            self.education_institution = Some(institution);
        }

        // Extract education completion date
        if let Ok(Some(completion_date)) =
            crate::utils::field_extractors::extract_date32(batch, row, "AFSLUTNINGSDATO", false)
        {
            self.education_completion_date = Some(completion_date);
        }

        Ok(true)
    }
}

// Implement VndsRegistry for Individual
impl VndsRegistry for Individual {
    fn from_vnds_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Extract PNR from VNDS registry data
        let pnr_col = crate::utils::array_utils::get_column(batch, "PNR", &DataType::Utf8, false)?;
        let pnr = if let Some(array) = pnr_col {
            let string_array =
                crate::utils::array_utils::downcast_array::<StringArray>(&array, "PNR", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                string_array.value(row).to_string()
            } else {
                return Ok(None); // No valid PNR
            }
        } else {
            return Ok(None); // No PNR column
        };

        // Create a basic individual with just the PNR
        let individual = Individual::new(pnr, Gender::Unknown, None);

        Ok(Some(individual))
    }

    fn from_vnds_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let mut individuals = Vec::new();
        for row in 0..batch.num_rows() {
            if let Some(individual) = Self::from_vnds_record(batch, row)? {
                individuals.push(individual);
            }
        }
        Ok(individuals)
    }

    fn enhance_with_migration_data(&mut self, batch: &RecordBatch, row: usize) -> Result<bool> {
        // Extract migration-related fields
        if let Ok(Some(emigration_date)) =
            crate::utils::field_extractors::extract_date32(batch, row, "UDRDTO", false)
        {
            self.emigration_date = Some(emigration_date);
        }

        if let Ok(Some(immigration_date)) =
            crate::utils::field_extractors::extract_date32(batch, row, "INDRDTO", false)
        {
            self.immigration_date = Some(immigration_date);
        }

        Ok(true)
    }
}

// Implement RegistryAware for Child
impl RegistryAware for Child {
    /// Get the registry name for this model
    fn registry_name() -> &'static str {
        "MFR" // Primary registry for Children
    }

    /// Create a model from a registry-specific record
    fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // First create an Individual from the registry record
        if let Some(individual) = Individual::from_registry_record(batch, row)? {
            // Then convert that Individual to a Child
            Ok(Some(Self::from_individual(Arc::new(individual))))
        } else {
            Ok(None)
        }
    }

    /// Create models from an entire registry record batch
    fn from_registry_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // First create Individuals from the registry batch
        let individuals = Individual::from_registry_batch(batch)?;

        // Then convert those Individuals to Children
        let children = individuals
            .into_iter()
            .map(|individual| Self::from_individual(Arc::new(individual)))
            .collect();

        Ok(children)
    }
}

// Implement MfrRegistry for Child
impl MfrRegistry for Child {
    fn from_mfr_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // First create an Individual from the MFR registry record
        if let Some(individual) = Individual::from_mfr_record(batch, row)? {
            // Then convert that Individual to a Child
            Ok(Some(Self::from_individual(Arc::new(individual))))
        } else {
            Ok(None)
        }
    }

    fn from_mfr_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        // First create Individuals from the MFR registry batch
        let individuals = Individual::from_mfr_batch(batch)?;

        // Then convert those Individuals to Children
        let children = individuals
            .into_iter()
            .map(|individual| Self::from_individual(Arc::new(individual)))
            .collect();

        Ok(children)
    }
}

/// Extension methods for Individual for direct serde_arrow conversion from registry data
impl Individual {
    /// Convert a registry batch to Individual models using serde_arrow
    ///
    /// This method uses serde_arrow for efficient direct deserialization from Arrow to Rust structs.
    /// It handles field name mapping and type conversions automatically.
    pub fn from_registry_batch_with_serde_arrow(batch: &RecordBatch) -> Result<Vec<Self>> {
        use serde_arrow::schema::SchemaLike;
        use serde_arrow::schema::TracingOptions;
        use std::collections::HashMap;

        // Create schema options that are tolerant of missing fields and nulls
        let options = TracingOptions::default().allow_null_fields(true);

        // We'll use different field mappings based on registry type
        let registry_type = detect_registry_type(batch);
        debug!("Using serde_arrow for registry type: {}", registry_type);

        // Attempt direct deserialization
        let result = match registry_type {
            "BEF" => {
                // BEF field mapping handles special field name conversions
                Self::deserialize_with_field_mapping(batch, Self::bef_field_mapping())
            }
            "IND" => {
                // IND field mapping
                Self::deserialize_with_field_mapping(batch, Self::ind_field_mapping())
            }
            _ => {
                // For other registry types, try direct deserialization
                serde_arrow::from_record_batch(batch)
                    .map_err(|e| anyhow::anyhow!("Serde Arrow deserialization error: {}", e))
            }
        };

        match result {
            Ok(individuals) => {
                // Post-process the individuals to ensure consistent field values
                let mut processed_individuals = individuals;
                for individual in &mut processed_individuals {
                    Self::post_process_serde_arrow(individual, registry_type);
                }
                Ok(processed_individuals)
            }
            Err(e) => Err(anyhow::anyhow!("Failed to deserialize batch: {}", e)),
        }
    }

    /// Deserialize a RecordBatch with custom field mapping
    fn deserialize_with_field_mapping(
        batch: &RecordBatch,
        field_mapping: HashMap<String, String>,
    ) -> Result<Vec<Self>> {
        // Use the new BefSerdeConverter for BEF registry if it's a BEF batch
        if detect_registry_type(batch) == "BEF" {
            use crate::registry::conversions::bef::BefSerdeConverter;
            return BefSerdeConverter::convert_batch_with_serde_arrow(batch);
        }

        // Fall back to generic implementation for other registry types
        use arrow::datatypes::{DataType, Field, Schema};

        // Create a new schema with mapped field names
        let mut fields = Vec::new();
        for field in batch.schema().fields() {
            let field_name = field.name();
            if let Some(mapped_name) = field_mapping.get(field_name) {
                fields.push(Field::new(
                    mapped_name,
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            } else {
                fields.push(field.clone());
            }
        }

        let mapped_schema = Schema::new(fields);

        // Create a new RecordBatch with the mapped schema
        let mapped_batch = RecordBatch::try_new(Arc::new(mapped_schema), batch.columns().to_vec())?;

        // Now deserialize using the mapped batch
        serde_arrow::from_record_batch(&mapped_batch)
            .map_err(|e| anyhow::anyhow!("Serde Arrow deserialization error: {}", e))
    }

    /// Post-process individuals created through serde_arrow
    fn post_process_serde_arrow(individual: &mut Self, registry_type: &str) {
        match registry_type {
            "BEF" => {
                // Convert gender code to Gender enum for BEF
                if individual.gender == Gender::Unknown {
                    // For testing, manually extract gender from BEF format
                    if let Some(gender_code) = &individual.extract_field::<String>("gender_code") {
                        individual.gender = match gender_code.as_str() {
                            "M" => Gender::Male,
                            "F" => Gender::Female,
                            _ => Gender::Unknown,
                        };
                    }
                }
            }
            "IND" => {
                // Handle socioeconomic status codes
                if individual.socioeconomic_status == SocioeconomicStatus::Unknown {
                    if let Some(status_code) =
                        individual.extract_field::<i32>("socioeconomic_status_code")
                    {
                        individual.socioeconomic_status = match status_code {
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
                    }
                }
            }
            _ => {
                // No special post-processing for other registry types
            }
        }
    }

    /// Helper method to extract a field value for post-processing
    fn extract_field<T: 'static>(&self, field_name: &str) -> Option<T> {
        // This would require reflection capabilities that Rust doesn't have natively
        // In a real implementation, we might use a custom serialization framework
        // or a dynamic type system to extract fields by name

        // Simple hardcoded implementations for testing
        if field_name == "gender_code"
            && std::any::TypeId::of::<T>() == std::any::TypeId::of::<String>()
        {
            if self.gender == Gender::Male {
                return Some("M".to_string()) as Option<T>;
            } else if self.gender == Gender::Female {
                return Some("F".to_string()) as Option<T>;
            }
        }

        None
    }

    /// Get field mapping for BEF registry
    fn bef_field_mapping() -> HashMap<String, String> {
        use crate::registry::conversions::bef::BefSerdeConverter;
        BefSerdeConverter::field_mapping()
    }

    /// Get field mapping for IND registry
    fn ind_field_mapping() -> HashMap<String, String> {
        let mut mapping = HashMap::new();
        mapping.insert("PNR".to_string(), "pnr".to_string());
        mapping.insert("PERINDKIALT".to_string(), "annual_income".to_string());
        mapping.insert("DISPON_NY".to_string(), "disposable_income".to_string());
        mapping.insert("LOENMV".to_string(), "employment_income".to_string());
        mapping.insert(
            "NETOVSKUD".to_string(),
            "self_employment_income".to_string(),
        );
        mapping.insert("KPITALIND".to_string(), "capital_income".to_string());
        mapping.insert("OFFHJ".to_string(), "transfer_income".to_string());
        mapping.insert("AAR".to_string(), "income_year".to_string());
        mapping.insert("SOCIO".to_string(), "socioeconomic_status_code".to_string());
        mapping
    }
}
