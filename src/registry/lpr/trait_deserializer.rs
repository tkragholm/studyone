//! LPR registry trait-based deserializer
//!
//! This module provides functionality for deserializing LPR registry data
//! using the trait-based field access system.

use std::any::Any;
use std::collections::HashMap;

use arrow::array::{Array, Date32Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use log::debug;

use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::registry_traits::LprFields;
use crate::registry::trait_deserializer::{RegistryDeserializer, RegistryFieldExtractor};

/// LPR field extractor for string fields
struct LprStringExtractor {
    source_field: String,
    target_field: String,
    setter: Box<dyn Fn(&mut dyn Any, Option<String>) -> Result<()> + Send + Sync>,
}

impl LprStringExtractor {
    /// Create a new string field extractor
    pub fn new<F>(source_field: &str, target_field: &str, setter: F) -> Self
    where
        F: Fn(&mut dyn Any, Option<String>) -> Result<()> + Send + Sync + 'static,
    {
        Self {
            source_field: source_field.to_string(),
            target_field: target_field.to_string(),
            setter: Box::new(setter),
        }
    }
}

impl RegistryFieldExtractor for LprStringExtractor {
    fn extract_and_set(&self, batch: &RecordBatch, row: usize, target: &mut dyn Any) -> Result<()> {
        // Get the column by name
        if let Ok(col_idx) = batch.schema().index_of(&self.source_field) {
            let array = batch.column(col_idx);

            // Extract the value as a string
            let value = if array.is_null(row) {
                None
            } else if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                Some(string_array.value(row).to_string())
            } else {
                // Try to convert any other type to string
                // Generic conversion without specific array type - just make a string representation
                Some(format!("{array:?}"))
            };

            // Set the value using the provided setter
            (self.setter)(target, value)?;
            Ok(())
        } else {
            // Field not found, just skip it
            Ok(())
        }
    }

    fn source_field_name(&self) -> &str {
        &self.source_field
    }

    fn target_field_name(&self) -> &str {
        &self.target_field
    }
}

/// LPR field extractor for date fields
struct LprDateExtractor {
    source_field: String,
    target_field: String,
    setter: Box<dyn Fn(&mut dyn Any, Option<NaiveDate>) -> Result<()> + Send + Sync>,
}

impl LprDateExtractor {
    /// Create a new date field extractor
    pub fn new<F>(source_field: &str, target_field: &str, setter: F) -> Self
    where
        F: Fn(&mut dyn Any, Option<NaiveDate>) -> Result<()> + Send + Sync + 'static,
    {
        Self {
            source_field: source_field.to_string(),
            target_field: target_field.to_string(),
            setter: Box::new(setter),
        }
    }
}

impl RegistryFieldExtractor for LprDateExtractor {
    fn extract_and_set(&self, batch: &RecordBatch, row: usize, target: &mut dyn Any) -> Result<()> {
        // Get the column by name
        if let Ok(col_idx) = batch.schema().index_of(&self.source_field) {
            let array = batch.column(col_idx);

            // Extract the value as a date
            let value = if array.is_null(row) {
                None
            } else if let Some(date_array) = array.as_any().downcast_ref::<Date32Array>() {
                let days_since_epoch = date_array.value(row);
                // Convert from days since UNIX epoch to NaiveDate
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                Some(epoch + chrono::Duration::days(i64::from(days_since_epoch)))
            } else {
                None
            };

            // Set the value using the provided setter
            (self.setter)(target, value)?;
            Ok(())
        } else {
            // Field not found, just skip it
            Ok(())
        }
    }

    fn source_field_name(&self) -> &str {
        &self.source_field
    }

    fn target_field_name(&self) -> &str {
        &self.target_field
    }
}

/// LPR field extractor for integer fields
struct LprIntExtractor {
    source_field: String,
    target_field: String,
    setter: Box<dyn Fn(&mut dyn Any, Option<i32>) -> Result<()> + Send + Sync>,
}

impl LprIntExtractor {
    /// Create a new integer field extractor
    pub fn new<F>(source_field: &str, target_field: &str, setter: F) -> Self
    where
        F: Fn(&mut dyn Any, Option<i32>) -> Result<()> + Send + Sync + 'static,
    {
        Self {
            source_field: source_field.to_string(),
            target_field: target_field.to_string(),
            setter: Box::new(setter),
        }
    }
}

impl RegistryFieldExtractor for LprIntExtractor {
    fn extract_and_set(&self, batch: &RecordBatch, row: usize, target: &mut dyn Any) -> Result<()> {
        // Get the column by name
        if let Ok(col_idx) = batch.schema().index_of(&self.source_field) {
            let array = batch.column(col_idx);

            // Extract the value as an integer
            let value = if array.is_null(row) {
                None
            } else {
                // We use a generic approach that works with various numeric array types
                match array.data_type() {
                    arrow::datatypes::DataType::Int8 => {
                        let array = arrow::array::cast::as_primitive_array::<
                            arrow::datatypes::Int8Type,
                        >(array);
                        Some(i32::from(array.value(row)))
                    }
                    arrow::datatypes::DataType::Int16 => {
                        let array = arrow::array::cast::as_primitive_array::<
                            arrow::datatypes::Int16Type,
                        >(array);
                        Some(i32::from(array.value(row)))
                    }
                    arrow::datatypes::DataType::Int32 => {
                        let array = arrow::array::cast::as_primitive_array::<
                            arrow::datatypes::Int32Type,
                        >(array);
                        Some(array.value(row))
                    }
                    arrow::datatypes::DataType::Int64 => {
                        let array = arrow::array::cast::as_primitive_array::<
                            arrow::datatypes::Int64Type,
                        >(array);
                        Some(array.value(row) as i32)
                    }
                    arrow::datatypes::DataType::UInt8 => {
                        let array = arrow::array::cast::as_primitive_array::<
                            arrow::datatypes::UInt8Type,
                        >(array);
                        Some(i32::from(array.value(row)))
                    }
                    arrow::datatypes::DataType::UInt16 => {
                        let array = arrow::array::cast::as_primitive_array::<
                            arrow::datatypes::UInt16Type,
                        >(array);
                        Some(i32::from(array.value(row)))
                    }
                    arrow::datatypes::DataType::UInt32 => {
                        let array = arrow::array::cast::as_primitive_array::<
                            arrow::datatypes::UInt32Type,
                        >(array);
                        Some(array.value(row) as i32)
                    }
                    _ => None,
                }
            };

            // Set the value using the provided setter
            (self.setter)(target, value)?;
            Ok(())
        } else {
            // Field not found, just skip it
            Ok(())
        }
    }

    fn source_field_name(&self) -> &str {
        &self.source_field
    }

    fn target_field_name(&self) -> &str {
        &self.target_field
    }
}

/// LPR field extractor for diagnosis fields
///
/// This special extractor handles adding diagnosis codes to the `LprFields` trait
struct LprDiagnosisExtractor {
    source_field: String,
}

impl LprDiagnosisExtractor {
    /// Create a new diagnosis field extractor
    pub fn new(source_field: &str) -> Self {
        Self {
            source_field: source_field.to_string(),
        }
    }
}

impl RegistryFieldExtractor for LprDiagnosisExtractor {
    fn extract_and_set(&self, batch: &RecordBatch, row: usize, target: &mut dyn Any) -> Result<()> {
        // Get the column by name
        if let Ok(col_idx) = batch.schema().index_of(&self.source_field) {
            let array = batch.column(col_idx);

            // Extract the value as a string
            if !array.is_null(row) {
                if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
                    let diagnosis = string_array.value(row).to_string();

                    // Add the diagnosis to the Individual using the LprFields trait
                    if let Some(individual) = target.downcast_mut::<Individual>() {
                        let lpr_fields: &mut dyn LprFields = individual;
                        lpr_fields.add_diagnosis(diagnosis);
                    }
                }
            }

            Ok(())
        } else {
            // Field not found, just skip it
            Ok(())
        }
    }

    fn source_field_name(&self) -> &str {
        &self.source_field
    }

    fn target_field_name(&self) -> &'static str {
        "diagnoses"
    }
}

/// LPR registry ADM deserializer that uses the trait-based field access system
pub struct LprAdmTraitDeserializer {
    field_extractors: Vec<Box<dyn RegistryFieldExtractor>>,
    field_map: HashMap<String, String>,
}

impl Default for LprAdmTraitDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl LprAdmTraitDeserializer {
    /// Create a new LPR ADM trait deserializer
    #[must_use] pub fn new() -> Self {
        let mut field_extractors: Vec<Box<dyn RegistryFieldExtractor>> = Vec::new();

        // Add string field extractors
        field_extractors.push(Box::new(LprStringExtractor::new(
            "PNR",
            "pnr",
            |target, value| {
                if let Some(individual) = target.downcast_mut::<Individual>() {
                    individual.pnr = value.unwrap_or_default();
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Target is not an Individual"))
                }
            },
        )));

        // Add date field extractors
        field_extractors.push(Box::new(LprDateExtractor::new(
            "D_INDDTO",
            "hospital_admission",
            |target, value| {
                if let Some(individual) = target.downcast_mut::<Individual>() {
                    if let Some(date) = value {
                        let lpr_fields: &mut dyn LprFields = individual;
                        lpr_fields.add_hospital_admission(date);
                    }
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Target is not an Individual"))
                }
            },
        )));

        field_extractors.push(Box::new(LprDateExtractor::new(
            "D_UDDTO",
            "discharge_date",
            |target, value| {
                if let Some(individual) = target.downcast_mut::<Individual>() {
                    if let Some(date) = value {
                        let lpr_fields: &mut dyn LprFields = individual;
                        lpr_fields.add_discharge_date(date);
                    }
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Target is not an Individual"))
                }
            },
        )));

        // Add integer field extractors
        field_extractors.push(Box::new(LprIntExtractor::new(
            "V_SENGDAGE",
            "length_of_stay",
            |target, value| {
                if let Some(individual) = target.downcast_mut::<Individual>() {
                    let lpr_fields: &mut dyn LprFields = individual;
                    lpr_fields.set_length_of_stay(value);
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Target is not an Individual"))
                }
            },
        )));

        // Add diagnosis extractors
        field_extractors.push(Box::new(LprDiagnosisExtractor::new("C_ADIAG")));

        // Create empty field mapping (removed backward compatibility)
        let field_map = HashMap::new();

        Self {
            field_extractors,
            field_map,
        }
    }
}

impl RegistryDeserializer for LprAdmTraitDeserializer {
    fn registry_type(&self) -> &'static str {
        "LPR_ADM"
    }

    fn field_extractors(&self) -> &[Box<dyn RegistryFieldExtractor>] {
        &self.field_extractors
    }

    fn field_mapping(&self) -> HashMap<String, String> {
        self.field_map.clone()
    }
}

/// LPR registry DIAG deserializer that uses the trait-based field access system
pub struct LprDiagTraitDeserializer {
    field_extractors: Vec<Box<dyn RegistryFieldExtractor>>,
    field_map: HashMap<String, String>,
    pnr_lookup: Option<HashMap<String, String>>,
}

impl Default for LprDiagTraitDeserializer {
    fn default() -> Self {
        Self::new()
    }
}

impl LprDiagTraitDeserializer {
    /// Create a new LPR DIAG trait deserializer
    #[must_use] pub fn new() -> Self {
        let mut field_extractors: Vec<Box<dyn RegistryFieldExtractor>> = Vec::new();

        // Add diagnosis extractors
        field_extractors.push(Box::new(LprDiagnosisExtractor::new("C_DIAG")));

        // RECNUM is used for PNR lookup
        field_extractors.push(Box::new(LprStringExtractor::new(
            "RECNUM",
            "recnum",
            |_, _| Ok(()),
        )));

        // Create empty field mapping (removed backward compatibility)
        let field_map = HashMap::new();

        Self {
            field_extractors,
            field_map,
            pnr_lookup: None,
        }
    }

    /// Set PNR lookup table for this deserializer
    #[must_use] pub fn with_pnr_lookup(mut self, lookup: HashMap<String, String>) -> Self {
        self.pnr_lookup = Some(lookup);
        self
    }

    /// Get a PNR from RECNUM using the lookup table
    fn get_pnr_from_recnum(&self, recnum: &str) -> Option<String> {
        self.pnr_lookup
            .as_ref()
            .and_then(|lookup| lookup.get(recnum).cloned())
    }

    /// Deserialize a DIAG row - needs special handling due to PNR lookup
    pub fn deserialize_diag_row(
        &self,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Option<Individual>> {
        // First, try to get the RECNUM
        use crate::utils::array_utils::{downcast_array, get_column};
        use arrow::array::StringArray;
        use arrow::datatypes::DataType;

        let recnum_col = get_column(batch, "RECNUM", &DataType::Utf8, false)?;
        let recnum = if let Some(array) = recnum_col {
            let string_array = downcast_array::<StringArray>(&array, "RECNUM", "String")?;
            if row < string_array.len() && !string_array.is_null(row) {
                string_array.value(row).to_string()
            } else {
                return Ok(None); // No valid RECNUM
            }
        } else {
            return Ok(None); // No RECNUM column
        };

        // Look up the PNR from the RECNUM
        let pnr = if let Some(pnr) = self.get_pnr_from_recnum(&recnum) {
            pnr
        } else {
            return Ok(None); // No PNR found for this RECNUM
        };

        // Create a basic individual
        let mut individual = Individual::new(pnr, None);

        // Apply field extractors
        for extractor in self.field_extractors() {
            extractor.extract_and_set(batch, row, &mut individual as &mut dyn Any)?;
        }

        Ok(Some(individual))
    }
}

impl RegistryDeserializer for LprDiagTraitDeserializer {
    fn registry_type(&self) -> &'static str {
        "LPR_DIAG"
    }

    fn field_extractors(&self) -> &[Box<dyn RegistryFieldExtractor>] {
        &self.field_extractors
    }

    fn field_mapping(&self) -> HashMap<String, String> {
        self.field_map.clone()
    }

    fn deserialize_row(&self, batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
        self.deserialize_diag_row(batch, row)
    }

    fn deserialize_batch(&self, batch: &RecordBatch) -> Result<Vec<Individual>> {
        let mut individuals = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            if let Some(individual) = self.deserialize_diag_row(batch, row)? {
                individuals.push(individual);
            }
        }

        Ok(individuals)
    }
}

/// Deserialize an LPR ADM record batch using the trait-based deserializer
///
/// # Arguments
///
/// * `batch` - The LPR ADM record batch to deserialize
///
/// # Returns
///
/// A Result containing a Vec of Individual models
pub fn deserialize_adm_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing LPR ADM batch with trait-based deserializer");

    let deserializer = LprAdmTraitDeserializer::new();
    deserializer.deserialize_batch(batch)
}

/// Deserialize a single row from an LPR ADM record batch
///
/// # Arguments
///
/// * `batch` - The LPR ADM record batch
/// * `row` - The row index to deserialize
///
/// # Returns
///
/// A Result containing an Option with the deserialized Individual
pub fn deserialize_adm_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    let deserializer = LprAdmTraitDeserializer::new();
    deserializer.deserialize_row(batch, row)
}

/// Deserialize an LPR DIAG record batch using the trait-based deserializer
///
/// # Arguments
///
/// * `batch` - The LPR DIAG record batch to deserialize
/// * `pnr_lookup` - A mapping from RECNUM to PNR
///
/// # Returns
///
/// A Result containing a Vec of Individual models
pub fn deserialize_diag_batch(
    batch: &RecordBatch,
    pnr_lookup: Option<HashMap<String, String>>,
) -> Result<Vec<Individual>> {
    debug!("Deserializing LPR DIAG batch with trait-based deserializer");

    let deserializer = if let Some(lookup) = pnr_lookup {
        LprDiagTraitDeserializer::new().with_pnr_lookup(lookup)
    } else {
        LprDiagTraitDeserializer::new()
    };

    deserializer.deserialize_batch(batch)
}

/// Deserialize a single row from an LPR DIAG record batch
///
/// # Arguments
///
/// * `batch` - The LPR DIAG record batch
/// * `row` - The row index to deserialize
/// * `pnr_lookup` - A mapping from RECNUM to PNR
///
/// # Returns
///
/// A Result containing an Option with the deserialized Individual
pub fn deserialize_diag_row(
    batch: &RecordBatch,
    row: usize,
    pnr_lookup: Option<HashMap<String, String>>,
) -> Result<Option<Individual>> {
    let deserializer = if let Some(lookup) = pnr_lookup {
        LprDiagTraitDeserializer::new().with_pnr_lookup(lookup)
    } else {
        LprDiagTraitDeserializer::new()
    };

    deserializer.deserialize_row(batch, row)
}
