//! LPR v2 registry trait-based deserializer
//!
//! This module provides functionality for deserializing LPR v2 registry data
//! using the trait-based field access system.

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use log::debug;
use std::collections::HashMap;

use crate::error::Result;
use crate::models::core::Individual;
use crate::models::core::registry_traits::LprFields;
use crate::registry::lpr::v2::schema::schema_unified::{
    create_lpr_adm_schema, create_lpr_bes_schema, create_lpr_diag_schema,
};
use crate::registry::trait_deserializer::RegistryDeserializer;

/// Generate trait deserializers for LPR v2 registries

// Generate trait deserializer for LPR_ADM
crate::generate_trait_deserializer!(LprAdmTraitDeserializer, "LPR_ADM", create_lpr_adm_schema);

// Generate trait deserializer for LPR_DIAG
crate::generate_trait_deserializer!(LprDiagTraitDeserializer, "LPR_DIAG", create_lpr_diag_schema);

// Generate trait deserializer for LPR_BES
crate::generate_trait_deserializer!(LprBesTraitDeserializer, "LPR_BES", create_lpr_bes_schema);

/// Deserialize an LPR ADM record batch using the trait-based deserializer
pub fn deserialize_adm_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing LPR ADM batch with trait-based deserializer");
    let deserializer = LprAdmTraitDeserializer::new();
    deserializer.deserialize_batch(batch)
}

/// Deserialize a single row from an LPR ADM record batch
pub fn deserialize_adm_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    let deserializer = LprAdmTraitDeserializer::new();
    deserializer.deserialize_row(batch, row)
}

/// Deserialize an LPR DIAG record batch using the trait-based deserializer
pub fn deserialize_diag_batch(
    batch: &RecordBatch,
    pnr_lookup: Option<HashMap<String, String>>,
) -> Result<Vec<Individual>> {
    debug!("Deserializing LPR DIAG batch with trait-based deserializer");
    if let Some(lookup) = pnr_lookup {
        let deserializer = LprDiagWithPnrDeserializer::new(lookup);
        deserializer.deserialize_batch(batch)
    } else {
        let deserializer = LprDiagTraitDeserializer::new();
        deserializer.deserialize_batch(batch)
    }
}

/// Deserialize a single row from an LPR DIAG record batch
pub fn deserialize_diag_row(
    batch: &RecordBatch,
    row: usize,
    pnr_lookup: Option<HashMap<String, String>>,
) -> Result<Option<Individual>> {
    if let Some(lookup) = pnr_lookup {
        let deserializer = LprDiagWithPnrDeserializer::new(lookup);
        deserializer.deserialize_row(batch, row)
    } else {
        let deserializer = LprDiagTraitDeserializer::new();
        deserializer.deserialize_row(batch, row)
    }
}

/// Deserialize an LPR BES record batch using the trait-based deserializer
pub fn deserialize_bes_batch(batch: &RecordBatch) -> Result<Vec<Individual>> {
    debug!("Deserializing LPR BES batch with trait-based deserializer");
    let deserializer = LprBesTraitDeserializer::new();
    deserializer.deserialize_batch(batch)
}

/// Deserialize a single row from an LPR BES record batch
pub fn deserialize_bes_row(batch: &RecordBatch, row: usize) -> Result<Option<Individual>> {
    let deserializer = LprBesTraitDeserializer::new();
    deserializer.deserialize_row(batch, row)
}

/// `LprDiagTraitDeserializer` with PNR lookup support
///
/// This extension of the auto-generated trait deserializer adds support for
/// PNR lookups when deserializing `LPR_DIAG` records.
pub struct LprDiagWithPnrDeserializer {
    inner: LprDiagTraitDeserializer,
    pnr_lookup: HashMap<String, String>,
}

impl LprDiagWithPnrDeserializer {
    /// Create a new `LPR_DIAG` trait deserializer with PNR lookup
    #[must_use] pub fn new(pnr_lookup: HashMap<String, String>) -> Self {
        Self {
            inner: LprDiagTraitDeserializer::new(),
            pnr_lookup,
        }
    }

    /// Get a PNR from RECNUM using the lookup table
    fn get_pnr_from_recnum(&self, recnum: &str) -> Option<String> {
        self.pnr_lookup.get(recnum).cloned()
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

        // Create a basic individual with the looked-up PNR
        let mut individual = Individual::new(pnr, None);

        // Apply field extractors
        for extractor in self.inner.field_extractors() {
            extractor.extract_and_set(batch, row, &mut individual as &mut dyn std::any::Any)?;
        }

        Ok(Some(individual))
    }
}

impl RegistryDeserializer for LprDiagWithPnrDeserializer {
    fn registry_type(&self) -> &'static str {
        "LPR_DIAG"
    }

    fn field_extractors(
        &self,
    ) -> &[Box<dyn crate::registry::trait_deserializer::RegistryFieldExtractor>] {
        self.inner.field_extractors()
    }

    fn field_mapping(&self) -> HashMap<String, String> {
        self.inner.field_mapping()
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

/// Enhance individuals with diagnosis information from an `LPR_DIAG` batch
///
/// This function takes a slice of Individual models and an `LPR_DIAG` record batch,
/// and adds diagnosis codes to individuals using a PNR lookup.
pub fn enhance_individuals_with_diagnoses(
    individuals: &mut [Individual],
    batch: &RecordBatch,
    pnr_lookup: &HashMap<String, String>,
) -> Result<usize> {
    let mut count = 0;

    // Create a deserializer with PNR lookup
    let deserializer = LprDiagWithPnrDeserializer::new(pnr_lookup.clone());

    // Create a map of PNRs to individuals for fast lookup
    let mut pnr_map = std::collections::HashMap::new();
    for (idx, individual) in individuals.iter().enumerate() {
        pnr_map.insert(individual.pnr.clone(), idx);
    }

    // Deserialize each row and add diagnoses to individuals
    for row in 0..batch.num_rows() {
        if let Some(diagnosis_individual) = deserializer.deserialize_row(batch, row)? {
            // Extract diagnosis info
            let lpr_fields: &dyn LprFields = &diagnosis_individual;
            if let Some(diagnoses) = lpr_fields.diagnoses() {
                if let Some(&idx) = pnr_map.get(&diagnosis_individual.pnr) {
                    // Add diagnoses to the corresponding individual
                    let target_individual = &mut individuals[idx];
                    let target_lpr_fields: &mut dyn LprFields = target_individual;

                    for diagnosis in diagnoses {
                        target_lpr_fields.add_diagnosis(diagnosis.to_string());
                    }

                    count += 1;
                }
            }
        }
    }

    Ok(count)
}

/// Enhance individuals with admission information from an `LPR_ADM` batch
///
/// This function takes a slice of Individual models and an `LPR_ADM` record batch,
/// and adds hospital admission dates and diagnosis information to individuals.
pub fn enhance_individuals_with_admissions(
    individuals: &mut [Individual],
    batch: &RecordBatch,
) -> Result<usize> {
    let mut count = 0;

    // Create a deserializer
    let deserializer = LprAdmTraitDeserializer::new();

    // Create a map of PNRs to individuals for fast lookup
    let mut pnr_map = std::collections::HashMap::new();
    for (idx, individual) in individuals.iter().enumerate() {
        pnr_map.insert(individual.pnr.clone(), idx);
    }

    // Create a map of RECNUMs to individuals for later linking
    let mut recnum_map = std::collections::HashMap::new();

    // Deserialize each row and add admission info to individuals
    for row in 0..batch.num_rows() {
        if let Some(adm_individual) = deserializer.deserialize_row(batch, row)? {
            // Get RECNUM if available for later linking
            if let Some(recnum) = get_recnum(batch, row)? {
                recnum_map.insert(recnum, adm_individual.pnr.clone());
            }

            if let Some(&idx) = pnr_map.get(&adm_individual.pnr) {
                // Extract admission info
                let lpr_fields: &dyn LprFields = &adm_individual;
                let target_individual = &mut individuals[idx];
                let target_lpr_fields: &mut dyn LprFields = target_individual;

                // Copy hospital admissions
                if let Some(admissions) = lpr_fields.hospital_admissions() {
                    for admission_date in admissions {
                        target_lpr_fields.add_hospital_admission(*admission_date);
                    }
                }

                // Copy discharge dates
                if let Some(discharges) = lpr_fields.discharge_dates() {
                    for discharge_date in discharges {
                        target_lpr_fields.add_discharge_date(*discharge_date);
                    }
                }

                // Copy length of stay
                if let Some(los) = lpr_fields.length_of_stay() {
                    target_lpr_fields.set_length_of_stay(Some(los));
                }

                // Also copy diagnoses from action diagnosis
                if let Some(diagnoses) = lpr_fields.diagnoses() {
                    for diagnosis in diagnoses {
                        target_lpr_fields.add_diagnosis(diagnosis.to_string());
                    }
                }

                count += 1;
            }
        }
    }

    // Return the RECNUM to PNR mapping for use with other LPR tables
    Ok(count)
}

/// Helper function to extract RECNUM from a batch row
fn get_recnum(batch: &RecordBatch, row: usize) -> Result<Option<String>> {
    use crate::utils::array_utils::{downcast_array, get_column};
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;

    let recnum_col = get_column(batch, "RECNUM", &DataType::Utf8, false)?;
    if let Some(array) = recnum_col {
        let string_array = downcast_array::<StringArray>(&array, "RECNUM", "String")?;
        if row < string_array.len() && !string_array.is_null(row) {
            Ok(Some(string_array.value(row).to_string()))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}
