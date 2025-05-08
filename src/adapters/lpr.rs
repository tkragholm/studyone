//! LPR Registry Adapters
//!
//! This module provides adapters for converting LPR registry data
//! to Diagnosis domain models using the unified adapter interface.

use crate::common::traits::{ModelLookup, RegistryAdapter, StatefulAdapter};
use crate::error::Result;
use crate::models::Diagnosis;
use crate::registry::RegisterLoader;
use crate::registry::{
    Lpr3DiagnoserRegister, LprDiagRegister, ModelConversion,
};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Adapter for converting LPR diagnosis data to Diagnosis models
#[derive(Debug)]
pub struct LprDiagnosisAdapter {
    registry: LprDiagRegister,
}

impl LprDiagnosisAdapter {
    /// Create a new LPR diagnosis adapter
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: LprDiagRegister::new(),
        }
    }
}

impl Default for LprDiagnosisAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl StatefulAdapter<Diagnosis> for LprDiagnosisAdapter {
    fn convert_batch(&self, batch: &RecordBatch) -> Result<Vec<Diagnosis>> {
        ModelConversion::to_models(&self.registry, batch)
    }

    fn transform_models(&self, models: &mut [Diagnosis]) -> Result<()> {
        ModelConversion::transform_models(&self.registry, models)
    }
}

impl RegistryAdapter<Diagnosis> for LprDiagnosisAdapter {
    fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Diagnosis>> {
        let registry = LprDiagRegister::new();
        ModelConversion::to_models(&registry, batch)
    }

    fn transform(models: &mut [Diagnosis]) -> Result<()> {
        let registry = LprDiagRegister::new();
        ModelConversion::transform_models(&registry, models)
    }
}

/// Adapter for converting LPR3 diagnosis data to Diagnosis models
#[derive(Debug)]
pub struct Lpr3DiagnosisAdapter {
    registry: Lpr3DiagnoserRegister,
}

impl Lpr3DiagnosisAdapter {
    /// Create a new LPR3 diagnosis adapter
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: Lpr3DiagnoserRegister::new(),
        }
    }
}

impl Default for Lpr3DiagnosisAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl StatefulAdapter<Diagnosis> for Lpr3DiagnosisAdapter {
    fn convert_batch(&self, batch: &RecordBatch) -> Result<Vec<Diagnosis>> {
        ModelConversion::to_models(&self.registry, batch)
    }

    fn transform_models(&self, models: &mut [Diagnosis]) -> Result<()> {
        ModelConversion::transform_models(&self.registry, models)
    }
}

/// Combined adapter that handles multiple LPR versions
#[derive(Debug)]
pub struct LprCombinedAdapter {
    lpr2_adapter: LprDiagnosisAdapter,
    lpr3_adapter: Lpr3DiagnosisAdapter,
}

impl LprCombinedAdapter {
    /// Create a new combined LPR adapter
    #[must_use]
    pub fn new() -> Self {
        Self {
            lpr2_adapter: LprDiagnosisAdapter::new(),
            lpr3_adapter: Lpr3DiagnosisAdapter::new(),
        }
    }

    /// Load data from both LPR2 and LPR3 registries
    ///
    /// # Arguments
    ///
    /// * `lpr2_path` - Path to LPR2 data
    /// * `lpr3_path` - Path to LPR3 data
    /// * `pnr_filter` - Optional PNR filter
    ///
    /// # Returns
    ///
    /// * Combined diagnosis data from both registries
    pub fn load_combined(
        &self,
        lpr2_path: &std::path::Path,
        lpr3_path: &std::path::Path,
        pnr_filter: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Diagnosis>> {
        // Load diagnoses from LPR2
        let lpr2_batches = self.lpr2_adapter.registry.load(lpr2_path, pnr_filter)?;
        let lpr2_diagnoses: Vec<Diagnosis> = lpr2_batches
            .into_iter()
            .flat_map(|batch| {
                ModelConversion::to_models(&self.lpr2_adapter.registry, &batch)
                    .unwrap_or_default()
            })
            .collect();

        // Load diagnoses from LPR3
        let lpr3_batches = self.lpr3_adapter.registry.load(lpr3_path, pnr_filter)?;
        let lpr3_diagnoses: Vec<Diagnosis> = lpr3_batches
            .into_iter()
            .flat_map(|batch| {
                ModelConversion::to_models(&self.lpr3_adapter.registry, &batch)
                    .unwrap_or_default()
            })
            .collect();

        // Combine results
        let mut all_diagnoses = Vec::with_capacity(lpr2_diagnoses.len() + lpr3_diagnoses.len());
        all_diagnoses.extend(lpr2_diagnoses);
        all_diagnoses.extend(lpr3_diagnoses);

        Ok(all_diagnoses)
    }

    /// Load data from both LPR2 and LPR3 registries asynchronously
    ///
    /// # Arguments
    ///
    /// * `lpr2_path` - Path to LPR2 data
    /// * `lpr3_path` - Path to LPR3 data
    /// * `pnr_filter` - Optional PNR filter
    ///
    /// # Returns
    ///
    /// * Combined diagnosis data from both registries
    pub async fn load_combined_async(
        &self,
        lpr2_path: &std::path::Path,
        lpr3_path: &std::path::Path,
        pnr_filter: Option<&std::collections::HashSet<String>>,
    ) -> Result<Vec<Diagnosis>> {
        use futures::future;

        // Load diagnoses from both registries concurrently
        let (lpr2_batches, lpr3_batches) = future::join(
            self.lpr2_adapter.registry.load_async(lpr2_path, pnr_filter),
            self.lpr3_adapter.registry.load_async(lpr3_path, pnr_filter),
        )
        .await;

        // Handle results
        let lpr2_batches = lpr2_batches?;
        let lpr3_batches = lpr3_batches?;

        // Convert batches to diagnoses
        let lpr2_diagnoses: Vec<Diagnosis> = lpr2_batches
            .into_iter()
            .flat_map(|batch| {
                ModelConversion::to_models(&self.lpr2_adapter.registry, &batch)
                    .unwrap_or_default()
            })
            .collect();

        let lpr3_diagnoses: Vec<Diagnosis> = lpr3_batches
            .into_iter()
            .flat_map(|batch| {
                ModelConversion::to_models(&self.lpr3_adapter.registry, &batch)
                    .unwrap_or_default()
            })
            .collect();

        // Combine results
        let mut all_diagnoses = Vec::with_capacity(lpr2_diagnoses.len() + lpr3_diagnoses.len());
        all_diagnoses.extend(lpr2_diagnoses);
        all_diagnoses.extend(lpr3_diagnoses);

        Ok(all_diagnoses)
    }
}

impl Default for LprCombinedAdapter {
    fn default() -> Self {
        Self::new()
    }
}

/// Implement `ModelLookup` for Diagnosis
impl ModelLookup<Diagnosis, (String, String)> for Diagnosis {
    /// Create a lookup map from (PNR, diagnosis code) to Diagnosis
    fn create_lookup(diagnoses: &[Diagnosis]) -> HashMap<(String, String), Arc<Diagnosis>> {
        let mut lookup = HashMap::with_capacity(diagnoses.len());
        for diagnosis in diagnoses {
            lookup.insert(
                (
                    diagnosis.individual_pnr.clone(),
                    diagnosis.diagnosis_code.clone(),
                ),
                Arc::new(diagnosis.clone()),
            );
        }
        lookup
    }
}
