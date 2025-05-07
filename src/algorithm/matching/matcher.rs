//! Core matching algorithm implementation
//!
//! This module implements the Matcher struct which orchestrates the matching process.

use crate::algorithm::matching::control_data::ControlData;
use crate::algorithm::matching::criteria::MatchingConfig;
use crate::algorithm::matching::extraction::extract_attributes_with_indices;
use crate::algorithm::matching::filtering::filter_batch_by_indices;
use crate::algorithm::matching::parallel::perform_parallel_matching;
use crate::algorithm::matching::sequential::perform_sequential_matching;
use crate::algorithm::matching::types::MatchingResult;
use crate::algorithm::matching::validation::validate_batches;
use crate::error::{ParquetReaderError, Result};
use arrow::record_batch::RecordBatch;
use log::info;
use std::time::Instant;

/// Matcher for pairing cases with controls
#[derive(Debug)]
pub struct Matcher {
    /// Matching configuration
    config: MatchingConfig,
}

impl Matcher {
    // Constants for optimization
    const PARALLEL_THRESHOLD: usize = 1000; // Threshold for switching to parallel processing

    /// Create a new matcher with the given configuration
    #[must_use]
    pub const fn new(config: MatchingConfig) -> Self {
        Self { config }
    }

    /// Perform matching between cases and controls
    ///
    /// # Arguments
    ///
    /// * `cases` - `RecordBatch` containing case records
    /// * `controls` - `RecordBatch` containing control records
    ///
    /// # Returns
    ///
    /// Result containing matched case and control batches
    pub fn perform_matching(
        &self,
        cases: &RecordBatch,
        controls: &RecordBatch,
    ) -> Result<MatchingResult> {
        let start_time = Instant::now();

        // Validate input batches
        validate_batches(cases, controls, &self.config)?;

        // Extract attributes from cases with indices
        let case_attributes = extract_attributes_with_indices(cases, &self.config)?;
        let control_attributes = extract_attributes_with_indices(controls, &self.config)?;

        if case_attributes.is_empty() {
            return Err(ParquetReaderError::ValidationError(
                "No valid cases found with complete required attributes".to_string(),
            )
            .into());
        }

        if control_attributes.is_empty() {
            return Err(ParquetReaderError::ValidationError(
                "No valid controls found with complete required attributes".to_string(),
            )
            .into());
        }

        info!(
            "Matching {} cases with control pool of {} candidates",
            case_attributes.pnrs.len(),
            control_attributes.pnrs.len()
        );

        // Create optimized control data structure
        let mut control_data = ControlData::new(
            control_attributes.pnrs,
            control_attributes.birth_dates,
            control_attributes.genders,
            control_attributes.family_sizes,
            control_attributes.indices,
        );

        // Sort controls by birth day for binary search
        control_data.sort_by_birth_day();

        // Track results
        let matching_ratio = self.config.matching_ratio;
        let mut matched_case_indices = Vec::with_capacity(case_attributes.pnrs.len());
        let mut matched_control_indices =
            Vec::with_capacity(case_attributes.pnrs.len() * matching_ratio);

        // Use parallel or sequential matching based on configuration and dataset size
        let use_parallel =
            self.config.use_parallel && case_attributes.pnrs.len() >= Self::PARALLEL_THRESHOLD;

        if use_parallel {
            // Parallel matching implementation
            perform_parallel_matching(
                case_attributes,
                &control_data,
                &mut matched_case_indices,
                &mut matched_control_indices,
                &self.config,
            )?;
        } else {
            // Sequential matching implementation
            perform_sequential_matching(
                case_attributes,
                &control_data,
                &mut matched_case_indices,
                &mut matched_control_indices,
                &self.config,
            )?;
        }

        if matched_case_indices.is_empty() {
            return Err(ParquetReaderError::ValidationError(
                "No matches found for any cases".to_string(),
            )
            .into());
        }

        // Create filtered RecordBatches with matched cases and controls
        let matched_cases = filter_batch_by_indices(cases, &matched_case_indices)?;
        let matched_controls = filter_batch_by_indices(controls, &matched_control_indices)?;

        let elapsed = start_time.elapsed();

        info!(
            "Matching complete: {} cases matched with {} controls in {:.2?} ({:.2} cases/sec)",
            matched_cases.num_rows(),
            matched_controls.num_rows(),
            elapsed,
            matched_case_indices.len() as f64 / elapsed.as_secs_f64()
        );

        Ok(MatchingResult {
            matched_cases: matched_cases.clone(),
            matched_controls: matched_controls.clone(),
            matched_case_count: matched_cases.num_rows(),
            matched_control_count: matched_controls.num_rows(),
            matching_time: elapsed,
        })
    }
}
