//! Core matching algorithm for case-control matching
//!
//! This module implements the optimized matching algorithm for pairing cases
//! with controls based on the specified matching criteria.

use super::criteria::MatchingConfig;
use crate::error::{ParquetReaderError, Result};
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use arrow::compute;
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{info, warn};
use rand::prelude::*;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Result of the matching process
#[derive(Debug, Clone)]
pub struct MatchingResult {
    /// Matched cases batch
    pub matched_cases: RecordBatch,

    /// Matched controls batch
    pub matched_controls: RecordBatch,

    /// Number of cases matched
    pub matched_case_count: usize,

    /// Number of controls matched
    pub matched_control_count: usize,

    /// Time taken for matching
    pub matching_time: std::time::Duration,
}

/// Pair of matched case and control
#[derive(Debug, Clone)]
pub struct MatchedPair {
    /// Case PNR (personal identification number)
    pub case_pnr: String,

    /// Case birth date
    pub case_birth_date: NaiveDate,

    /// Control PNR
    pub control_pnr: String,

    /// Control birth date
    pub control_birth_date: NaiveDate,

    /// Date when the match was made
    pub match_date: NaiveDate,
}

/// Optimized struct-of-arrays data structure for controls
/// This improves cache locality by storing each attribute in its own contiguous array
struct ControlData {
    /// Array of control PNRs
    pnrs: Vec<String>,

    /// Array of birth dates stored as days since epoch for faster comparison
    birth_days: Vec<i32>,

    /// Original birth dates for output
    birth_dates: Vec<NaiveDate>,

    /// Array of genders
    genders: Vec<Option<String>>,

    /// Array of family sizes
    family_sizes: Vec<Option<i32>>,

    /// Record batch indices for the controls
    indices: Vec<usize>,
}

impl ControlData {
    /// Create a new `ControlData` from extracted control attributes
    fn new(
        pnrs: Vec<String>,
        birth_dates: Vec<NaiveDate>,
        genders: Vec<Option<String>>,
        family_sizes: Vec<Option<i32>>,
        indices: Vec<usize>,
    ) -> Self {
        let capacity = pnrs.len();
        let mut birth_days = Vec::with_capacity(capacity);

        // Calculate days from CE for each birth date for efficient comparison
        for date in &birth_dates {
            birth_days.push(date.num_days_from_ce());
        }

        Self {
            pnrs,
            birth_days,
            birth_dates,
            genders,
            family_sizes,
            indices,
        }
    }

    /// Sort the control data by birth days for more efficient searching
    fn sort_by_birth_day(&mut self) {
        // Create a vector of indices
        let mut idx_vec: Vec<usize> = (0..self.pnrs.len()).collect();

        // Sort indices by birth_days
        idx_vec.sort_unstable_by_key(|&i| self.birth_days[i]);

        // Create new arrays with sorted data
        let mut sorted_pnrs = Vec::with_capacity(self.pnrs.len());
        let mut sorted_birth_days = Vec::with_capacity(self.birth_days.len());
        let mut sorted_birth_dates = Vec::with_capacity(self.birth_dates.len());
        let mut sorted_genders = Vec::with_capacity(self.genders.len());
        let mut sorted_family_sizes = Vec::with_capacity(self.family_sizes.len());
        let mut sorted_indices = Vec::with_capacity(self.indices.len());

        for &i in &idx_vec {
            sorted_pnrs.push(self.pnrs[i].clone());
            sorted_birth_days.push(self.birth_days[i]);
            sorted_birth_dates.push(self.birth_dates[i]);
            sorted_genders.push(self.genders[i].clone());
            sorted_family_sizes.push(self.family_sizes[i]);
            sorted_indices.push(self.indices[i]);
        }

        // Replace the original arrays
        self.pnrs = sorted_pnrs;
        self.birth_days = sorted_birth_days;
        self.birth_dates = sorted_birth_dates;
        self.genders = sorted_genders;
        self.family_sizes = sorted_family_sizes;
        self.indices = sorted_indices;
    }

    /// Find the range of controls with birth days within the window
    fn find_birth_day_range(&self, target_birth_day: i32, window: i32) -> (usize, usize) {
        let min_birth_day = target_birth_day - window;
        let max_birth_day = target_birth_day + window;

        // Find the first index where birth_day >= min_birth_day
        let start_idx = match self.birth_days.binary_search_by(|&day| {
            if day < min_birth_day {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        // Find the first index where birth_day > max_birth_day
        let end_idx = match self.birth_days.binary_search_by(|&day| {
            if day <= max_birth_day {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }) {
            Ok(idx) => idx + 1,
            Err(idx) => idx,
        };

        (start_idx, end_idx)
    }

    /// Get the length of the control data
    fn len(&self) -> usize {
        self.pnrs.len()
    }

    /// Check if the control data is empty
    fn is_empty(&self) -> bool {
        self.pnrs.is_empty()
    }
}

/// Case data grouped by birth day ranges for efficient parallel processing
struct CaseGroup {
    /// Array of case PNRs
    pnrs: Vec<String>,

    /// Array of birth dates
    birth_dates: Vec<NaiveDate>,

    /// Array of genders
    genders: Vec<Option<String>>,

    /// Array of family sizes
    family_sizes: Vec<Option<i32>>,

    /// Record batch indices for the cases
    indices: Vec<usize>,

    /// Birth day range (start, end)
    birth_day_range: (i32, i32),
}

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
    pub fn new(config: MatchingConfig) -> Self {
        Self { config }
    }

    /// Perform matching between cases and controls
    ///
    /// # Arguments
    ///
    /// * `cases` - RecordBatch containing case records
    /// * `controls` - RecordBatch containing control records
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
        self.validate_batches(cases, controls)?;

        // Extract attributes from cases with indices
        let case_attributes = self.extract_attributes_with_indices(cases)?;
        let control_attributes = self.extract_attributes_with_indices(controls)?;

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
            self.perform_parallel_matching(
                case_attributes.clone(),
                &control_data,
                &mut matched_case_indices,
                &mut matched_control_indices,
            )?;
        } else {
            // Sequential matching implementation
            self.perform_sequential_matching(
                case_attributes.clone(),
                &control_data,
                &mut matched_case_indices,
                &mut matched_control_indices,
            )?;
        }

        if matched_case_indices.is_empty() {
            return Err(ParquetReaderError::ValidationError(
                "No matches found for any cases".to_string(),
            )
            .into());
        }

        // Create filtered RecordBatches with matched cases and controls
        let matched_cases = self.filter_batch_by_indices(cases, &matched_case_indices)?;
        let matched_controls = self.filter_batch_by_indices(controls, &matched_control_indices)?;

        let elapsed = start_time.elapsed();

        info!(
            "Matching complete: {} cases matched with {} controls in {:.2?} ({:.2} cases/sec)",
            matched_cases.num_rows(),
            matched_controls.num_rows(),
            elapsed,
            case_attributes.pnrs.len() as f64 / elapsed.as_secs_f64()
        );

        Ok(MatchingResult {
            matched_cases: matched_cases.clone(),
            matched_controls: matched_controls.clone(),
            matched_case_count: matched_cases.num_rows(),
            matched_control_count: matched_controls.num_rows(),
            matching_time: elapsed,
        })
    }

    /// Validate that the input batches have the required columns
    fn validate_batches(&self, cases: &RecordBatch, controls: &RecordBatch) -> Result<()> {
        let required_columns = ["PNR", "FOED_DAG"];

        for column in &required_columns {
            if cases.schema().field_with_name(column).is_err() {
                return Err(ParquetReaderError::ValidationError(format!(
                    "Cases batch missing required column: {column}"
                ))
                .into());
            }

            if controls.schema().field_with_name(column).is_err() {
                return Err(ParquetReaderError::ValidationError(format!(
                    "Controls batch missing required column: {column}"
                ))
                .into());
            }
        }

        // Check for KOEN if gender matching is required
        if self.config.criteria.require_same_gender {
            if cases.schema().field_with_name("KOEN").is_err() {
                return Err(ParquetReaderError::ValidationError(
                    "Cases batch missing KOEN column required for gender matching".to_string(),
                )
                .into());
            }

            if controls.schema().field_with_name("KOEN").is_err() {
                return Err(ParquetReaderError::ValidationError(
                    "Controls batch missing KOEN column required for gender matching".to_string(),
                )
                .into());
            }
        }

        // Additional validation for other matching criteria would go here

        Ok(())
    }

    /// Extract attributes from a RecordBatch with indices
    fn extract_attributes_with_indices(&self, batch: &RecordBatch) -> Result<ExtractedAttributes> {
        let pnr_idx = batch
            .schema()
            .index_of("PNR")
            .map_err(|_| ParquetReaderError::ValidationError("PNR column not found".to_string()))?;

        let birth_date_idx = batch.schema().index_of("FOED_DAG").map_err(|_| {
            ParquetReaderError::ValidationError("FOED_DAG column not found".to_string())
        })?;

        // Gender is optional based on criteria
        let gender_idx = if self.config.criteria.require_same_gender {
            match batch.schema().index_of("KOEN") {
                Ok(idx) => Some(idx),
                Err(_) => None,
            }
        } else {
            None
        };

        // Family size is optional based on criteria
        let family_size_idx = if self.config.criteria.match_family_size {
            match batch.schema().index_of("ANTAL_BOERN") {
                Ok(idx) => Some(idx),
                Err(_) => {
                    warn!("ANTAL_BOERN column not found but family size matching is enabled");
                    None
                }
            }
        } else {
            None
        };

        // Extract PNR values
        let pnr_col = batch.column(pnr_idx);
        let pnr_array = pnr_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ParquetReaderError::ValidationError("PNR column is not a string array".to_string())
            })?;

        // Get birth date column
        let birth_date_col = batch.column(birth_date_idx);

        // Get gender column if available
        let gender_col = gender_idx.map(|idx| batch.column(idx));

        // Get family size column if available
        let family_size_col = family_size_idx.map(|idx| batch.column(idx));

        let mut pnrs = Vec::new();
        let mut birth_dates = Vec::new();
        let mut genders = Vec::new();
        let mut family_sizes = Vec::new();
        let mut indices = Vec::new();

        for i in 0..batch.num_rows() {
            // Skip if PNR is null
            if pnr_array.is_null(i) {
                continue;
            }

            // Extract birth date
            let birth_date = match arrow_array_to_date(&birth_date_col, i) {
                Some(date) => date,
                None => continue, // Skip if birth date is missing
            };

            // Extract gender if needed
            let gender = if let Some(col) = &gender_col {
                arrow_array_to_string(col, i)
            } else {
                None
            };

            // Extract family size if needed
            let family_size = if let Some(col) = &family_size_col {
                arrow_array_to_i32(col, i)
            } else {
                None
            };

            pnrs.push(pnr_array.value(i).to_string());
            birth_dates.push(birth_date);
            genders.push(gender);
            family_sizes.push(family_size);
            indices.push(i);
        }

        Ok(ExtractedAttributes {
            pnrs,
            birth_dates,
            genders,
            family_sizes,
            indices,
        })
    }

    /// Group cases by birth day ranges for parallel processing
    fn group_cases_by_birth_day_range(
        &self,
        attributes: ExtractedAttributes,
        num_groups: usize,
    ) -> Vec<CaseGroup> {
        if attributes.pnrs.is_empty() || num_groups == 0 {
            return Vec::new();
        }

        // Create vectors of tuples for sorting
        let mut sorted_tuples: Vec<(String, NaiveDate, Option<String>, Option<i32>, usize, i32)> =
            attributes
                .pnrs
                .into_iter()
                .zip(attributes.birth_dates.iter())
                .zip(attributes.genders.iter())
                .zip(attributes.family_sizes.iter())
                .zip(attributes.indices.iter())
                .map(|((((pnr, date), gender), family_size), &idx)| {
                    (
                        pnr,
                        *date,
                        gender.clone(),
                        *family_size,
                        idx,
                        date.num_days_from_ce(),
                    )
                })
                .collect();

        // Sort by birth day
        sorted_tuples.sort_by_key(|(_, _, _, _, _, days)| *days);

        // Find min and max birth days
        let min_birth_day = sorted_tuples[0].5;
        let max_birth_day = sorted_tuples[sorted_tuples.len() - 1].5;

        // Calculate range size
        let total_range = max_birth_day - min_birth_day + 1;
        let group_range_size = std::cmp::max(1, total_range / num_groups as i32);

        // Create groups
        let mut groups = Vec::with_capacity(num_groups);
        let mut current_start = min_birth_day;

        for _ in 0..num_groups {
            let current_end = std::cmp::min(current_start + group_range_size, max_birth_day + 1);

            // Filter tuples for this range
            let range_tuples: Vec<_> = sorted_tuples
                .iter()
                .filter(|(_, _, _, _, _, days)| *days >= current_start && *days < current_end)
                .cloned()
                .collect();

            if !range_tuples.is_empty() {
                // Split into separate vectors
                let mut pnrs = Vec::with_capacity(range_tuples.len());
                let mut birth_dates = Vec::with_capacity(range_tuples.len());
                let mut genders = Vec::with_capacity(range_tuples.len());
                let mut family_sizes = Vec::with_capacity(range_tuples.len());
                let mut indices = Vec::with_capacity(range_tuples.len());

                for (pnr, date, gender, family_size, idx, _) in range_tuples {
                    pnrs.push(pnr);
                    birth_dates.push(date);
                    genders.push(gender);
                    family_sizes.push(family_size);
                    indices.push(idx);
                }

                groups.push(CaseGroup {
                    pnrs,
                    birth_dates,
                    genders,
                    family_sizes,
                    indices,
                    birth_day_range: (current_start, current_end),
                });
            }

            current_start = current_end;

            // Break if we've reached the max birth day
            if current_start > max_birth_day {
                break;
            }
        }

        groups
    }

    /// Perform parallel matching for large datasets
    fn perform_parallel_matching(
        &self,
        case_attributes: ExtractedAttributes,
        control_data: &ControlData,
        matched_case_indices: &mut Vec<usize>,
        matched_control_indices: &mut Vec<usize>,
    ) -> Result<()> {
        // Set up progress reporting
        let mp = MultiProgress::new();
        let main_pb = mp.add(ProgressBar::new(case_attributes.pnrs.len() as u64));
        main_pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} cases ({per_sec}) {msg}")
                .unwrap()
                .progress_chars("#>-"),
        );

        // Determine the number of threads for optimal parallelism
        let num_threads = rayon::current_num_threads();
        info!("Using parallel processing with {num_threads} threads");

        // Group cases by birth day range for better parallelism
        let case_groups = self.group_cases_by_birth_day_range(case_attributes, num_threads);
        info!("Grouped cases into {} birth day ranges", case_groups.len());

        // Create shared collections for results
        let matched_cases = Arc::new(Mutex::new(Vec::with_capacity(
            case_groups.iter().map(|g| g.pnrs.len()).sum(),
        )));
        let matched_controls = Arc::new(Mutex::new(Vec::with_capacity(
            case_groups.iter().map(|g| g.pnrs.len()).sum::<usize>() * self.config.matching_ratio,
        )));
        let used_control_indices = Arc::new(Mutex::new(FxHashSet::default()));

        // Set up random number generator
        let rng_seed = self.config.random_seed;

        // Process each group in parallel
        let results: Vec<_> = case_groups
            .par_iter()
            .map(|group| {
                let group_size = group.pnrs.len();
                let group_pb = mp.add(ProgressBar::new(group_size as u64));
                group_pb.set_style(
                    ProgressStyle::default_bar()
                        .template("{spinner} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
                        .unwrap()
                        .progress_chars("#>-"),
                );
                group_pb.set_message(format!("Range: {} to {}", group.birth_day_range.0, group.birth_day_range.1));

                let mut local_matched_cases = Vec::with_capacity(group_size);
                let mut local_matched_controls = Vec::with_capacity(group_size * self.config.matching_ratio);

                // Create a thread-local RNG with the provided seed or from system entropy
                let mut thread_rng = match rng_seed {
                    Some(seed) => {
                        // Create a unique seed for each thread from the base seed
                        let thread_seed = seed.wrapping_add(group.birth_day_range.0 as u64);
                        StdRng::seed_from_u64(thread_seed)
                    },
                    None => StdRng::from_os_rng(),
                };

                for i in 0..group_size {
                    let case_pnr = &group.pnrs[i];
                    let case_birth_date = group.birth_dates[i];
                    let case_gender = &group.genders[i];
                    let case_family_size = &group.family_sizes[i];
                    let case_idx = group.indices[i];
                    let case_birth_day = case_birth_date.num_days_from_ce();

                    // Find range of potentially eligible controls using binary search
                    let (start_idx, end_idx) = control_data.find_birth_day_range(
                        case_birth_day,
                        self.config.criteria.birth_date_window_days
                    );

                    // Collect eligible controls
                    let mut eligible_control_indices = SmallVec::<[usize; 32]>::new();

                    // Check all controls in the range
                    {
                        let used_controls = used_control_indices.lock().unwrap();

                        for ctrl_idx in start_idx..end_idx {
                            // Skip if control already used
                            if used_controls.contains(&ctrl_idx) {
                                continue;
                            }

                            let control_pnr = &control_data.pnrs[ctrl_idx];

                            // Skip if case and control are the same person
                            if case_pnr == control_pnr {
                                continue;
                            }

                            // Check gender match if required
                            if self.config.criteria.require_same_gender {
                                if let Some(case_gender) = case_gender {
                                    if let Some(control_gender) = &control_data.genders[ctrl_idx] {
                                        if case_gender != control_gender {
                                            continue;
                                        }
                                    } else {
                                        continue;  // Control has no gender information
                                    }
                                }
                            }

                            // Check family size match if required
                            if self.config.criteria.match_family_size {
                                if let Some(case_size) = case_family_size {
                                    if let Some(control_size) = &control_data.family_sizes[ctrl_idx] {
                                        let diff = (case_size - control_size).abs();
                                        if diff > self.config.criteria.family_size_tolerance {
                                            continue;
                                        }
                                    } else {
                                        continue;  // Control has no family size information
                                    }
                                }
                            }

                            // Additional matching criteria would be checked here

                            eligible_control_indices.push(ctrl_idx);
                        }
                    }

                    // Select up to matching_ratio controls randomly
                    let num_to_select = std::cmp::min(self.config.matching_ratio, eligible_control_indices.len());
                    if num_to_select > 0 {
                        local_matched_cases.push(case_idx);

                        // Randomly select controls
                        let mut indices_vec: Vec<usize> = eligible_control_indices.into_iter().collect();
                        indices_vec.partial_shuffle(&mut thread_rng, num_to_select);

                        // Add selected controls to results and mark as used
                        let mut used_controls = used_control_indices.lock().unwrap();

                        for i in 0..num_to_select {
                            let ctrl_idx = indices_vec[i];
                            let control_batch_idx = control_data.indices[ctrl_idx];

                            local_matched_controls.push(control_batch_idx);
                            used_controls.insert(ctrl_idx);
                        }
                    }

                    group_pb.inc(1);
                    main_pb.inc(1);

                    if i % 100 == 0 {
                        group_pb.set_message(format!("Found {} matches", local_matched_cases.len()));
                    }
                }

                group_pb.finish_and_clear();

                // Return local results
                (local_matched_cases, local_matched_controls)
            })
            .collect();

        // Combine results from all groups
        for (local_cases, local_controls) in results {
            let mut all_cases = matched_cases.lock().unwrap();
            let mut all_controls = matched_controls.lock().unwrap();

            all_cases.extend(local_cases);
            all_controls.extend(local_controls);
        }

        // Extract results from mutexes
        *matched_case_indices = matched_cases.lock().unwrap().clone();
        *matched_control_indices = matched_controls.lock().unwrap().clone();

        main_pb.finish_with_message("Matching complete");

        Ok(())
    }

    /// Perform sequential matching for smaller datasets
    fn perform_sequential_matching(
        &self,
        case_attributes: ExtractedAttributes,
        control_data: &ControlData,
        matched_case_indices: &mut Vec<usize>,
        matched_control_indices: &mut Vec<usize>,
    ) -> Result<()> {
        info!(
            "Using sequential processing for {} cases",
            case_attributes.pnrs.len()
        );

        // Set up progress bar
        let pb = ProgressBar::new(case_attributes.pnrs.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} cases ({per_sec}) {msg}")
                .unwrap()
                .progress_chars("#>-"),
        );

        // Track which controls have been used
        let mut used_control_indices = FxHashSet::default();

        // Create RNG with optional seed
        let mut rng = match self.config.random_seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_os_rng(),
        };

        // Process each case
        for case_idx in 0..case_attributes.pnrs.len() {
            let case_pnr = &case_attributes.pnrs[case_idx];
            let case_birth_date = case_attributes.birth_dates[case_idx];
            let case_gender = &case_attributes.genders[case_idx];
            let case_family_size = &case_attributes.family_sizes[case_idx];
            let case_birth_day = case_birth_date.num_days_from_ce();
            let case_batch_idx = case_attributes.indices[case_idx];

            // Find range of potentially eligible controls using binary search
            let (start_idx, end_idx) = control_data
                .find_birth_day_range(case_birth_day, self.config.criteria.birth_date_window_days);

            // Collect eligible controls
            let mut eligible_control_indices = SmallVec::<[usize; 32]>::new();

            // Check all controls in the range
            for ctrl_idx in start_idx..end_idx {
                // Skip if control already used
                if used_control_indices.contains(&ctrl_idx) {
                    continue;
                }

                let control_pnr = &control_data.pnrs[ctrl_idx];

                // Skip if case and control are the same person
                if case_pnr == control_pnr {
                    continue;
                }

                // Check gender match if required
                if self.config.criteria.require_same_gender {
                    if let Some(case_gender) = case_gender {
                        if let Some(control_gender) = &control_data.genders[ctrl_idx] {
                            if case_gender != control_gender {
                                continue;
                            }
                        } else {
                            continue; // Control has no gender information
                        }
                    }
                }

                // Check family size match if required
                if self.config.criteria.match_family_size {
                    if let Some(case_size) = case_family_size {
                        if let Some(control_size) = &control_data.family_sizes[ctrl_idx] {
                            let diff = (case_size - control_size).abs();
                            if diff > self.config.criteria.family_size_tolerance {
                                continue;
                            }
                        } else {
                            continue; // Control has no family size information
                        }
                    }
                }

                // Additional matching criteria would be checked here

                eligible_control_indices.push(ctrl_idx);
            }

            // Select up to matching_ratio controls randomly
            let num_to_select =
                std::cmp::min(self.config.matching_ratio, eligible_control_indices.len());
            if num_to_select > 0 {
                matched_case_indices.push(case_batch_idx);

                // Randomly select controls
                let mut indices_vec: Vec<usize> = eligible_control_indices.into_iter().collect();
                indices_vec.partial_shuffle(&mut rng, num_to_select);

                // Add selected controls to results and mark as used
                for i in 0..num_to_select {
                    let ctrl_idx = indices_vec[i];
                    let control_batch_idx = control_data.indices[ctrl_idx];

                    matched_control_indices.push(control_batch_idx);
                    used_control_indices.insert(ctrl_idx);
                }
            }

            // Update progress
            pb.inc(1);
            if case_idx % 100 == 0 {
                pb.set_message(format!("Found {} matches", matched_case_indices.len()));
            }
        }

        pb.finish_with_message("Matching complete");

        Ok(())
    }

    /// Filter a RecordBatch by row indices
    fn filter_batch_by_indices(
        &self,
        batch: &RecordBatch,
        indices: &[usize],
    ) -> Result<RecordBatch> {
        // Create a boolean mask for the selected rows
        let mut mask = vec![false; batch.num_rows()];
        for &idx in indices {
            if idx < mask.len() {
                mask[idx] = true;
            } else {
                return Err(ParquetReaderError::ValidationError(format!(
                    "Index out of bounds: {} >= {}",
                    idx,
                    mask.len()
                ))
                .into());
            }
        }

        let bool_array = BooleanArray::from(mask);

        // Apply the mask to all columns
        let filtered_columns: Result<Vec<ArrayRef>> = batch
            .columns()
            .iter()
            .map(|col| {
                compute::filter(col, &bool_array).map_err(|e| {
                    ParquetReaderError::ValidationError(format!("Failed to filter column: {e}"))
                        .into()
                })
            })
            .collect();

        // Create the filtered RecordBatch
        RecordBatch::try_new(batch.schema(), filtered_columns?).map_err(|e| {
            ParquetReaderError::ValidationError(format!("Failed to create filtered batch: {e}"))
                .into()
        })
    }
}

/// Structure to hold extracted attributes with indices
#[derive(Debug, Clone)]
struct ExtractedAttributes {
    /// Personal identification numbers
    pnrs: Vec<String>,

    /// Birth dates
    birth_dates: Vec<NaiveDate>,

    /// Genders (optional)
    genders: Vec<Option<String>>,

    /// Family sizes (optional)
    family_sizes: Vec<Option<i32>>,

    /// Record batch indices
    indices: Vec<usize>,
}

impl ExtractedAttributes {
    /// Check if the attributes are empty
    fn is_empty(&self) -> bool {
        self.pnrs.is_empty()
    }
}

/// Helper function to convert an Arrow array element to a date
fn arrow_array_to_date(array: &ArrayRef, index: usize) -> Option<NaiveDate> {
    use arrow::array::{Date32Array, Date64Array, StringArray};
    use arrow::datatypes::DataType;

    if array.is_null(index) {
        return None;
    }

    match array.data_type() {
        DataType::Date32 => {
            let date_array = array.as_any().downcast_ref::<Date32Array>().unwrap();
            date_array.value_as_date(index)
        }
        DataType::Date64 => {
            let date_array = array.as_any().downcast_ref::<Date64Array>().unwrap();
            date_array.value_as_date(index)
        }
        DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            let date_str = string_array.value(index);

            // Try different date formats
            for format in &["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"] {
                if let Ok(date) = NaiveDate::parse_from_str(date_str, format) {
                    return Some(date);
                }
            }

            None
        }
        _ => None,
    }
}

/// Helper function to convert an Arrow array element to a string
fn arrow_array_to_string(array: &ArrayRef, index: usize) -> Option<String> {
    use arrow::array::StringArray;

    if array.is_null(index) {
        return None;
    }

    match array.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Some(string_array.value(index).to_string())
        }
        _ => None,
    }
}

/// Helper function to convert an Arrow array element to an i32
fn arrow_array_to_i32(array: &ArrayRef, index: usize) -> Option<i32> {
    use arrow::array::{Float32Array, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::DataType;

    if array.is_null(index) {
        return None;
    }

    match array.data_type() {
        DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Some(int_array.value(index))
        }
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Some(int_array.value(index) as i32)
        }
        DataType::Float32 => {
            let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Some(float_array.value(index) as i32)
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Some(float_array.value(index) as i32)
        }
        _ => None,
    }
}
