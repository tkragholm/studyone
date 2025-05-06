use crate::error::{IdsError, Result};
use crate::model::pnr::Pnr;
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use arrow::compute;
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, Duration, NaiveDate};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::info;
use rand::rng;
use rand::seq::{IndexedRandom, SliceRandom};
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use std::cmp::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Criteria for matching cases to controls
#[derive(Debug, Clone)]
pub struct MatchingCriteria {
    /// Maximum difference in days between birth dates
    pub birth_date_window_days: i32,

    /// Maximum difference in days between parent birth dates
    pub parent_birth_date_window_days: i32,

    /// Whether both parents are required
    pub require_both_parents: bool,

    /// Whether the same gender is required
    pub require_same_gender: bool,
}

impl Default for MatchingCriteria {
    fn default() -> Self {
        Self {
            birth_date_window_days: 30,
            parent_birth_date_window_days: 365,
            require_both_parents: false,
            require_same_gender: true,
        }
    }
}

/// Pair of matched case and control
#[derive(Debug, Clone)]
pub struct MatchedPair {
    /// Case PNR
    pub case_pnr: Pnr,

    /// Case birth date
    pub case_birth_date: NaiveDate,

    /// Control PNR
    pub control_pnr: Pnr,

    /// Control birth date
    pub control_birth_date: NaiveDate,

    /// Date when the match was made
    pub match_date: NaiveDate,
}

/// Optimized struct-of-arrays data structure for controls
/// This improves cache locality by storing each attribute in its own contiguous array
struct ControlData {
    /// Array of control PNRs
    pnrs: Vec<Pnr>,

    /// Array of birth dates stored as days since epoch for faster comparison
    birth_days: Vec<i32>,

    /// Original birth dates for output
    birth_dates: Vec<NaiveDate>,

    /// Record batch indices for the controls
    indices: Vec<usize>,
}

impl ControlData {
    /// Create a new `ControlData` from a vector of (Pnr, `NaiveDate`) pairs and their indices
    fn new(controls: Vec<(Pnr, NaiveDate, usize)>) -> Self {
        let capacity = controls.len();
        let mut pnrs = Vec::with_capacity(capacity);
        let mut birth_days = Vec::with_capacity(capacity);
        let mut birth_dates = Vec::with_capacity(capacity);
        let mut indices = Vec::with_capacity(capacity);

        for (pnr, date, idx) in controls {
            pnrs.push(pnr);
            birth_days.push(date.num_days_from_ce());
            birth_dates.push(date);
            indices.push(idx);
        }

        Self {
            pnrs,
            birth_days,
            birth_dates,
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
        let mut sorted_indices = Vec::with_capacity(self.indices.len());

        for &i in &idx_vec {
            sorted_pnrs.push(self.pnrs[i].clone());
            sorted_birth_days.push(self.birth_days[i]);
            sorted_birth_dates.push(self.birth_dates[i]);
            sorted_indices.push(self.indices[i]);
        }

        // Replace the original arrays
        self.pnrs = sorted_pnrs;
        self.birth_days = sorted_birth_days;
        self.birth_dates = sorted_birth_dates;
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
    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.pnrs.len()
    }

    /// Check if the control data is empty
    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.pnrs.is_empty()
    }
}

/// Case data grouped by birth day ranges
struct CaseGroup {
    /// Array of case PNRs
    pnrs: Vec<Pnr>,

    /// Array of birth dates
    birth_dates: Vec<NaiveDate>,

    /// Record batch indices for the cases
    indices: Vec<usize>,

    /// Birth day range (start, end)
    birth_day_range: (i32, i32),
}

/// Matcher for pairing cases with controls
pub struct Matcher {
    /// Matching criteria
    criteria: MatchingCriteria,
}

impl Matcher {
    // Constants for optimization
    #[allow(dead_code)]
    const BATCH_SIZE: usize = 1024;
    const PARALLEL_THRESHOLD: usize = 5000; // Threshold for switching to parallel processing

    /// Create a new matcher with the given criteria
    #[must_use]
    pub const fn new(criteria: MatchingCriteria) -> Self {
        Self { criteria }
    }

    /// Match cases to controls
    pub fn match_cases_to_controls(
        &self,
        cases: Vec<(Pnr, NaiveDate)>,
        controls: Vec<(Pnr, NaiveDate)>,
        match_date: NaiveDate,
    ) -> Result<Vec<MatchedPair>> {
        let mut matches = Vec::new();
        let mut available_controls = controls;

        for (case_pnr, case_birth_date) in cases {
            // Find eligible controls
            let eligible_indices =
                self.find_eligible_controls(&case_pnr, case_birth_date, &available_controls)?;

            if eligible_indices.is_empty() {
                return Err(IdsError::Validation(format!(
                    "No eligible controls found for case {}",
                    case_pnr.value()
                )));
            }

            // Select a random control
            let mut rng = rng();
            let selected_idx = *eligible_indices.choose(&mut rng).unwrap();
            let (control_pnr, control_birth_date) = available_controls.remove(selected_idx);

            // Create matched pair
            matches.push(MatchedPair {
                case_pnr,
                case_birth_date,
                control_pnr,
                control_birth_date,
                match_date,
            });
        }

        Ok(matches)
    }

    /// Build optimized birth date index for faster matching
    #[allow(dead_code)]
    fn build_birth_date_index(
        &self,
        controls: &[(Pnr, NaiveDate)],
    ) -> FxHashMap<i32, SmallVec<[usize; 16]>> {
        let mut index = FxHashMap::default();
        let window_days = self.criteria.birth_date_window_days;

        for (idx, (_, birth_date)) in controls.iter().enumerate() {
            // Create buckets of birth dates based on days from CE
            let days_from_ce = birth_date.num_days_from_ce();
            let bucket = days_from_ce / window_days;

            // Store the control index in the appropriate bucket
            index
                .entry(bucket)
                .or_insert_with(SmallVec::<[usize; 16]>::new)
                .push(idx);
        }

        index
    }

    /// Find eligible controls for a case
    fn find_eligible_controls(
        &self,
        case_pnr: &Pnr,
        case_birth_date: NaiveDate,
        controls: &[(Pnr, NaiveDate)],
    ) -> Result<Vec<usize>> {
        let mut eligible_indices = Vec::new();
        let _window = Duration::days(i64::from(self.criteria.birth_date_window_days));

        for (idx, (control_pnr, control_birth_date)) in controls.iter().enumerate() {
            // Skip if case and control are the same person
            if case_pnr.value() == control_pnr.value() {
                continue;
            }

            // Check birth date window
            let diff = (*control_birth_date - case_birth_date).num_days().abs() as i32;
            if diff > self.criteria.birth_date_window_days {
                continue;
            }

            // Additional criteria checks would go here
            // (gender, parents, etc. - simplified for this example)

            eligible_indices.push(idx);
        }

        Ok(eligible_indices)
    }

    /// Group cases by birth day ranges for parallel processing
    fn group_cases_by_birth_day_range(
        &self,
        case_pairs: &[(Pnr, NaiveDate, usize)],
        num_groups: usize,
    ) -> Vec<CaseGroup> {
        if case_pairs.is_empty() || num_groups == 0 {
            return Vec::new();
        }

        // Sort cases by birth_day
        let mut sorted_cases = case_pairs.to_vec();
        sorted_cases.sort_by_key(|(_, date, _)| date.num_days_from_ce());

        // Find min and max birth days
        let min_birth_day = sorted_cases[0].1.num_days_from_ce();
        let max_birth_day = sorted_cases[sorted_cases.len() - 1].1.num_days_from_ce();

        // Calculate range size
        let total_range = max_birth_day - min_birth_day + 1;
        let group_range_size = total_range / num_groups as i32 + 1;

        let mut groups = Vec::with_capacity(num_groups);
        let mut current_start = min_birth_day;

        for _ in 0..num_groups {
            let current_end = std::cmp::min(current_start + group_range_size, max_birth_day + 1);

            let group_cases: Vec<(Pnr, NaiveDate, usize)> = sorted_cases
                .iter()
                .filter(|(_, date, _)| {
                    let days = date.num_days_from_ce();
                    days >= current_start && days < current_end
                })
                .cloned()
                .collect();

            if !group_cases.is_empty() {
                let mut pnrs = Vec::with_capacity(group_cases.len());
                let mut birth_dates = Vec::with_capacity(group_cases.len());
                let mut indices = Vec::with_capacity(group_cases.len());

                for (pnr, date, idx) in group_cases {
                    pnrs.push(pnr);
                    birth_dates.push(date);
                    indices.push(idx);
                }

                groups.push(CaseGroup {
                    pnrs,
                    birth_dates,
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

    /// Perform optimized matching between cases and controls
    pub fn perform_matching(
        &self,
        cases: &RecordBatch,
        controls: &RecordBatch,
        matching_ratio: usize,
    ) -> Result<(RecordBatch, RecordBatch)> {
        let start_time = Instant::now();

        // Extract PNR and birth date pairs with indices
        let case_pairs_with_indices = self.extract_pnr_and_birth_date_with_indices(cases)?;
        let control_pairs_with_indices = self.extract_pnr_and_birth_date_with_indices(controls)?;

        info!(
            "Matching {} cases with control pool of {} candidates",
            case_pairs_with_indices.len(),
            control_pairs_with_indices.len()
        );

        // Create optimized control data structure
        let mut control_data = ControlData::new(control_pairs_with_indices);

        // Sort controls by birth day for binary search
        control_data.sort_by_birth_day();

        // Track results
        let mut matched_case_indices = Vec::with_capacity(case_pairs_with_indices.len());
        let mut matched_control_indices =
            Vec::with_capacity(case_pairs_with_indices.len() * matching_ratio);

        // Decide whether to use parallel or sequential processing
        if case_pairs_with_indices.len() >= Self::PARALLEL_THRESHOLD {
            // Use parallel processing
            let mp = MultiProgress::new();
            let main_pb = mp.add(ProgressBar::new(case_pairs_with_indices.len() as u64));
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
            let case_groups =
                self.group_cases_by_birth_day_range(&case_pairs_with_indices, num_threads);
            info!("Grouped cases into {} birth day ranges", case_groups.len());

            // Create shared collections for results
            let matched_cases = Arc::new(Mutex::new(Vec::with_capacity(
                case_pairs_with_indices.len(),
            )));
            let matched_controls = Arc::new(Mutex::new(Vec::with_capacity(
                case_pairs_with_indices.len() * matching_ratio,
            )));
            let used_control_indices = Arc::new(Mutex::new(FxHashSet::default()));

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
                    let mut local_matched_controls = Vec::with_capacity(group_size * matching_ratio);

                    for i in 0..group_size {
                        let case_pnr = &group.pnrs[i];
                        let case_birth_date = group.birth_dates[i];
                        let case_idx = group.indices[i];
                        let case_birth_day = case_birth_date.num_days_from_ce();

                        // Find range of potentially eligible controls using binary search
                        let (start_idx, end_idx) = control_data.find_birth_day_range(
                            case_birth_day,
                            self.criteria.birth_date_window_days
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
                                if case_pnr.value() == control_pnr.value() {
                                    continue;
                                }

                                // Birth dates are already known to be within range (from binary search)
                                eligible_control_indices.push(ctrl_idx);
                            }
                        }

                        // Select up to matching_ratio controls randomly
                        let num_to_select = std::cmp::min(matching_ratio, eligible_control_indices.len());
                        if num_to_select > 0 {
                            local_matched_cases.push(case_idx);

                            // Randomly select controls
                            let mut rng = rng();
                            let mut indices_vec: Vec<usize> = eligible_control_indices.into_iter().collect();
                            indices_vec.partial_shuffle(&mut rng, num_to_select);

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
                            main_pb.set_message(format!("Found {} matches", local_matched_cases.len()));
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
            matched_case_indices = matched_cases.lock().unwrap().clone();
            matched_control_indices = matched_controls.lock().unwrap().clone();

            main_pb.finish_with_message("Matching complete");
        } else {
            // Use sequential processing for smaller datasets
            info!(
                "Using sequential processing for {} cases",
                case_pairs_with_indices.len()
            );

            // Set up progress bar
            let pb = ProgressBar::new(case_pairs_with_indices.len() as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} cases ({per_sec}) {msg}")
                    .unwrap()
                    .progress_chars("#>-"),
            );

            // Track which controls have been used
            let mut used_control_indices = FxHashSet::default();

            // Process each case
            for (case_idx, (case_pnr, case_birth_date, case_batch_idx)) in
                case_pairs_with_indices.iter().enumerate()
            {
                let case_birth_day = case_birth_date.num_days_from_ce();

                // Find range of potentially eligible controls using binary search
                let (start_idx, end_idx) = control_data
                    .find_birth_day_range(case_birth_day, self.criteria.birth_date_window_days);

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
                    if case_pnr.value() == control_pnr.value() {
                        continue;
                    }

                    // Birth dates are already known to be within range (from binary search)
                    eligible_control_indices.push(ctrl_idx);
                }

                // Select up to matching_ratio controls randomly
                let num_to_select = std::cmp::min(matching_ratio, eligible_control_indices.len());
                if num_to_select > 0 {
                    matched_case_indices.push(*case_batch_idx);

                    // Randomly select controls
                    let mut rng = rng();
                    let mut indices_vec: Vec<usize> =
                        eligible_control_indices.into_iter().collect();
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
        }

        if matched_case_indices.is_empty() {
            return Err(IdsError::Validation(
                "No matches found for any cases".to_string(),
            ));
        }

        // Create filtered RecordBatches using batch filtering
        let case_batch = self.filter_batch_by_indices(cases, &matched_case_indices)?;
        let control_batch = self.filter_batch_by_indices(controls, &matched_control_indices)?;

        let elapsed = start_time.elapsed();
        info!(
            "Matching complete: {} cases matched with {} controls in {:.2?} ({:.2} cases/sec)",
            case_batch.num_rows(),
            control_batch.num_rows(),
            elapsed,
            case_pairs_with_indices.len() as f64 / elapsed.as_secs_f64()
        );

        Ok((case_batch, control_batch))
    }

    /// Extract PNR and birth date pairs with record batch indices from a `RecordBatch`
    fn extract_pnr_and_birth_date_with_indices(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<(Pnr, NaiveDate, usize)>> {
        let pnr_idx = batch
            .schema()
            .index_of("PNR")
            .map_err(|e| IdsError::Data(format!("PNR column not found: {e}")))?;

        let birth_date_idx = batch
            .schema()
            .index_of("FOED_DAG")
            .map_err(|e| IdsError::Data(format!("FOED_DAG column not found: {e}")))?;

        let pnr_col = batch.column(pnr_idx);
        let birth_date_col = batch.column(birth_date_idx);

        let pnr_array = pnr_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IdsError::Data("PNR column is not a string array".to_string()))?;

        let mut pairs = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            if pnr_array.is_null(i) {
                continue;
            }

            let pnr_str = pnr_array.value(i);
            let pnr = Pnr::from(pnr_str);

            if let Some(date) =
                crate::utils::date_utils::extract_date_from_array(birth_date_col.as_ref(), i)
            {
                pairs.push((pnr, date, i));
            }
        }

        Ok(pairs)
    }

    /// Extract PNR and birth date pairs from a `RecordBatch`
    #[allow(dead_code)]
    fn extract_pnr_and_birth_date(&self, batch: &RecordBatch) -> Result<Vec<(Pnr, NaiveDate)>> {
        let pnr_idx = batch
            .schema()
            .index_of("PNR")
            .map_err(|e| IdsError::Data(format!("PNR column not found: {e}")))?;

        let birth_date_idx = batch
            .schema()
            .index_of("FOED_DAG")
            .map_err(|e| IdsError::Data(format!("FOED_DAG column not found: {e}")))?;

        let pnr_col = batch.column(pnr_idx);
        let birth_date_col = batch.column(birth_date_idx);

        let pnr_array = pnr_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IdsError::Data("PNR column is not a string array".to_string()))?;

        let mut pairs = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            if pnr_array.is_null(i) {
                continue;
            }

            let pnr_str = pnr_array.value(i);
            let pnr = Pnr::from(pnr_str);

            if let Some(date) =
                crate::utils::date_utils::extract_date_from_array(birth_date_col.as_ref(), i)
            {
                pairs.push((pnr, date));
            }
        }

        Ok(pairs)
    }

    /// Build a map from PNR to row index for fast lookups
    #[allow(dead_code)]
    fn build_pnr_index(&self, batch: &RecordBatch) -> Result<FxHashMap<String, usize>> {
        let pnr_idx = batch
            .schema()
            .index_of("PNR")
            .map_err(|e| IdsError::Data(format!("PNR column not found: {e}")))?;

        let pnr_col = batch.column(pnr_idx);
        let pnr_array = pnr_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IdsError::Data("PNR column is not a string array".to_string()))?;

        let mut pnr_to_idx = FxHashMap::default();
        pnr_to_idx.reserve(batch.num_rows());

        for i in 0..pnr_array.len() {
            if !pnr_array.is_null(i) {
                pnr_to_idx.insert(pnr_array.value(i).to_string(), i);
            }
        }

        Ok(pnr_to_idx)
    }

    /// Filter a `RecordBatch` by row indices
    fn filter_batch_by_indices(
        &self,
        batch: &RecordBatch,
        indices: &[usize],
    ) -> Result<RecordBatch> {
        // Create a boolean mask for the selected rows
        let mut mask = vec![false; batch.num_rows()];
        for &idx in indices {
            mask[idx] = true;
        }

        let bool_array = BooleanArray::from(mask);

        // Apply the mask to all columns
        let filtered_columns: Result<Vec<ArrayRef>> = batch
            .columns()
            .iter()
            .map(|col| {
                compute::filter(col, &bool_array)
                    .map_err(|e| IdsError::Data(format!("Failed to filter column: {e}")))
            })
            .collect();

        // Create the filtered RecordBatch
        RecordBatch::try_new(batch.schema(), filtered_columns?)
            .map_err(|e| IdsError::Data(format!("Failed to create filtered batch: {e}")))
    }
}
