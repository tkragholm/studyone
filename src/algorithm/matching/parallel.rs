//! Parallel matching implementation
//!
//! This module implements the parallel processing version of the matching algorithm
//! for large datasets, using Rayon for parallel processing.

use crate::algorithm::matching::case_group::CaseGroup;
use crate::algorithm::matching::control_data::ControlData;
use crate::algorithm::matching::criteria::MatchingConfig;
use crate::algorithm::matching::extraction::group_cases_by_birth_day_range;
use crate::algorithm::matching::types::ExtractedAttributes;
use crate::error::Result;
use crate::utils::progress;
use chrono::Datelike;
use indicatif::MultiProgress;
use log::info;
use rand::prelude::*;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use smallvec::SmallVec;
use std::sync::{Arc, Mutex};

/// Perform parallel matching for large datasets
pub fn perform_parallel_matching(
    case_attributes: ExtractedAttributes,
    control_data: &ControlData,
    matched_case_indices: &mut Vec<usize>,
    matched_control_indices: &mut Vec<usize>,
    config: &MatchingConfig,
) -> Result<()> {
    // Set up progress reporting
    let (mp, main_pb) = progress::create_multi_progress(
        case_attributes.pnrs.len() as u64,
        Some("Matching cases with controls"),
    );

    // Determine the number of threads for optimal parallelism
    let num_threads = rayon::current_num_threads();
    info!("Using parallel processing with {num_threads} threads");

    // Group cases by birth day range for better parallelism
    let case_groups = group_cases_by_birth_day_range(case_attributes, num_threads);
    info!("Grouped cases into {} birth day ranges", case_groups.len());

    // Create shared collections for results
    let matched_cases = Arc::new(Mutex::new(Vec::with_capacity(
        case_groups.iter().map(|g| g.pnrs.len()).sum(),
    )));
    let matched_controls = Arc::new(Mutex::new(Vec::with_capacity(
        case_groups.iter().map(|g| g.pnrs.len()).sum::<usize>() * config.matching_ratio,
    )));
    let used_control_indices = Arc::new(Mutex::new(FxHashSet::default()));

    // Set up random number generator
    let rng_seed = config.random_seed;

    // Process each group in parallel
    let results: Vec<_> = case_groups
        .par_iter()
        .map(|group| {
            process_case_group(
                group,
                control_data,
                config,
                rng_seed,
                &mp,
                &used_control_indices,
            )
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

    progress::finish_progress_bar(&main_pb, Some("Matching complete"));

    Ok(())
}

/// Process a single case group in parallel
#[allow(clippy::too_many_arguments)]
fn process_case_group(
    group: &CaseGroup,
    control_data: &ControlData,
    config: &MatchingConfig,
    rng_seed: Option<u64>,
    mp: &MultiProgress,
    used_control_indices: &Arc<Mutex<FxHashSet<usize>>>,
) -> (Vec<usize>, Vec<usize>) {
    let group_size = group.pnrs.len();
    let group_pb = progress::add_group_progress_bar(
        mp,
        group_size as u64,
        Some(&format!(
            "Range: {} to {}",
            group.birth_day_range.0, group.birth_day_range.1
        )),
    );

    let mut local_matched_cases = Vec::with_capacity(group_size);
    let mut local_matched_controls = Vec::with_capacity(group_size * config.matching_ratio);

    // Create a thread-local RNG with the provided seed or from system entropy
    let mut thread_rng = match rng_seed {
        Some(seed) => {
            // Create a unique seed for each thread from the base seed
            let thread_seed = seed.wrapping_add(group.birth_day_range.0 as u64);
            StdRng::seed_from_u64(thread_seed)
        }
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
        let (start_idx, end_idx) = control_data
            .find_birth_day_range(case_birth_day, config.criteria.birth_date_window_days);

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
                if config.criteria.require_same_gender {
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
                if config.criteria.match_family_size {
                    if let Some(case_size) = case_family_size {
                        if let Some(control_size) = &control_data.family_sizes[ctrl_idx] {
                            let diff = (case_size - control_size).abs();
                            if diff > config.criteria.family_size_tolerance {
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
        }

        // Select up to matching_ratio controls randomly
        let num_to_select = std::cmp::min(config.matching_ratio, eligible_control_indices.len());
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

        if i % 100 == 0 {
            group_pb.set_message(format!("Found {} matches", local_matched_cases.len()));
        }
    }

    progress::finish_and_clear(&group_pb);

    // Return local results
    (local_matched_cases, local_matched_controls)
}
