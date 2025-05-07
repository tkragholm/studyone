//! Sequential matching implementation
//!
//! This module implements the sequential (non-parallel) version of the matching algorithm
//! for smaller datasets.

use crate::algorithm::matching::control_data::ControlData;
use crate::algorithm::matching::criteria::MatchingConfig;
use crate::algorithm::matching::types::ExtractedAttributes;
use crate::error::Result;
use crate::utils::progress;
use chrono::Datelike;
use log::info;
use rand::prelude::*;
use rand::seq::SliceRandom;
use rustc_hash::FxHashSet;
use smallvec::SmallVec;

/// Perform sequential matching for smaller datasets
pub fn perform_sequential_matching(
    case_attributes: ExtractedAttributes,
    control_data: &ControlData,
    matched_case_indices: &mut Vec<usize>,
    matched_control_indices: &mut Vec<usize>,
    config: &MatchingConfig,
) -> Result<()> {
    info!(
        "Using sequential processing for {} cases",
        case_attributes.pnrs.len()
    );

    // Set up progress bar
    let pb = progress::create_main_progress_bar(
        case_attributes.pnrs.len() as u64,
        Some("Sequential matching"),
    );

    // Track which controls have been used
    let mut used_control_indices = FxHashSet::default();

    // Create RNG with optional seed
    let mut rng = match config.random_seed {
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
            .find_birth_day_range(case_birth_day, config.criteria.birth_date_window_days);

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

        // Select up to matching_ratio controls randomly
        let num_to_select = std::cmp::min(config.matching_ratio, eligible_control_indices.len());
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

    progress::finish_progress_bar(&pb, Some("Matching complete"));

    Ok(())
}
