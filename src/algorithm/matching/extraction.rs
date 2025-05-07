//! Data extraction utilities for the matching algorithm
//!
//! This module provides functions for extracting attributes from record batches
//! and preparing data for matching.

use crate::algorithm::matching::case_group::CaseGroup;
use crate::algorithm::matching::criteria::MatchingConfig;
use crate::algorithm::matching::types::ExtractedAttributes;
use crate::error::{ParquetReaderError, Result};
use crate::utils::arrow_utils;
use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use log::warn;

/// Extract attributes from a `RecordBatch` with indices
pub fn extract_attributes_with_indices(
    batch: &RecordBatch,
    config: &MatchingConfig,
) -> Result<ExtractedAttributes> {
    let pnr_idx = batch
        .schema()
        .index_of("PNR")
        .map_err(|_| ParquetReaderError::ValidationError("PNR column not found".to_string()))?;

    let birth_date_idx = batch.schema().index_of("FOED_DAG").map_err(|_| {
        ParquetReaderError::ValidationError("FOED_DAG column not found".to_string())
    })?;

    // Gender is optional based on criteria
    let gender_idx = if config.criteria.require_same_gender {
        batch.schema().index_of("KOEN").ok()
    } else {
        None
    };

    // Family size is optional based on criteria
    let family_size_idx = if config.criteria.match_family_size {
        if let Ok(idx) = batch.schema().index_of("ANTAL_BOERN") {
            Some(idx)
        } else {
            warn!("ANTAL_BOERN column not found but family size matching is enabled");
            None
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
        let birth_date = match arrow_utils::arrow_array_to_date(birth_date_col, i) {
            Some(date) => date,
            None => continue, // Skip if birth date is missing
        };

        // Extract gender if needed
        let gender = if let Some(col) = &gender_col {
            arrow_utils::arrow_array_to_string(col, i)
        } else {
            None
        };

        // Extract family size if needed
        let family_size = if let Some(col) = &family_size_col {
            arrow_utils::arrow_array_to_i32(col, i)
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

/// Group cases by birth day range for parallel processing
pub fn group_cases_by_birth_day_range(
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
