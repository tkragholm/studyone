//! Optimized PNR filtering utilities - DEPRECATED
//!
//! This module is maintained for backward compatibility only.
//! Use the new filter module instead: `crate::filter::pnr`

use crate::RecordBatch;
use crate::Result;
use crate::filter::core::BatchFilter;
use std::collections::HashSet;

// Re-export from the new centralized module
pub use crate::filter::pnr::{
    FilterPlan, PnrFilter, apply_filter_plan, build_filter_plan, join_and_filter_by_pnr,
};

/// Filter a record batch by PNR values - DEPRECATED
///
/// # Arguments
/// * `batch` - The record batch to filter
/// * `pnr_column` - The name of the PNR column
/// * `pnr_filter` - The set of PNR values to filter by
///
/// # Returns
/// * `Result<RecordBatch>` - The filtered record batch
pub fn filter_batch_by_pnr<S: ::std::hash::BuildHasher>(
    batch: &RecordBatch,
    pnr_column: &str,
    pnr_filter: &HashSet<String, S>,
) -> Result<RecordBatch> {
    // Use the new implementation
    let pnr_filter_obj = PnrFilter::new(pnr_filter, Some(pnr_column.to_string()));
    pnr_filter_obj.filter(batch)
}

// The remainder of the file is just re-exports from the centralized filter module
// All these functions are implemented in the filter/pnr.rs file now
