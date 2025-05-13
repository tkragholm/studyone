//! Filtering capabilities for Parquet files and domain entities
//!
//! This module provides a flexible expression-based filtering system
//! for Parquet files and domain entities, allowing you to filter data based on various criteria.

// Core modules
pub mod core;
pub mod date;
pub mod error;
pub mod expr;
pub mod pnr;

// Generic filtering framework
pub mod adapter;
pub mod generic;

// Async filtering
pub mod async_filtering {
    //! Async filtering functionality
    //!
    //! This module re-exports the async filtering capabilities from the async module
    pub use super::async_rs::*;
}

// Import async module but name it differently to avoid conflict with the keyword
mod async_rs {
    pub use super::r#async::*;
}

// The actual async module
mod r#async;

// Re-export the most commonly used types and functions
pub use self::core::{
    AndFilter, BatchFilter, ExcludeAllFilter, IncludeAllFilter, NotFilter, OrFilter,
    filter_record_batch, read_parquet_with_filter,
};

pub use self::expr::{
    Expr, ExpressionFilter, LiteralValue, create_pnr_filter, eq_filter, in_filter,
};

pub use self::pnr::{
    FilterPlan, PnrFilter, apply_filter_plan, build_filter_plan, join_and_filter_by_pnr,
};

pub use self::date::{DateRangeFilter, add_year_column, filter_by_year};

// Re-export error handling utilities
pub use self::error::{
    FilterResultExt, column_not_found, column_type_error, filter_err, filter_path_err,
    invalid_expr, with_filter_context,
};

// Re-export generic filtering framework
pub use self::generic::{
    AndFilter as GenericAndFilter, BoxedFilter, ExcludeAllFilter as GenericExcludeAllFilter,
    Filter, FilterAdapter, FilterBuilder, FilterExpressionBuilder, FilterExt,
    IncludeAllFilter as GenericIncludeAllFilter, NotFilter as GenericNotFilter,
    OrFilter as GenericOrFilter,
};

// Re-export adapter implementations
pub use self::adapter::{
    BatchFilterAdapter, EntityFilterAdapter, EntityToBatchAdapter, IndividualFilter,
};
