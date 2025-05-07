//! Filtering capabilities for Parquet files and domain entities
//!
//! This module provides a flexible expression-based filtering system
//! for Parquet files and domain entities, allowing you to filter data based on various criteria.

// Core modules
pub mod core;
pub mod expr;
pub mod pnr;
pub mod date;
pub mod error;

// Generic filtering framework
pub mod generic;
pub mod adapter;

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
    BatchFilter, filter_record_batch, read_parquet_with_filter,
    AndFilter, OrFilter, NotFilter, IncludeAllFilter, ExcludeAllFilter,
};

pub use self::expr::{
    Expr, LiteralValue, ExpressionFilter,
    eq_filter, in_filter, create_pnr_filter,
};

pub use self::pnr::{
    PnrFilter, FilterPlan, build_filter_plan, apply_filter_plan,
    join_and_filter_by_pnr,
};

pub use self::date::{
    DateRangeFilter, filter_by_year, add_year_column,
};

// Re-export error handling utilities
pub use self::error::{
    FilterResultExt, filter_err, with_filter_context, filter_path_err,
    column_not_found, column_type_error, invalid_expr,
};

// Re-export generic filtering framework
pub use self::generic::{
    Filter, IncludeAllFilter as GenericIncludeAllFilter, 
    ExcludeAllFilter as GenericExcludeAllFilter,
    AndFilter as GenericAndFilter, OrFilter as GenericOrFilter,
    NotFilter as GenericNotFilter, FilterAdapter, FilterExpressionBuilder,
    FilterBuilder, FilterExt, BoxedFilter,
};

// Re-export adapter implementations
pub use self::adapter::{
    BatchFilterAdapter, EntityFilterAdapter, EntityToBatchAdapter,
    IndividualFilter, FamilyFilter,
};