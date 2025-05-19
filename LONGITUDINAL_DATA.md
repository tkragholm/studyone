# Longitudinal Data Handling

This document describes the implementation of longitudinal data handling in the codebase, focusing on how data from different time periods is managed.

## Overview

Many Danish registers deliver data in a longitudinal format with data files corresponding to specific time periods (years, months, quarters). Our implementation ensures that:

1. Time period information is extracted from filenames
2. Time periods are associated with individual data
3. Data from multiple time periods can be loaded and merged

## Key Components

### 1. Time Period Representation

The `TimePeriod` enum in `src/models/core/individual/temporal.rs` represents different granularities of time periods:

```rust
pub enum TimePeriod {
    Year(i32),                // e.g., 2020
    Month(i32, u32),         // e.g., 2020-01
    Quarter(i32, u32),       // e.g., 2020-Q1
    Day(NaiveDate),          // e.g., 2020-01-01
}
```

This flexible representation allows working with various registry file naming conventions.

### 2. Individual Model Extensions

The `Individual` model has been enhanced to track time period information:

```rust
pub struct Individual {
    // ... other fields ...
    
    // Map of registry name -> time periods -> data source
    pub time_periods: HashMap<String, BTreeMap<TimePeriod, String>>,
    
    // Currently active time period (for context)
    pub current_time_period: Option<(String, TimePeriod)>,
}
```

These fields allow tracking which time periods data comes from and provide methods for querying and filtering data based on specific periods.

### 3. Temporal Registry Loader

The `TemporalRegistryLoader` in `src/registry/temporal_registry_loader.rs` wraps regular registry loaders to add time period awareness:

```rust
pub struct TemporalRegistryLoader {
    registry_name: &'static str,
    inner_loader: Arc<dyn RegisterLoader>,
    pnr_column: Option<&'static str>,
}
```

It provides methods for:
- Loading data from specific time periods
- Loading data from multiple time periods in parallel
- Discovering available time periods in a registry

### 4. Longitudinal Utilities

The new `src/utils/register/longitudinal.rs` module provides high-level functions for working with longitudinal data:

- `detect_registry_time_periods`: Discovers available registries and their time periods
- `load_longitudinal_data`: Loads data with time period information
- `merge_temporal_individuals`: Merges individuals from different time periods

### 5. File Path Utilities

The `src/utils/io/paths/time_period.rs` module provides utilities for:
- Extracting time periods from filenames
- Finding files for specific time periods
- Working with date ranges

## File Naming Conventions

The implementation supports various file naming conventions:

1. **Yearly data**: `2020.parquet`, `year_2020.parquet`
2. **Monthly data**: `202001.parquet`, `2020-01.parquet`
3. **Quarterly data**: `2020Q1.parquet`, `2020-Q1.parquet`
4. **Daily data**: `20200101.parquet`

The pattern matching is extensible and can be customized for different registry file naming schemes.

## Using Longitudinal Data

### Basic Example

```rust
// 1. Configure the longitudinal data loading
let mut config = LongitudinalConfig::new("/path/to/data");
config.add_registry("bef", "bef");
config.add_registry("akm", "akm");

// 2. Set a date range if needed
let start_date = NaiveDate::from_ymd_opt(2018, 1, 1).unwrap();
let end_date = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();
config.with_date_range(start_date, end_date);

// 3. Create a registry loader
let registry_loader = factory::registry_from_name("bef")?;

// 4. Load longitudinal data
let temporal_data = load_longitudinal_data(&*registry_loader, &config)?;

// 5. Merge individuals from different time periods
let merged_individuals = merge_temporal_individuals(&temporal_data);
```

### Handling Different Registry Types

Different registries have different file naming conventions:

- **AKM Registry**: Uses yearly files (e.g., `2000.parquet`, `2001.parquet`)
- **BEF Registry**: Uses monthly files (e.g., `200012.parquet`, `201903.parquet`)
- **IND Registry**: Uses yearly files, similar to AKM

The implementation handles these differences automatically through pattern matching.

## Advanced Features

### Temporal Filtering

Individuals can be filtered based on time periods:

```rust
// Get individuals valid at a specific date
let reference_date = NaiveDate::from_ymd_opt(2020, 6, 15).unwrap();
let valid_individuals = individuals.iter()
    .filter(|ind| ind.was_valid_at(&reference_date))
    .collect::<Vec<_>>();
```

### Time Series Analysis

You can analyze how data changes over time:

```rust
// Find the earliest and latest time periods for an individual
if let Some(ind) = individuals.iter().find(|i| i.pnr == "1234567890") {
    for (registry, periods) in &ind.time_periods {
        if let (Some(first), Some(last)) = (periods.keys().next(), periods.keys().last()) {
            println!("Registry {}: data from {} to {}", 
                registry, first.to_string(), last.to_string());
        }
    }
}
```

### Merging Strategies

When merging data from different time periods, conflicts are resolved using the following strategy:

1. Process time periods in chronological order (earliest to latest)
2. For each individual, merge fields from newer time periods into existing records
3. Newer data overwrites older data only if the older field is None/empty

This ensures that the most complete and up-to-date information is preserved.

## Future Enhancements

1. **Snapshot Views**: Create views of the population at specific points in time
2. **Change Detection**: Track changes in fields over time for longitudinal analysis
3. **Time-Based Queries**: Enhanced query capabilities for time-based research questions
4. **Data Consistency Checks**: Validate consistency of data across time periods
5. **Incremental Loading**: Efficiently load only the most recent time periods

## Example Code

See the full example in `examples/longitudinal_data.rs` for a detailed demonstration of longitudinal data handling.

## Recent Enhancements

### 1. Improved Individual Creation with Time Period Information

The `Individual` model now has a new method for creating individuals with time period information:

```rust
pub fn from_batch_with_time_period(
    batch: &RecordBatch,
    file_path: &std::path::Path,
    registry_name: &str,
) -> Result<Vec<Self>>
```

This automatically extracts time period information from the file path and sets it on all individuals created from the batch.

### 2. Direct Loading with Time Period

New factory methods simplify loading registry data with time period information:

```rust
// Load registry data from a file with time period information
pub fn load_registry_with_time_period(
    file_path: &Path,
    registry_name: &str,
    pnr_filter: Option<&HashSet<String>>,
) -> Result<Vec<Individual>>

// Load data from all time periods for a registry
pub fn load_registry_time_periods(
    registry_dir: &Path,
    registry_name: &str,
    date_range: Option<(NaiveDate, NaiveDate)>,
    pnr_filter: Option<&HashSet<String>>,
) -> Result<BTreeMap<TimePeriod, Vec<Individual>>>
```

### 3. Enhanced Registry Deserialization

The `enhance_from_registry` method has been updated to automatically extract time period information from file paths:

```rust
pub fn enhance_from_registry(
    &mut self,
    batch: &RecordBatch,
    row: usize,
    registry_name: &str,
    time_period: Option<TimePeriod>,
    file_path: Option<&std::path::Path>, // New parameter
) -> Result<bool>
```

This allows working with both explicitly provided time periods and automatically extracted ones.

### 4. Full Direct Deserialization Support

We now have proper support for direct registry deserialization with time period information using the `DirectIndividualDeserializer`:

```rust
// Helper function to deserialize a row based on registry name
pub fn deserialize_row(
    registry_name: &str, 
    batch: &RecordBatch, 
    row: usize
) -> Result<Option<Individual>>
```

This integrates time period awareness with the existing direct deserialization system.