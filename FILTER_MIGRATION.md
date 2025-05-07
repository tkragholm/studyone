# Migrating to the Generic Filter Framework

This document describes the migration strategy for moving from the legacy filtering systems to the new unified generic filter framework.

## Motivation

The codebase currently has multiple independent filtering approaches:

1. **Arrow-based batch filtering** (`src/filter/core.rs`) - For filtering record batches
2. **Object-based domain filtering** (`src/algorithm/population/filters.rs`) - For filtering domain entities
3. **Configuration-based matching criteria** (`src/algorithm/matching/criteria.rs`) - For defining match conditions

This creates inconsistency and results in duplicated code across the system. The generic filter framework provides a unified approach with common patterns.

## Benefits of the New System

- **Consistent pattern** across all types of filtering
- **Composable filters** with common combinators (AND, OR, NOT)
- **Type safety** with compile-time type checking
- **Resource tracking** for required fields/columns
- **Extension methods** for fluent filter composition
- **Comprehensive error handling** with specific error messages
- **Adapter pattern** for compatibility with existing code
- **Builder patterns** for complex filter construction

## Migration Guide

### 1. Implementing the Filter Trait

To implement a filter, create a new struct and implement the `Filter<T>` trait:

```rust
use crate::filter::generic::Filter;
use crate::error::{ParquetReaderError, Result};
use std::collections::HashSet;

#[derive(Debug, Clone)]
struct MyFilter {
    // Filter parameters
    parameter: String,
}

impl Filter<MyType> for MyFilter {
    fn apply(&self, input: &MyType) -> Result<MyType> {
        if /* condition */ {
            Ok(input.clone())
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: format!("Excluded by MyFilter: {}", self.parameter),
            }.into())
        }
    }
    
    fn required_resources(&self) -> HashSet<String> {
        let mut resources = HashSet::new();
        resources.insert("field_name".to_string());
        resources
    }
}
```

### 2. Converting Legacy Filters

For legacy `FilterCriteria` implementations, use the `LegacyFilterAdapter`:

```rust
use crate::filter::generic::{Filter, FilterExt};

// Create a legacy filter
let legacy_filter = LegacyIndividualFilter::AgeRange {
    min_age: Some(18),
    max_age: Some(65),
    reference_date: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
};

// Adapt it to the new system
let adapted_filter = LegacyFilterAdapter::new(legacy_filter);

// Now it can be used with the new framework
let result = adapted_filter.apply(&individual)?;
```

### 3. Converting BatchFilters

To use existing `BatchFilter` implementations with the new system:

```rust
use crate::filter::adapter::BatchFilterAdapter;

// Create a batch filter
let batch_filter = MyExistingBatchFilter::new();

// Adapt it to the new system
let adapted_filter = BatchFilterAdapter::new(batch_filter);

// Now it can be used with the new framework
let result = adapted_filter.apply(&batch)?;
```

### 4. Composing Filters

With the new system, filters can be composed in multiple ways:

```rust
// Using filter combinators directly
let and_filter = AndFilter::new(vec![filter1, filter2]);
let or_filter = OrFilter::new(vec![filter1, filter2]);
let not_filter = NotFilter::new(filter1);

// Using the builder pattern
let complex_filter = FilterBuilder::new()
    .add_filter(filter1)
    .add_filter(filter2)
    .build_and();

// Using extension methods
let chained_filter = filter1.and(filter2).or(filter3).not();
```

### 5. Gradual Migration Strategy

To minimize risk, follow this gradual approach:

1. **Create New Implementations** alongside existing ones
2. **Use Adapters** for compatibility during transition
3. **Migrate Call Sites** one at a time to use the new APIs
4. **Remove Legacy Code** once all usage has been migrated

```rust
// Example of mixed approach during migration
let legacy_filter = LegacyIndividualFilter::Gender(Gender::Female);
let legacy_adapter = LegacyFilterAdapter::new(legacy_filter);

// New filter implementation
let age_filter = AgeRangeFilter::new(18, 65);

// Combine using new framework
let combined_filter = legacy_adapter.and(age_filter);
```

## Examples

See the following files for comprehensive examples:

- `src/examples/filter_example.rs` - Basic usage examples
- `src/examples/filter_migration_example.rs` - Migration examples

## Common Filter Patterns

Here are some common patterns to use when implementing new filters:

### Domain Entity Filters

```rust
#[derive(Debug, Clone)]
struct AgeRangeFilter {
    min_age: u32,
    max_age: u32,
}

impl Filter<Individual> for AgeRangeFilter {
    fn apply(&self, input: &Individual) -> Result<Individual> {
        if let Some(age) = input.age {
            if age >= self.min_age && age <= self.max_age {
                Ok(input.clone())
            } else {
                Err(ParquetReaderError::FilterExcluded {
                    message: format!("Age {} outside range {}-{}", age, self.min_age, self.max_age),
                }.into())
            }
        } else {
            Err(ParquetReaderError::FilterExcluded {
                message: "No age available".to_string(),
            }.into())
        }
    }
    
    fn required_resources(&self) -> HashSet<String> {
        let mut resources = HashSet::new();
        resources.insert("age".to_string());
        resources
    }
}
```

### Record Batch Filters

```rust
#[derive(Debug, Clone)]
struct ColumnValueFilter {
    column_name: String,
    value: String,
}

impl Filter<RecordBatch> for ColumnValueFilter {
    fn apply(&self, input: &RecordBatch) -> Result<RecordBatch> {
        // Find the column
        let col_idx = input.schema()
            .column_with_name(&self.column_name)
            .ok_or_else(|| anyhow::anyhow!("Column not found: {}", self.column_name))?
            .0;
            
        let column = input.column(col_idx);
        
        // Create a mask for matching values
        // ... implementation details ...
        
        // Apply the mask
        filter_record_batch(input, &mask)
    }
    
    fn required_resources(&self) -> HashSet<String> {
        let mut resources = HashSet::new();
        resources.insert(self.column_name.clone());
        resources
    }
}
```

## Tips for Effective Filtering

1. **Be Specific** - Define clear and focused filters for readability
2. **Resource Tracking** - Accurately declare all required fields/columns
3. **Error Messages** - Provide descriptive error messages for debugging
4. **Reuse** - Compose small filters into complex ones using combinators
5. **Consistency** - Follow common patterns across filter implementations
6. **Documentation** - Document filter behavior and expected input/output