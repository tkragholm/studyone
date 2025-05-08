# Direct Registry-Model Conversion Migration Guide

This document describes how to migrate code from using the adapter pattern to the new direct registry-model conversion approach. The adapter pattern has been completely removed in favor of a more efficient and maintainable direct integration between registries and models.

## Overview of Changes

1. **Removed Components**:
   - The entire `src/models/adapters` directory and all adapter implementations
   - Adapter-specific code in examples and tests

2. **New Components**:
   - `ModelConversion` trait for registry types to directly convert to models
   - `ModelConversionExt` trait providing convenient loading methods
   - Schema-aware constructors in model types
   - Registry-specific model conversion implementations

## Migration Steps

### 1. Replace Adapter Construction and Usage

**Before**:
```rust
// Create adapter
let bef_adapter = BefAdapter::new();

// Use adapter
let individuals = bef_adapter.from_record_batch(&batch)?;
```

**After**:
```rust
// Use registry directly with ModelConversionExt trait
let bef_registry = BefRegister::new();
let individuals = bef_registry.load_as::<Individual>(base_path, None)?;

// Or convert single batch
let individuals = bef_registry.to_models(&batch)?;
```

### 2. Year-Configured Income Data

**Before**:
```rust
// Configure year for IND adapter
let ind_adapter = IndIncomeAdapter::new(2020, cpi_indices);
let incomes = ind_adapter.from_record_batch_with_year(&batch)?;
```

**After**:
```rust
// Use year-configured registry
let ind_registry = YearConfiguredIndRegister::new(2020)
    .with_cpi_indices(cpi_indices);
let incomes = ind_registry.load_as::<Income>(base_path, None)?;
```

### 3. Child Models with Individual Lookup

**Before**:
```rust
// Create adapter with individual lookup
let mfr_adapter = MfrChildAdapter::new(individual_map);
let children = mfr_adapter.process_batch(&batch)?;
```

**After**:
```rust
// Use child-specific registry
let mut mfr_registry = MfrChildRegister::new();
mfr_registry.set_individual_lookup(individual_map);
let children = mfr_registry.load_as::<Child>(base_path, None)?;
```

### 4. LPR Diagnosis Models

**Before**:
```rust
// Create adapter with PNR lookup
let lpr_adapter = LprDiagAdapter::new(pnr_lookup);
let diagnoses = lpr_adapter.from_record_batch(&batch)?;
```

**After**:
```rust
// Use LPR registry with direct conversion
let mut lpr_registry = LprDiagRegister::new();
lpr_registry.set_pnr_lookup(pnr_lookup);
let diagnoses = lpr_registry.load_as::<Diagnosis>(base_path, None)?;
```

### 5. Async Loading

**Before**:
```rust
// Load data then use adapter
let batches = registry.load_async(base_path, pnr_filter).await?;
let mut models = Vec::new();
for batch in batches {
    models.extend(adapter.from_record_batch(&batch)?);
}
```

**After**:
```rust
// Use async loading method directly
let models = registry.load_as_async::<ModelType>(base_path, pnr_filter).await?;
```

## Benefits of the New Approach

1. **Simplified Code**: No need for separate adapter classes
2. **Direct Integration**: Models understand their source schema
3. **Consistent Interface**: Common pattern across all registry types
4. **Better Performance**: Fewer transformations and data copying
5. **Type Safety**: Stronger compile-time type checking

## Common Issues During Migration

1. **Missing Individual Lookups**: Some conversions (like MFR → Child) require individual lookups. Make sure these are set before attempting conversion.

2. **Year Configuration**: The IND registry requires year configuration for proper income data interpretation. Use `YearConfiguredIndRegister` instead of plain `IndRegister` for income-related data.

3. **PNR Lookups**: LPR registries require PNR lookups for Diagnosis conversion. Ensure these are set with `set_pnr_lookup()` before conversion.

## Complete Example: Loading Individuals from BEF

```rust
use par_reader::registry::{BefRegister, ModelConversionExt};
use par_reader::models::Individual;
use std::path::Path;

fn load_individuals(base_path: &Path) -> Result<Vec<Individual>> {
    let bef_registry = BefRegister::new();
    bef_registry.load_as::<Individual>(base_path, None)
}
```

For assistance with specific migration cases, refer to the examples in:
- `src/examples/registry_model_integration_example.rs`
- `src/examples/ind_model_integration_example.rs`
- `src/examples/lpr_model_integration_example.rs`