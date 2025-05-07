# Migration Plan: From Adapters to Direct Registry Integration

This document outlines the plan for directly replacing the adapter pattern with the new registry-model integration approach.

## Current Pattern vs. New Approach

**Current Pattern:**
- Uses separate adapter classes (`BefIndividualAdapter`, `BefFamilyAdapter`, etc.)
- Implements `RegistryAdapter<T>` trait with static methods
- Requires separate code paths for different model types
- Each adapter duplicates conversion logic

**New Approach:**
- Registry types directly implement `ModelConversion<T>` for various model types
- Registry objects can directly convert to models: `registry.to_models::<T>(batch)`
- Models contain schema-aware constructors for different registry types
- Conversion logic is centralized with the appropriate registry

## Migration Steps

### 1. Replace Usage Patterns

Instead of:
```rust
let individuals = BefIndividualAdapter::from_record_batch(batch)?;
let families = BefFamilyAdapter::from_record_batch(batch)?;
```

Use directly:
```rust
let bef_registry = BefRegister::new();
let individuals = bef_registry.to_models::<Individual>(batch)?;
let families = bef_registry.to_models::<Family>(batch)?;
```

For loading data and conversion in one step:
```rust
let bef_registry = BefRegister::new();
let individuals = bef_registry.load_as::<Individual>(base_path, pnr_filter)?;
```

### 2. Identify and Change All Call Sites

Locations where adapters are used:
- Population generation code
- Test suites
- Example code
- Analysis pipelines

For each location:
1. Instantiate the appropriate registry
2. Replace adapter calls with direct conversions via registry
3. Update documentation to reflect the new pattern

### 3. Phase Out Adapter Modules

Once all usage is migrated:
1. Add deprecation notices to adapter modules
2. Eventually remove adapter modules entirely
3. Update documentation and examples to use only the new approach

### 4. Benefits of Direct Replacement

- Cleaner, more intuitive API
- Reduced code duplication
- Registry types become more "intelligent" about their data
- More maintainable in the long run
- Better type safety through trait bounds

### 5. Example Migration

**Before:**
```rust
fn process_population_data(batch: &RecordBatch) -> Result<Population> {
    // Use adapters to convert data
    let individuals = BefIndividualAdapter::from_record_batch(batch)?;
    let families = BefFamilyAdapter::from_record_batch(batch)?;
    
    // Further processing...
    // ...
}
```

**After:**
```rust
fn process_population_data(batch: &RecordBatch) -> Result<Population> {
    // Create registry instance
    let bef_registry = BefRegister::new();
    
    // Direct conversion to models
    let individuals = bef_registry.to_models::<Individual>(batch)?;
    let families = bef_registry.to_models::<Family>(batch)?;
    
    // Further processing...
    // ...
}
```

### 6. Testing Strategy

For each registry type:
1. Write tests confirming that model conversion gives identical results to the old adapters
2. Create new tests demonstrating the direct loading capabilities
3. Ensure error cases are properly handled in the new approach

## Conclusion

By directly replacing the adapter pattern with the new registry integration approach, we'll achieve a cleaner, more maintainable codebase with less duplication and a more intuitive API. The migration can be done progressively, ensuring stability throughout the process.