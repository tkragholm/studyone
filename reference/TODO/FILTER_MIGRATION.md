# Filter Framework Migration Guide

This document outlines the strategy for migrating from the legacy `FilterCriteria` 
filtering system to the new generic `Filter<T>` trait-based system.

## Overview of Changes

### Legacy System
- Uses `FilterCriteria<T>` trait with `meets_criteria(&self, entity: &T) -> bool` method
- Implemented with enums (`IndividualFilter`, `FamilyFilter`, etc.)
- Returns boolean values (passes/fails)
- No error propagation or precise error messages
- No standardized way to express required resources (columns/fields)
- Limited composability through enum variants (`All`, `Any`)

### New System
- Uses `Filter<T>` trait with `apply(&self, input: &T) -> Result<T>` method
- Filter implementations are struct-based for type safety
- Returns `Result<T>` for proper error handling with detailed messages
- Expresses resource requirements with `required_resources()` method
- Rich composability through `AndFilter`, `OrFilter`, `NotFilter`
- Fluent builder API with `FilterBuilder`
- Extension methods through `FilterExt` trait

## Migration Strategy

### Phase 1: Parallel Implementation
1. Keep existing legacy filters working without changes
2. Implement new `Filter<T>` trait for basic filter types
3. Use adapter patterns to bridge between systems
4. Write examples showing both approaches

### Phase 2: Gradual Conversion
1. Convert individual filter types one at a time
2. Use adapters to mix legacy and new filters during transition
3. Update unit tests to use new filter framework
4. Maintain backward compatibility through adapters

### Phase 3: Complete Migration
1. Convert all remaining legacy filters to new system
2. Update all filter usage in the codebase
3. Mark legacy `FilterCriteria` trait as deprecated
4. Eventually remove legacy code

## Migration Patterns

### Direct Conversion Pattern
Convert a legacy filter directly to a new struct-based filter:

```rust
// Legacy
enum IndividualFilter {
    AgeRange {
        min_age: Option<u32>,
        max_age: Option<u32>,
        reference_date: NaiveDate,
    },
    // ...
}

// New
struct AgeRangeFilter {
    min_age: u32,
    max_age: u32,
    reference_date: NaiveDate,
}

impl Filter<Individual> for AgeRangeFilter {
    fn apply(&self, input: &Individual) -> Result<Individual> {
        // Implementation
    }
    
    fn required_resources(&self) -> HashSet<String> {
        // Resource requirements
    }
}
```

### Adapter Pattern
Use adapters to make legacy filters compatible with the new system:

```rust
struct LegacyFilterAdapter<T, F> 
where 
    F: FilterCriteria<T> + Debug + Clone,
    T: Clone,
{
    legacy_filter: F,
    _phantom: PhantomData<T>,
}

impl<T, F> Filter<T> for LegacyFilterAdapter<T, F>
where
    F: FilterCriteria<T> + Debug + Clone,
    T: Clone,
{
    fn apply(&self, input: &T) -> Result<T> {
        if self.legacy_filter.meets_criteria(input) {
            Ok(input.clone())
        } else {
            Err(/* error */)
        }
    }
    
    fn required_resources(&self) -> HashSet<String> {
        HashSet::new()
    }
}
```

### Composition Pattern
Use rich composition APIs instead of enum variants:

```rust
// Legacy
let filter = IndividualFilter::All(vec![
    IndividualFilter::AgeRange { ... },
    IndividualFilter::Gender(Gender::Female),
]);

// New - Builder approach
let filter = FilterBuilder::new()
    .add_filter(AgeRangeFilter { ... })
    .add_filter(GenderFilter { gender: Gender::Female })
    .build_and();

// New - Extension method approach
let filter = AgeRangeFilter { ... }
    .and(GenderFilter { gender: Gender::Female });
```

## Benefits of Migration

1. **Error Handling**: Detailed error messages through the `Result` type
2. **Resource Tracking**: Explicit tracking of required fields/columns
3. **Type Safety**: Struct-based filters instead of enum variants
4. **Composability**: More expressive ways to compose filters
5. **Performance**: Better optimization opportunities
6. **Extensibility**: Easier to add new filter types
7. **Code Organization**: More maintainable code structure

## Testing Strategy

1. Write parallel tests for both systems during migration
2. Ensure exact behavioral equivalence between legacy and new filters
3. Use adapters in tests to validate compatibility
4. Gradually replace legacy filter tests with new system tests

## Examples

See the following example files for detailed migration examples:
- `src/examples/filter_example.rs` - Basic usage of the new filter system
- `src/examples/filter_migration_example.rs` - How to migrate from the legacy system

## Timeline

- **Month 1**: Implement core filter infrastructure and adapters
- **Month 2**: Convert basic filters and update tests
- **Month 3**: Convert remaining filters and update application code
- **Month 4**: Clean up legacy code and finalize migration