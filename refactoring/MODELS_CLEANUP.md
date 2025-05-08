# Models Cleanup Plan

This document outlines the plan for removing old code from the models and registry modules as part of our ongoing refactoring efforts.

## Models Module Cleanup

The `src/models` directory contains several components that can be cleaned up:

### 1. Remove/Migrate Registry-Related Traits

- ✅ Registry-related traits have already been moved to `src/common/traits/registry.rs`
- We can remove the following from `models/traits.rs` (line comments):
  - Comment about `RegistryAware` being moved (already done)

### 2. Clean up Entity Model Implementations

The following entity models need to be migrated to use the new trait system:

- `individual.rs` - Remove direct registry-aware implementation in favor of separate trait implementations
- `child.rs` - Same as above
- `parent.rs` - Same as above
- `family.rs` - Same as above
- `diagnosis.rs` - Same as above
- `income.rs` - Same as above

For each entity model:

1. Remove the direct registry-specific conversion methods
2. Keep the entity model structure itself (core fields and basic methods)
3. Move registry-specific conversion implementations to respective registry-specific files

### 3. Create Clear Separation Between Entity and Registry Concerns

- Remove `RegistryAware` imports from entity models
- Remove registry-specific names and constants from entity models
- Ensure entity models only focus on their domain-specific behavior

## Registry Module Cleanup

The `src/registry` directory has a mix of old and new approaches:

### 1. Migrate Registry Implementations to New Trait-Based Approach

- Use `bef_migration_example.rs` as a template for all registry implementations
- Move all registries to use the new trait-based loader pattern

### 2. Update Model Conversion to Use the Common Traits

- Move all model conversion implementations to use the new trait-based system
- Ensure `ModelConversion` implementations properly align with the new registry implementations

### 3. Remove Outdated Model Conversion Methods

- Remove direct use of registry-specific functions in model conversion code
- Standardize error handling and data mapping patterns

## Implementation Steps

1. **Phase 1: Create Robust Trait-Based Registry Implementations**
   - Complete migration of all registry loaders to the trait-based approach
   - Update factory methods to work with new implementations

2. **Phase 2: Clean Up Model Conversion**
   - Update model conversion to use the new traits
   - Remove direct registry dependencies from entity models

3. **Phase 3: Remove Obsolete Code**
   - Remove commented-out and redundant code
   - Remove unused methods and imports
   - Apply consistent code style throughout
