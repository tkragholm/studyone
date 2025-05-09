# Registry-Model Boundaries

This document outlines the clear boundaries between the registry and model modules, along with a migration guide for transitioning to the new approach.

## Problem Statement

The current codebase has redundant code shared between the models module and registry module:

1. **Duplicate Extraction Logic**: The same field extraction code exists in both registry conversion files and model files.
2. **Overlapping Registry-Aware Logic**: Similar functionality for detecting registry types and mapping fields exists in multiple places.
3. **Multiple Conversion Interfaces**: There are at least two separate mechanisms for converting registry data to models.
4. **Redundant Field Mappings**: Field mappings for serde_arrow exist in both model and registry modules.

## Clear Boundaries

Here are the defined boundaries between modules:

### Models Module Responsibilities
1. **Domain Model Definitions**: Define entity structs (Individual, Child, Family, etc.)
2. **Model Business Logic**: Implement domain-specific behavior (age calculation, relationships, etc.)
3. **Model Relationships**: Handle relationships between entities (parent-child, family groups)
4. **Model Validation**: Implement validation logic for model data
5. **Generic Serialization**: Implement generic serialization/deserialization (serde, etc.)

### Registry Module Responsibilities
1. **Registry Schema Definitions**: Define registry-specific schemas and field layouts
2. **Registry Data Loading**: Handle file I/O and record batch creation
3. **Registry Type Detection**: Identify registry types from data schemas
4. **Registry-to-Model Conversion**: Convert registry data to domain models
5. **Registry-specific Field Mapping**: Map registry fields to model fields
6. **Enhancement Functions**: Provide functions to enhance models with registry data

### Common Module Responsibilities
1. **Shared Traits**: Define traits used by both modules
2. **Utility Functions**: Provide utilities used by both modules
3. **Error Types**: Define error types used across the system

## New Architecture

The new architecture introduces:

1. **RegistryModel Trait**: Implemented by domain models to enable registry integration
2. **RegistryConverter Trait**: Implemented by registry modules to provide conversion logic
3. **Centralized Conversion Module**: Houses all registry-specific conversion logic
4. **Registry Type Detection**: Centralized registry type detection

## Migration Guide

To migrate existing code to the new architecture:

1. **Update Model Files**:
   - Implement `RegistryModel` for model types
   - For models that support multiple registries, implement `MultiRegistryModel`
   - Remove registry-specific conversion code from model files

2. **Create Registry Converters**:
   - For each registry type, create a converter that implements `RegistryConverter<ModelType>`
   - Move all registry-specific conversion logic to these converters

3. **Update Registry References**:
   - Update any code that directly calls model conversion methods to use converters
   - Replace calls to `from_registry_record` with calls to converters

4. **Centralize Detection**:
   - Use the centralized `RegistryTypeDetector` instead of scattered detection logic

## Example Usage

### Old Approach:
```rust
// Convert data from BEF registry to Individual
let individual = Individual::from_bef_record(batch, row)?;

// Enhance an Individual with data from any registry
individual.enhance_from_registry(batch, row)?;
```

### New Approach:
```rust
// Using centralized converter
let converter = BefConverter::new();
let individual = converter.convert_record(batch, row)?;

// Enhance using the multi-registry approach
individual.enhance_from_registry(batch, row)?;
```

## Benefits

1. **Clear Separation of Concerns**: Each module has well-defined responsibilities
2. **Reduced Duplication**: Registry-specific logic is centralized
3. **Easier Maintenance**: Changes to registry formats only need to be made in one place
4. **Type Safety**: Proper traits ensure type safety across the system
5. **Better Extensibility**: Adding new registry types is simplified

## Implementation Status

- ✅ Defined clear boundaries
- ✅ Created new traits in `common/traits/registry_v2.rs`
- ✅ Implemented centralized registry conversion module
- ✅ Created BEF converter as example
- ✅ Updated Individual model with new traits
- ❌ Migrate remaining registry types
- ❌ Update client code to use new approach
- ❌ Remove redundant code after migration