# Unified Schema System

This directory contains the implementation of the unified schema system for the par-reader library.

## Overview

The unified schema system provides a centralized approach to defining registry schemas, field mappings, and data conversions. It creates a single source of truth for field definitions, reducing code duplication and making it easier to maintain and extend the system.

## Components

The unified schema system consists of the following components:

1. **Field Definitions**: Centralized definitions of registry fields, including their names, types, and semantics.
2. **Field Mappings**: Mappings between registry fields and Individual model fields.
3. **Registry Schemas**: Unified schema definitions for each registry.
4. **Generic Deserializer**: A generic deserializer that can convert registry data to Individual models using the unified schema.

## Usage

To use the unified schema system, you can either:

1. Enable it for individual registry loaders:
   ```rust
   let mut registry = AkmRegister::new();
   registry.use_unified_system(true);
   ```

2. Use the unified factory module for creating registry loaders:
   ```rust
   use crate::registry::unified as unified_factory;
   
   let registry = unified_factory::registry_from_name("akm")?;
   ```

3. Use the command-line flag in the main program:
   ```bash
   ./par-reader --unified
   ```

## Benefits

The unified schema system provides several benefits:

1. **Reduced Code Duplication**: Field definitions are defined once and reused across all registries.
2. **Centralized Field Mapping**: Field mappings are defined in a centralized location, making it easier to maintain and update.
3. **Type Safety**: The system provides type-safe extraction and setting of values.
4. **Extensibility**: New registries can be added easily by reusing existing field definitions and mappings.
5. **Documentation**: Field definitions include descriptions and metadata, improving documentation and discoverability.

## Implementation Details

The unified schema system is implemented in the following modules:

- `schema/field_def/`: Core definitions for field types and schema fields.
- `schema/field_def/mapping.rs`: Implements mapping between registry fields and Individual model fields.
- `schema/field_def/registry_schema.rs`: Defines a unified schema for registries.
- `registry/field_definitions.rs`: Central repository of standard field definitions.
- `registry/generic_deserializer.rs`: Generic deserializer that works with the unified schema system.
- `registry/{akm,bef,lpr,...}/schema_unified.rs`: Unified schema definitions for each registry.

## Examples

See `src/examples/unified_system_example.rs` for a complete example of how to use the unified schema system.