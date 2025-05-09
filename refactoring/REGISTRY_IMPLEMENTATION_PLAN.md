# Registry Implementation Plan

This document outlines the completed refactoring and the next steps required to fully implement the new registry deserialization architecture.

## Completed Refactoring

1. **Created Registry Detection Module**
   - Created `registry/detect.rs` with proper registry type enum
   - Moved registry detection logic from registry_aware_models.rs

2. **Implemented Registry-Specific Deserializers**
   - Created `registry/bef/deserializer.rs`
   - Created `registry/ind/deserializer.rs` 
   - Moved field mappings to schema modules

3. **Created Central Deserializer Interface**
   - Created `registry/deserializer.rs` with unified interface
   - Implemented fallback minimal deserializer for unsupported registry types

4. **Refactored Individual Model**
   - Replaced `registry.rs` with `registry_integration.rs`
   - Simplified model API to delegate to registry deserializers
   - Maintained backward compatibility

5. **Refactored Registry-Aware Models**
   - Created `registry_aware_models_refactored.rs` that uses new deserializers
   - Simplified trait implementations to use central deserializer

## Implementation Plan

### Phase 1: Complete Setup (Done)
- [x] Create registry detection module
- [x] Set up registry-specific deserializer modules
- [x] Implement central deserializer interface
- [x] Refactor Individual model registry integration

### Phase 2: Registry Implementations
- [x] Implement BEF deserializer
- [x] Implement IND deserializer
- [ ] Implement LPR deserializer
- [ ] Implement MFR deserializer
- [ ] Implement AKM deserializer
- [ ] Implement UDDF deserializer
- [ ] Implement VNDS deserializer
- [ ] Implement DOD deserializer

### Phase 3: Integration and Testing
- [ ] Fully replace `registry_aware_models.rs` with `registry_aware_models_refactored.rs`
- [ ] Update main deserializer to handle all registry types
- [ ] Add comprehensive unit tests for all deserializers
- [ ] Add integration tests to verify model conversions

### Phase 4: Optimization
- [ ] Optimize batch deserialization performance
- [ ] Implement caching for frequently used registry data
- [ ] Add registry-specific validation rules
- [ ] Monitor and optimize memory usage during deserialization

## Next Steps

1. Implement the remaining registry-specific deserializers following the patterns established for BEF and IND

2. Replace the old `registry_aware_models.rs` with the refactored version:
   ```bash
   mv /home/tkragholm/Development/studyone/src/registry/registry_aware_models_refactored.rs /home/tkragholm/Development/studyone/src/registry/registry_aware_models.rs
   ```

3. Update import paths throughout the codebase to use the new module structure

4. Create comprehensive unit tests to verify deserialization for all registry types

This refactoring gives us a clear module structure, with well-defined responsibilities:
- **models**: Define the structure and behavior of domain models
- **registry**: Handle all registry-specific data conversion 
- Each registry has its own schema, deserializer, and conversion logic

The SerdeIndividual wrapper is used as the bridge between Arrow record batches and Rust domain models, providing efficient deserialization.