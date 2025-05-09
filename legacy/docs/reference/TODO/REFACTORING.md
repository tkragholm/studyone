# Par-reader Refactoring Plan

## Completed Refactoring

We've refactored the algorithm module to improve maintainability and reduce code duplication. The main improvements include:

1. **Shared Utilities for Arrow Data Types**
   - Created a central `arrow_utils.rs` module with helpers for Arrow array conversions
   - Functions for extracting values from arrays of different types

2. **Unified Progress Reporting**
   - Added a `progress.rs` utility for standardized progress bars
   - Consistent interface for progress tracking across the codebase

3. **Modularized Matching Algorithm**
   - Split the monolithic algorithm.rs (1000+ lines) into focused modules:
     - `types.rs`: Common type definitions
     - `matcher.rs`: Core matching algorithm orchestration
     - `control_data.rs`: Optimized struct-of-arrays for controls
     - `case_group.rs`: Grouped cases for efficient parallel processing
     - `extraction.rs`: Data extraction utilities
     - `validation.rs`: Input validation
     - `filtering.rs`: Record batch filtering
     - `parallel.rs`: Parallel matching implementation
     - `sequential.rs`: Sequential matching implementation

## Planned Refactoring

### 1. Unify Filtering Framework

The codebase currently has multiple filtering approaches:
- Arrow-based batch filtering (`src/filter/`)
- Object-based domain filtering (`src/algorithm/population/filters.rs`)
- Configuration-based matching criteria (`src/algorithm/matching/criteria.rs`)

**Proposed Strategy:**

1. **Generalize Core Filter Trait**
   ```rust
   pub trait Filter<T> {
       fn apply(&self, input: &T) -> Result<T>;
       fn required_resources(&self) -> HashSet<String>;
   }
   ```

2. **Create Specialized Implementations**
   ```rust
   pub trait BatchFilter = Filter<RecordBatch>;
   pub trait EntityFilter<E> = Filter<E>; // For domain entities
   ```

3. **Implement Adapters**
   - Bridge between different filtering implementations
   - Allow domain filters to work with core framework

4. **Extend Expression System**
   - Support both Arrow and domain entities
   - Unified expression language

5. **Migration Path**
   - Adapt existing implementations to use the new system
   - Maintain backward compatibility during transition

### 2. Standardize Registry Integration

The registry integration code has duplication between:
- Health registry integration (`src/algorithm/health/lpr_integration.rs`)
- Population registry integration (`src/algorithm/population/integration.rs`)

**Proposed Architecture:**

1. **Three-Layer Architecture**
   - **Registry Access Layer**: Data loading from files
   - **Transformation Layer**: Converting raw data to domain models
   - **Integration Layer**: Combining data across registries

2. **Unified Registry Interface**
   ```rust
   pub trait Registry {
       // Registry metadata
       fn name(&self) -> &'static str;
       fn schema(&self) -> SchemaRef;
       
       // Data access
       fn load(&self, path: &Path, filter: Option<&HashSet<String>>) -> Result<Vec<RecordBatch>>;
       fn load_async<'a>(...) -> Pin<Box<dyn Future<...>>>;
       
       // Transformation to domain models
       fn transform<T: DomainModel>(&self, batch: &RecordBatch) -> Result<Vec<T>>;
       
       // Integration capabilities 
       fn can_integrate_with(&self, other: &dyn Registry) -> bool;
       fn integrate<T: DomainModel>(&self, data: &[T], other_data: &[T]) -> Result<Vec<T>>;
   }
   ```

3. **Integration Manager**
   ```rust
   pub struct RegistryIntegrationManager {
       // Registry loaders
       registries: HashMap<String, Arc<dyn Registry>>,
       
       // Domain model collections
       individuals: HashMap<String, Individual>,
       families: FamilyCollection,
       diagnoses: HashMap<String, Vec<Diagnosis>>,
       
       // Configuration
       config: RegistryConfig,
   }
   ```

### 3. Standardize Builder Pattern

1. **Create Generic Builder Trait**
   ```rust
   pub trait Builder<T> {
       type Error;
       fn build(self) -> Result<T, Self::Error>;
   }
   ```

2. **Extract Common Builder Implementations**
   - Apply consistent builder pattern across configuration objects
   - Reduce duplication in validation logic

3. **Create Builder Macros**
   - Simplify creation of new builders
   - Ensure consistent implementation

### 4. Consistent Error Handling

1. **Domain-Specific Error Types**
   - Create specialized error types for each domain
   - Use enums with contextual information

2. **Error Context Pattern**
   - Standardize error context wrapping
   - Create helper utilities for common error scenarios

3. **Error Conversion**
   - Implement consistent `From` trait implementations
   - Create helper methods for common patterns

## Migration Strategy

The current refactoring maintains backward compatibility by keeping original files and re-exporting new modules. This pattern should continue throughout the refactoring process:

1. Create new implementations alongside existing ones
2. Gradually migrate call sites to use the new implementations
3. Once migration is complete, remove the original implementations

This approach allows us to improve the codebase incrementally without breaking existing functionality.

## Immediate Next Steps

1. Create shared utilities for registry integration
2. Begin generalizing the filter trait
3. Implement adapters for existing filter implementations
4. Create documentation for the new architecture