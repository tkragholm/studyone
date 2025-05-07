# Filter Framework Testing Guide

This document provides guidance on testing the filter framework in the par-reader project.

## Filter Framework Overview

The filter framework consists of several components:

1. **Generic Filters** (`src/filter/generic.rs`):
   - Base trait `Filter<T>` for generic filtering
   - Combinators: `AndFilter`, `OrFilter`, `NotFilter`
   - Utility types: `BoxedFilter`, `FilterBuilder`
   - Extension trait: `FilterExt`

2. **Adapters** (`src/filter/adapter.rs`):
   - `BatchFilterAdapter`: Adapts generic filters to Arrow batch filters
   - `EntityFilterAdapter`: Adapts filters to entity types
   - Domain-specific filters: `IndividualFilter`, `FamilyFilter`

3. **Expression-Based Filters** (`src/filter/expr.rs`):
   - `ExpressionFilter`: Translates expression tree to filters
   - Operators: `Eq`, `Gt`, `Lt`, `And`, `Or`, `Not`, etc.

## Test Categories

The tests are organized into the following categories:

1. **Unit Tests** (`tests/filter/*`):
   - Tests for individual filter components
   - Examples: `generic_filter_test.rs`

2. **Integration Tests** (`tests/integration/*`):
   - Tests for end-to-end filter application
   - Examples: `filtering_test.rs`

3. **Performance Tests**:
   - Tests for filter performance with different data sizes
   - Examples: `test_filter_performance_comparison`, `test_filter_performance`

## Running Tests

### Basic Filter Tests

```bash
# Run all filter unit tests
cargo test filter::

# Run generic filter tests
cargo test filter::generic_filter_test

# Run specific test
cargo test filter::generic_filter_test::test_basic_filters
```

### Integration Tests

```bash
# Run all integration tests for filtering
cargo test integration::filtering_test

# Run specific integration test
cargo test integration::filtering_test::test_complex_filters_with_generic_framework
```

### Performance Tests

```bash
# Run performance tests
cargo test integration::filtering_test::test_filter_performance
```

## Writing New Tests

When writing tests for the filter framework, follow these guidelines:

1. **Unit Tests**:
   - Test each filter type in isolation
   - Test combinators with simple inputs
   - Test edge cases (empty filters, null values)
   - Test resource tracking
   - Test type safety with different entity types

2. **Integration Tests**:
   - Test filter application to actual Parquet data
   - Test complex filter expressions
   - Test filter selectivity
   - Test filtering with different data types
   - Test error handling and edge cases

3. **Test Coverage**:
   - Ensure all filter types are tested
   - Test all combinator operations (AND, OR, NOT)
   - Test adapter behavior
   - Test performance characteristics

## Test Fixtures

The test suite uses several fixtures:

1. **Simple Filters**:
   - `EvenNumberFilter`: Filters for even numbers
   - `GreaterThanFilter`: Filters for numbers > threshold
   - `PredicateFilter`: Filter using a predicate function

2. **Domain Filters**:
   - `AgeFilter`: Filters individuals by age
   - `GenderFilter`: Filters individuals by gender
   - `FamilySizeFilter`: Filters families by number of children
   - `FamilyRuralFilter`: Filters families by rural status

3. **Test Data**:
   - Generated in `tests/utils/individuals.rs` and `tests/utils/families.rs`
   - Various test parquet files in the test directory

## Common Issues and Solutions

1. **Resource Tracking**:
   - Ensure each filter correctly reports its required resources
   - Resource names should match field names used in filtering

2. **NOT Filter Behavior**:
   - The NOT filter inverts the result of its inner filter
   - It accepts values rejected by the inner filter
   - It rejects values accepted by the inner filter

3. **Filter Builder vs. Direct Combinators**:
   - FilterBuilder creates combinator filters (AND, OR, NOT)
   - FilterExt provides extension methods (.and(), .or(), .not())
   - Both approaches should produce equivalent results

4. **Type Casting Issues**:
   - BoxedFilter provides type erasure for heterogeneous filters
   - When encountering type issues, use BoxedFilter to wrap filters

## Conclusion

The filter framework is a powerful tool for efficient data filtering. The test suite ensures that all components work correctly both individually and together. By organizing tests into the categories above, we can maintain good test coverage and catch issues early.
