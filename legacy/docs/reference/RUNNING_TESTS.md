# Running Tests in Par-Reader

This document explains how to run specific tests in the par-reader project.

## Running All Tests

To run all tests:

```bash
cargo test
```

## Running Specific Test Modules

You can run tests from specific modules using the module path. The tests are organized into logical modules in the `tests/lib.rs` file.

### Examples:

1. Run all integration tests:

```bash
cargo test integration
```

2. Run filtering integration tests:

```bash
cargo test integration::filtering_test
```

3. Run specific test function:

```bash
cargo test integration::filtering_test::test_simple_filters
```

## Available Test Modules

The project has the following test modules:

1. **Integration Tests**:
   - `integration::filtering_test` - Tests for filtering functionality
   - `integration::async_test` - Tests for async operations
   - `integration::registry_integration_test` - Tests for registry integration
   - `integration::type_adaptation_test` - Tests for type adaptation

2. **Filter Tests**:
   - `filter::generic_filter_test` - Tests for the generic filter framework

3. **Registry Tests**:
   - `registry::akm_test` - Tests for AKM registry
   - `registry::bef_test` - Tests for BEF registry
   - `registry::ind_test` - Tests for IND registry
   - `registry::lpr_adm_test` - Tests for LPR admissions
   - `registry::lpr_bes_test` - Tests for LPR visits
   - `registry::lpr_diag_test` - Tests for LPR diagnoses
   - `registry::mfr_test` - Tests for MFR registry
   - `registry::uddf_test` - Tests for UDDF registry
   - `registry::vnds_test` - Tests for VNDS registry

4. **Model Tests**:
   - `models::child_test` - Tests for child model
   - `models::diagnosis_test` - Tests for diagnosis model
   - `models::family_test` - Tests for family model
   - `models::income_test` - Tests for income model
   - `models::parent_test` - Tests for parent model
   - `models::adapters::bef_adapter_test` - Tests for BEF adapter
   - `models::adapters::ind_adapter_test` - Tests for IND adapter
   - `models::adapters::lpr_adapter_test` - Tests for LPR adapter
   - `models::adapters::mfr_adapter_test` - Tests for MFR adapter

5. **Algorithm Tests**:
   - `algorithm::population::population_test` - Tests for population generation algorithms

## Viewing Test Output

To see the output from all tests (even passing ones), use:

```bash
cargo test -- --nocapture
```

To run tests in a single thread (useful for debugging thread-related issues):

```bash
cargo test -- --test-threads=1
```

## Filtering Tests by Name

You can run tests that match a pattern:

```bash
cargo test simple_filters  # Will run all tests with "simple_filters" in the name
```

## Running Tests with Features

If your project has feature flags, you can run tests with specific features:

```bash
cargo test --features "feature1 feature2"
```

## Running Tests for Integration and Library Code

To run only library tests (not integration tests):

```bash
cargo test --lib
```

To run only integration tests:

```bash
cargo test --test '*'
```

To run a specific integration test:

```bash
cargo test --test integration
```