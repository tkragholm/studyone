# Test Organization Guide

This guide outlines the improved organization of tests in the par-reader project to make them easier to run and maintain.

## Test Structure

The tests are now organized in a modular structure that mirrors the source code organization:

```
tests/
├── algorithm/
│   └── population/
│       └── population_test.rs
├── filter/
│   ├── mod.rs
│   └── generic_filter_test.rs
├── integration/
│   ├── async_test.rs
│   ├── filtering_test.rs
│   ├── registry_integration_test.rs
│   └── type_adaptation_test.rs
├── lib.rs
├── models/
│   ├── adapters/
│   │   ├── bef_adapter_test.rs
│   │   ├── ind_adapter_test.rs
│   │   ├── lpr_adapter_test.rs
│   │   └── mfr_adapter_test.rs
│   ├── child_test.rs
│   ├── diagnosis_test.rs
│   ├── family_test.rs
│   ├── income_test.rs
│   └── parent_test.rs
├── registry/
│   ├── akm_test.rs
│   ├── bef_test.rs
│   ├── ind_test.rs
│   ├── lpr_adm_test.rs
│   ├── lpr_bes_test.rs
│   ├── lpr_diag_test.rs
│   ├── mfr_test.rs
│   ├── uddf_test.rs
│   └── vnds_test.rs
└── utils/
    ├── families.rs
    ├── individuals.rs
    └── mod.rs
```

## Running Specific Tests

### 1. Filter Tests

```bash
# Run all filter tests
cargo test filter

# Run generic filter tests
cargo test filter::generic_filter_test

# Run a specific test function
cargo test filter::generic_filter_test::test_basic_filters
```

### 2. Integration Tests

```bash
# Run all integration tests
cargo test integration

# Run filtering integration tests
cargo test integration::filtering_test

# Run a specific integration test function
cargo test integration::filtering_test::test_simple_filters
```

### 3. Model Tests

```bash
# Run all model tests
cargo test models

# Run child model tests
cargo test models::child_test

# Run adapter tests
cargo test models::adapters
```

### 4. Registry Tests

```bash
# Run all registry tests
cargo test registry

# Run a specific registry test
cargo test registry::akm_test
```

### 5. Algorithm Tests

```bash
# Run algorithm tests
cargo test algorithm
```

## Test Tags

You can also run tests based on specific attributes:

```bash
# Run only tests with #[test] attribute
cargo test

# Run tests with specific attributes
cargo test -- --include-ignored

# Run only the tests that have been ignored
cargo test -- --ignored
```

## Recommended Test Groups

To make testing more manageable, some recommended test groups have been created:

```bash
# Run only the fast tests (good for quick feedback)
cargo test -- --include-ignored fast

# Run data integrity tests
cargo test -- --include-ignored integrity

# Run performance tests
cargo test -- --include-ignored performance
```

## Adding New Tests

When adding new tests:

1. Place them in the appropriate module based on the component they're testing
2. Update the module declarations in the nearest mod.rs or lib.rs file
3. Follow the naming convention: `test_[feature]_[scenario]`

## Test Utilities

The `tests/utils` directory contains helper functions and test data generators:

- `families.rs` - Functions for creating test family data
- `individuals.rs` - Functions for creating test individual data

## Integration with CI/CD

The test organization makes it easy to run specific test groups in CI/CD pipelines:

```yaml
# Example: Running only filter tests in CI
- name: Run filter tests
  run: cargo test filter
```

## Troubleshooting

If you encounter issues where tests can't be found:

1. Check that the module is properly declared in `tests/lib.rs`
2. Ensure the test has the `#[test]` attribute
3. Make sure the test function is public (or within a test module)

## Performance Considerations

When running tests, consider:

- Using `--no-capture` to see output from tests
- Using `--nocapture` to see output from tests that pass
- Using `--test-threads=1` for tests that might interfere with each other
- Using the `--quiet` flag to reduce output when running many tests