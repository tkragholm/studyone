failures:

---- filter::generic_filter_test::test_filter_builder stdout ----

thread 'filter::generic_filter_test::test_filter_builder' panicked at tests/filter/generic_filter_test.rs:119:5:
assertion `left == right` failed
left: [2, 4, 5, 7, 8]
right: [2, 4, 5, 7]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- filter::generic_filter_test::test_filter_combinators stdout ----

thread 'filter::generic_filter_test::test_filter_combinators' panicked at tests/filter/generic_filter_test.rs:59:5:
assertion `left == right` failed
left: [2, 4, 7, 8, 10]
right: [2, 4, 5, 7, 8, 10]

---- filter::generic_filter_test::test_resource_tracking stdout ----

thread 'filter::generic_filter_test::test_resource_tracking' panicked at tests/filter/generic_filter_test.rs:212:5:
assertion failed: age_resources.contains("age")

---- models::income_test::tests::test_family_income_trajectory stdout ----

thread 'models::income_test::tests::test_family_income_trajectory' panicked at tests/models/income_test.rs:130:9:
assertion `left == right` failed
left: 42500.0
right: 55000.0

---- models::income_test::tests::test_income_trajectory stdout ----

thread 'models::income_test::tests::test_income_trajectory' panicked at tests/models/income_test.rs:54:9:
assertion `left == right` failed
left: 34000.0
right: 28500.0

---- integration::filtering_test::test_complex_filters_with_generic_framework stdout ----
Total rows without filtering: 1000000
Rows after AND filter (SOCIO > 200 AND CPRTYPE = 5): 0
AND filter selectivity: 0.00%
Rows after OR filter (SOCIO > 300 OR CPRTYPE = 1): 286235
OR filter selectivity: 28.62%
Error: Filter excluded entity: NOT filter excluded item that passed inner filter

failures:
filter::generic_filter_test::test_filter_builder
filter::generic_filter_test::test_filter_combinators
filter::generic_filter_test::test_resource_tracking
integration::filtering_test::test_complex_filters_with_generic_framework
models::income_test::tests::test_family_income_trajectory
models::income_test::tests::test_income_trajectory
