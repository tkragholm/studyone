❯ cargo test
warning: function `load_multiple_registries_async` is never used
--> src/registry/factory.rs:108:14
|
108 | pub async fn load_multiple_registries_async(
| ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
|
= note: `#[warn(dead_code)]` on by default

warning: `par-reader` (lib) generated 1 warning
warning: `par-reader` (lib test) generated 1 warning (1 duplicate)
Compiling par-reader v0.1.0 (/Users/tobiaskragholm/par-reader)
Finished `test` profile [unoptimized + debuginfo] target(s) in 1.25s
Running unittests src/lib.rs (target/debug/deps/par_reader-13b1b94a590c9d92)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

    Running unittests src/main.rs (target/debug/deps/par_reader-cac032f0561ae5f0)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

    Running tests/lib.rs (target/debug/deps/lib-4648748cd64dc8c4)

running 47 tests
test integration::async_test::test_async_filtering ... ok
test integration::filtering_test::test_filter_performance ... ok
test registry::akm_test::test_akm_filtering ... ok
test registry::akm_test::test_akm_basic_read ... ok
test registry::akm_test::test_akm_schema_validation ... ok
test integration::async_test::test_async_read ... ok
test registry::bef_test::test_bef_basic_read ... FAILED
test integration::filtering_test::test_simple_filters ... ok
test integration::filtering_test::test_complex_filters ... FAILED
test integration::filtering_test::test_filter_data_types ... ok
test registry::akm_test::test_akm_transformation ... ok
test registry::ind_test::test_ind_filter_by_country ... ok
test integration::async_test::test_async_vs_sync_performance ... ok
test registry::ind_test::test_ind_basic_read ... ok
test registry::lpr_adm_test::test_lpr_adm_basic_read ... ok
test registry::lpr_adm_test::test_lpr_adm_date_transformation ... ok
test registry::lpr_adm_test::test_lpr_adm_parallel_read ... ok
test registry::ind_test::test_ind_registry_manager ... ok
test registry::ind_test::test_ind_parallel_read ... ok
test registry::lpr_bes_test::test_lpr_bes_filter_by_procedure ... ok
test registry::lpr_bes_test::test_lpr_bes_basic_read ... ok
test registry::bef_test::test_bef_registry_manager ... ok
test registry::akm_test::test_akm_registry_manager ... ok
test registry::bef_test::test_bef_parallel_read ... ok
test registry::lpr_diag_test::test_lpr_diag_filter_by_diagnosis ... FAILED
test registry::lpr_diag_test::test_lpr_diag_basic_read ... ok
test registry::mfr_test::test_mfr_basic_read ... ok
test registry::mfr_test::test_mfr_filter_by_birth_details ... ok
test registry::lpr_bes_test::test_lpr_bes_parallel_read ... ok
test registry::mfr_test::test_mfr_parallel_read ... ok
test registry::lpr_bes_test::test_lpr_bes_registry_manager ... ok
test registry::lpr_diag_test::test_lpr_diag_parallel_read ... ok
test registry::lpr_diag_test::test_lpr_diag_registry_manager ... ok
test registry::lpr_adm_test::test_lpr_adm_registry_manager ... ok
test registry::vnds_test::test_vnds_basic_read ... ok
test integration::async_test::test_parallel_async_read ... ok
test registry::uddf_test::test_uddf_basic_read ... ok
test registry::uddf_test::test_uddf_registry_manager ... ok
test registry::vnds_test::test_vnds_registry_manager ... ok
test registry::vnds_test::test_vnds_parallel_read ... ok
test registry::mfr_test::test_mfr_registry_manager ... ok
test registry::uddf_test::test_uddf_parallel_read ... ok
test registry::bef_test::test_bef_pnr_filter ... ok
test integration::registry_integration_test::test_cross_registry_operations ... ok
test integration::registry_integration_test::test_parallel_load_all_registries ... ok
test integration::registry_integration_test::test_multiple_registry_operations ... ok
test integration::async_test::test_concurrent_async_operations has been running for over 60 seconds
test integration::async_test::test_concurrent_async_operations ... ok

failures:

---- registry::bef_test::test_bef_basic_read stdout ----
Error: Test file not found: /Users/tobiaskragholm/generated_data/parquet/bef/2020.parquet

---- integration::filtering_test::test_complex_filters stdout ----
Total rows without filtering: 1000000
Rows after AND filter (SOCIO > 200 AND CPRTYPE = 5): 0
AND filter selectivity: 0.00%
Rows after OR filter (SOCIO > 300 OR CPRTYPE = 1): 409361
OR filter selectivity: 40.94%
Error: Unsupported filter expression: LtEq("SOCIO", Int(200))

---- registry::lpr_diag_test::test_lpr_diag_filter_by_diagnosis stdout ----
Error: Filter error: Column DIAG not found in batch

failures:
integration::filtering_test::test_complex_filters
registry::bef_test::test_bef_basic_read
registry::lpr_diag_test::test_lpr_diag_filter_by_diagnosis

test result: FAILED. 44 passed; 3 failed; 0 ignored; 0 measured; 0 filtered out; finished in 89.89s

error: test failed, to rerun pass `--test lib`
