// Main test module that includes all sub-modules
pub mod utils;
pub mod registry {
    pub mod akm_test;
    pub mod bef_test;
    pub mod ind_test;
    pub mod lpr_adm_test;
    pub mod lpr_bes_test;
    pub mod lpr_diag_test;
    pub mod mfr_test;
    pub mod uddf_test;
    pub mod vnds_test;
}
pub mod integration {
    pub mod async_test;
    pub mod filtering_test;
    pub mod registry_integration_test;
}

// pub mod models {
//     pub mod child_test;
//     pub mod diagnosis_test;
//     pub mod family_test;
//     pub mod income_test;
//     pub mod parent_test;
// }
