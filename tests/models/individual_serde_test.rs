use chrono::NaiveDate;
use par_reader::models::individual::Individual;
use par_reader::models::traits::ArrowSchema;
use par_reader::models::types::{
    CitizenshipStatus, EducationField, EducationLevel, Gender, HousingType, MaritalStatus, Origin,
    SocioeconomicStatus,
};

#[test]
fn test_individual_serde_arrow_roundtrip() {
    // Create sample individuals
    let individuals = vec![
        Individual {
            pnr: "1234567890".to_string(),
            gender: Gender::Male,
            birth_date: Some(NaiveDate::from_ymd_opt(1980, 1, 1).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::High,
            municipality_code: Some("0101".to_string()),
            is_rural: false,
            mother_pnr: Some("0987654321".to_string()),
            father_pnr: None,
            family_id: Some("F123".to_string()),
            emigration_date: None,
            immigration_date: None,

            // Employment and socioeconomic status
            socioeconomic_status: SocioeconomicStatus::MediumLevelEmployee,
            occupation_code: Some("2511".to_string()),
            industry_code: Some("62.01".to_string()),
            workplace_id: Some("W12345".to_string()),
            employment_start_date: Some(NaiveDate::from_ymd_opt(2010, 3, 15).unwrap()),
            working_hours: Some(37.5),

            // Education details
            education_field: EducationField::ScienceMathematicsComputing,
            education_completion_date: Some(NaiveDate::from_ymd_opt(2005, 6, 30).unwrap()),
            education_institution: Some("KU".to_string()),
            education_program_code: Some("COMP101".to_string()),

            // Income information
            annual_income: Some(550000.0),
            disposable_income: Some(350000.0),
            employment_income: Some(550000.0),
            self_employment_income: None,
            capital_income: Some(15000.0),
            transfer_income: None,
            income_year: Some(2022),

            // Healthcare usage
            hospital_admissions_count: Some(1),
            emergency_visits_count: Some(0),
            outpatient_visits_count: Some(3),
            gp_visits_count: Some(2),
            last_hospital_admission_date: Some(NaiveDate::from_ymd_opt(2021, 11, 5).unwrap()),
            hospitalization_days: Some(2),

            // Additional demographic information
            marital_status: MaritalStatus::Married,
            citizenship_status: CitizenshipStatus::Danish,
            housing_type: HousingType::SingleFamilyHouse,
            household_size: Some(4),
            household_type: Some("FAMILY_WITH_CHILDREN".to_string()),
        },
        Individual {
            pnr: "0987654321".to_string(),
            gender: Gender::Female,
            birth_date: Some(NaiveDate::from_ymd_opt(1950, 5, 10).unwrap()),
            death_date: None,
            origin: Origin::Western,
            education_level: EducationLevel::Medium,
            municipality_code: Some("0202".to_string()),
            is_rural: true,
            mother_pnr: None,
            father_pnr: None,
            family_id: Some("F456".to_string()),
            emigration_date: Some(NaiveDate::from_ymd_opt(2005, 8, 15).unwrap()),
            immigration_date: Some(NaiveDate::from_ymd_opt(2010, 2, 1).unwrap()),

            // Employment and socioeconomic status
            socioeconomic_status: SocioeconomicStatus::Pensioner,
            occupation_code: None,
            industry_code: None,
            workplace_id: None,
            employment_start_date: None,
            working_hours: None,

            // Education details
            education_field: EducationField::HealthWelfare,
            education_completion_date: Some(NaiveDate::from_ymd_opt(1975, 6, 15).unwrap()),
            education_institution: Some("NU".to_string()),
            education_program_code: Some("NURS101".to_string()),

            // Income information
            annual_income: Some(280000.0),
            disposable_income: Some(220000.0),
            employment_income: None,
            self_employment_income: None,
            capital_income: Some(5000.0),
            transfer_income: Some(275000.0),
            income_year: Some(2022),

            // Healthcare usage
            hospital_admissions_count: Some(2),
            emergency_visits_count: Some(1),
            outpatient_visits_count: Some(5),
            gp_visits_count: Some(4),
            last_hospital_admission_date: Some(NaiveDate::from_ymd_opt(2022, 1, 10).unwrap()),
            hospitalization_days: Some(8),

            // Additional demographic information
            marital_status: MaritalStatus::Widowed,
            citizenship_status: CitizenshipStatus::EuropeanUnion,
            housing_type: HousingType::Apartment,
            household_size: Some(1),
            household_type: Some("SINGLE_PERSON".to_string()),
        },
    ];

    // Convert to RecordBatch
    let batch =
        Individual::to_record_batch(&individuals).expect("Failed to convert to RecordBatch");

    // Verify RecordBatch properties
    assert_eq!(batch.num_rows(), 2, "Record batch should have 2 rows");
    assert!(
        !batch.schema().fields().is_empty(),
        "Schema should have fields"
    );

    // Convert back to Individual vector
    let roundtrip_individuals = Individual::from_record_batch(&batch)
        .expect("Failed to convert from RecordBatch back to Individuals");

    // Verify the number of individuals
    assert_eq!(
        roundtrip_individuals.len(),
        individuals.len(),
        "Number of individuals after roundtrip should match original"
    );

    // Compare each individual
    for (i, (original, roundtrip)) in individuals
        .iter()
        .zip(roundtrip_individuals.iter())
        .enumerate()
    {
        // Compare core fields
        assert_eq!(
            original.pnr, roundtrip.pnr,
            "PNR mismatch for individual {i}"
        );
        assert_eq!(
            original.gender, roundtrip.gender,
            "Gender mismatch for individual {i}"
        );
        assert_eq!(
            original.birth_date, roundtrip.birth_date,
            "Birth date mismatch for individual {i}"
        );
        assert_eq!(
            original.death_date, roundtrip.death_date,
            "Death date mismatch for individual {i}"
        );
        assert_eq!(
            original.origin, roundtrip.origin,
            "Origin mismatch for individual {i}"
        );
        assert_eq!(
            original.education_level, roundtrip.education_level,
            "Education level mismatch for individual {i}"
        );
        assert_eq!(
            original.municipality_code, roundtrip.municipality_code,
            "Municipality code mismatch for individual {i}"
        );
        assert_eq!(
            original.is_rural, roundtrip.is_rural,
            "Is rural mismatch for individual {i}"
        );
        assert_eq!(
            original.mother_pnr, roundtrip.mother_pnr,
            "Mother PNR mismatch for individual {i}"
        );
        assert_eq!(
            original.father_pnr, roundtrip.father_pnr,
            "Father PNR mismatch for individual {i}"
        );
        assert_eq!(
            original.family_id, roundtrip.family_id,
            "Family ID mismatch for individual {i}"
        );
        assert_eq!(
            original.emigration_date, roundtrip.emigration_date,
            "Emigration date mismatch for individual {i}"
        );
        assert_eq!(
            original.immigration_date, roundtrip.immigration_date,
            "Immigration date mismatch for individual {i}"
        );

        // Compare employment and socioeconomic status
        assert_eq!(
            original.socioeconomic_status, roundtrip.socioeconomic_status,
            "Socioeconomic status mismatch for individual {i}"
        );
        assert_eq!(
            original.occupation_code, roundtrip.occupation_code,
            "Occupation code mismatch for individual {i}"
        );
        assert_eq!(
            original.industry_code, roundtrip.industry_code,
            "Industry code mismatch for individual {i}"
        );
        assert_eq!(
            original.workplace_id, roundtrip.workplace_id,
            "Workplace ID mismatch for individual {i}"
        );
        assert_eq!(
            original.employment_start_date, roundtrip.employment_start_date,
            "Employment start date mismatch for individual {i}"
        );
        assert_eq!(
            original.working_hours, roundtrip.working_hours,
            "Working hours mismatch for individual {i}"
        );

        // Compare education details
        assert_eq!(
            original.education_field, roundtrip.education_field,
            "Education field mismatch for individual {i}"
        );
        assert_eq!(
            original.education_completion_date, roundtrip.education_completion_date,
            "Education completion date mismatch for individual {i}"
        );
        assert_eq!(
            original.education_institution, roundtrip.education_institution,
            "Education institution mismatch for individual {i}"
        );
        assert_eq!(
            original.education_program_code, roundtrip.education_program_code,
            "Education program code mismatch for individual {i}"
        );

        // Compare income information
        assert_eq!(
            original.annual_income, roundtrip.annual_income,
            "Annual income mismatch for individual {i}"
        );
        assert_eq!(
            original.disposable_income, roundtrip.disposable_income,
            "Disposable income mismatch for individual {i}"
        );
        assert_eq!(
            original.employment_income, roundtrip.employment_income,
            "Employment income mismatch for individual {i}"
        );
        assert_eq!(
            original.self_employment_income, roundtrip.self_employment_income,
            "Self employment income mismatch for individual {i}"
        );
        assert_eq!(
            original.capital_income, roundtrip.capital_income,
            "Capital income mismatch for individual {i}"
        );
        assert_eq!(
            original.transfer_income, roundtrip.transfer_income,
            "Transfer income mismatch for individual {i}"
        );
        assert_eq!(
            original.income_year, roundtrip.income_year,
            "Income year mismatch for individual {i}"
        );

        // Compare healthcare usage
        assert_eq!(
            original.hospital_admissions_count, roundtrip.hospital_admissions_count,
            "Hospital admissions count mismatch for individual {i}"
        );
        assert_eq!(
            original.emergency_visits_count, roundtrip.emergency_visits_count,
            "Emergency visits count mismatch for individual {i}"
        );
        assert_eq!(
            original.outpatient_visits_count, roundtrip.outpatient_visits_count,
            "Outpatient visits count mismatch for individual {i}"
        );
        assert_eq!(
            original.gp_visits_count, roundtrip.gp_visits_count,
            "GP visits count mismatch for individual {i}"
        );
        assert_eq!(
            original.last_hospital_admission_date, roundtrip.last_hospital_admission_date,
            "Last hospital admission date mismatch for individual {i}"
        );
        assert_eq!(
            original.hospitalization_days, roundtrip.hospitalization_days,
            "Hospitalization days mismatch for individual {i}"
        );

        // Compare additional demographic information
        assert_eq!(
            original.marital_status, roundtrip.marital_status,
            "Marital status mismatch for individual {i}"
        );
        assert_eq!(
            original.citizenship_status, roundtrip.citizenship_status,
            "Citizenship status mismatch for individual {i}"
        );
        assert_eq!(
            original.housing_type, roundtrip.housing_type,
            "Housing type mismatch for individual {i}"
        );
        assert_eq!(
            original.household_size, roundtrip.household_size,
            "Household size mismatch for individual {i}"
        );
        assert_eq!(
            original.household_type, roundtrip.household_type,
            "Household type mismatch for individual {i}"
        );
    }
}
