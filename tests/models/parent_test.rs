#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use par_reader::models::Parent;
    use par_reader::models::diagnosis::{Diagnosis, DiagnosisType};
    use par_reader::models::income::Income;
    use par_reader::models::individual::{EducationLevel, Gender, Individual, Origin};
    use par_reader::models::parent::JobSituation;
    use par_reader::models::parent::ParentCollection;
    use std::sync::Arc;

    /// Create a test individual for a parent
    fn create_test_individual() -> Individual {
        Individual {
            pnr: "1234567890".to_string(),
            gender: Gender::Female,
            birth_date: Some(NaiveDate::from_ymd_opt(1975, 6, 15).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::Medium,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: Some("FAM123".to_string()),
            emigration_date: None,
            immigration_date: None,
        }
    }

    #[test]
    fn test_parent_creation() {
        let individual = Arc::new(create_test_individual());
        let parent = Parent::from_individual(individual.clone());

        assert_eq!(parent.individual().pnr, "1234567890");
        assert!(!parent.employment_status);
        assert_eq!(parent.job_situation, JobSituation::Other);
        assert!(!parent.has_comorbidity);
        assert!(parent.pre_exposure_income.is_none());
        assert!(parent.diagnoses.is_empty());
        assert!(parent.income_data.is_empty());
    }

    #[test]
    fn test_parent_with_attributes() {
        let individual = Arc::new(create_test_individual());
        let parent = Parent::from_individual(individual.clone())
            .with_employment_status(true)
            .with_job_situation(JobSituation::EmployedFullTime)
            .with_pre_exposure_income(350000.0);

        assert!(parent.employment_status);
        assert_eq!(parent.job_situation, JobSituation::EmployedFullTime);
        assert_eq!(parent.pre_exposure_income, Some(350000.0));
    }

    #[test]
    fn test_add_diagnosis() {
        let individual = Arc::new(create_test_individual());
        let mut parent = Parent::from_individual(individual.clone());

        // Initially no comorbidity
        assert!(!parent.has_comorbidity);

        // Add a diagnosis
        let diagnosis = Diagnosis {
            individual_pnr: "1234567890".to_string(),
            diagnosis_code: "J45".to_string(),
            diagnosis_type: DiagnosisType::Secondary,
            diagnosis_date: Some(NaiveDate::from_ymd_opt(2015, 3, 10).unwrap()),
            is_scd: false,
            severity: 1,
        };

        parent.add_diagnosis(Arc::new(diagnosis));

        // Should now have comorbidity
        assert!(parent.has_comorbidity);
        assert_eq!(parent.diagnoses.len(), 1);
    }

    #[test]
    fn test_add_income() {
        let individual = Arc::new(create_test_individual());
        let mut parent = Parent::from_individual(individual.clone());

        // Add income data points
        let income1 = Income {
            individual_pnr: "1234567890".to_string(),
            year: 2010,
            amount: 300000.0,
            income_type: "salary".to_string(),
        };

        let income2 = Income {
            individual_pnr: "1234567890".to_string(),
            year: 2011,
            amount: 320000.0,
            income_type: "salary".to_string(),
        };

        parent.add_income(Arc::new(income1));
        parent.add_income(Arc::new(income2));

        // Check retrieval
        assert_eq!(parent.income_data.len(), 2);
        assert_eq!(parent.income_for_year(2010), Some(300000.0));
        assert_eq!(parent.income_for_year(2011), Some(320000.0));
        assert_eq!(parent.income_for_year(2012), None);

        // Check trajectory
        let trajectory = parent.income_trajectory(2009, 2012);
        assert_eq!(trajectory.len(), 2);
        assert_eq!(trajectory.get(&2010), Some(&300000.0));
        assert_eq!(trajectory.get(&2011), Some(&320000.0));
        assert_eq!(trajectory.get(&2009), None);
        assert_eq!(trajectory.get(&2012), None);
    }

    #[test]
    fn test_had_diagnosis_before() {
        let individual = Arc::new(create_test_individual());
        let mut parent = Parent::from_individual(individual.clone());

        // Add a diagnosis from 2015
        let diagnosis = Diagnosis {
            individual_pnr: "1234567890".to_string(),
            diagnosis_code: "J45".to_string(),
            diagnosis_type: DiagnosisType::Secondary,
            diagnosis_date: Some(NaiveDate::from_ymd_opt(2015, 3, 10).unwrap()),
            is_scd: false,
            severity: 1,
        };

        parent.add_diagnosis(Arc::new(diagnosis));

        // Test dates
        assert!(!parent.had_diagnosis_before(&NaiveDate::from_ymd_opt(2015, 3, 9).unwrap()));
        assert!(parent.had_diagnosis_before(&NaiveDate::from_ymd_opt(2015, 3, 11).unwrap()));
        assert!(parent.had_diagnosis_before(&NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()));
    }

    #[test]
    fn test_parent_collection() {
        let mut collection = ParentCollection::new();

        // Create parents
        let individual1 = Arc::new(Individual {
            pnr: "1111111111".to_string(),
            gender: Gender::Female,
            birth_date: Some(NaiveDate::from_ymd_opt(1975, 1, 1).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::Medium,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: Some("FAM1".to_string()),
            emigration_date: None,
            immigration_date: None,
        });

        let individual2 = Arc::new(Individual {
            pnr: "2222222222".to_string(),
            gender: Gender::Male,
            birth_date: Some(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::High,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: Some("FAM1".to_string()),
            emigration_date: None,
            immigration_date: None,
        });

        let parent1 = Parent::from_individual(individual1.clone())
            .with_employment_status(true)
            .with_job_situation(JobSituation::EmployedFullTime);

        let parent2 = Parent::from_individual(individual2.clone())
            .with_employment_status(false)
            .with_job_situation(JobSituation::Unemployed);

        // Add to collection
        collection.add_parent(parent1);
        collection.add_parent(parent2);

        // Test collection
        assert_eq!(collection.count(), 2);
        assert!(collection.get_parent("1111111111").is_some());
        assert!(collection.get_parent("2222222222").is_some());
        assert!(collection.get_parent("3333333333").is_none());

        // Test filtering
        let employed = collection.employed_parents();
        assert_eq!(employed.len(), 1);
        assert_eq!(employed[0].individual().pnr, "1111111111");

        let unemployed = collection.unemployed_parents();
        assert_eq!(unemployed.len(), 1);
        assert_eq!(unemployed[0].individual().pnr, "2222222222");
    }
}
