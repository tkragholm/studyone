#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::individual::{Gender, Individual};
    use crate::models::parent::JobSituation;
    use arrow::array::{ArrayRef, Float64Builder, Int8Builder, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use chrono::NaiveDate;

    fn create_test_batch() -> RecordBatch {
        // Create schema
        let schema = Schema::new(vec![
            Field::new("PNR", DataType::Utf8, false),
            Field::new("PERINDKIALT_13", DataType::Float64, true),
            Field::new("LOENMV_13", DataType::Float64, true),
            Field::new("BESKST13", DataType::Int8, true),
        ]);

        // Build string arrays
        let mut pnr_builder = StringBuilder::new();
        pnr_builder.append_value("1234567890"); // Father
        pnr_builder.append_value("2345678901"); // Mother
        let pnr_array = Arc::new(pnr_builder.finish()) as ArrayRef;

        // Build income array
        let mut total_income_builder = Float64Builder::new();
        total_income_builder.append_value(500000.0); // Father's income
        total_income_builder.append_value(400000.0); // Mother's income
        let total_income_array = Arc::new(total_income_builder.finish()) as ArrayRef;

        // Build salary array
        let mut salary_builder = Float64Builder::new();
        salary_builder.append_value(450000.0); // Father's salary
        salary_builder.append_value(380000.0); // Mother's salary
        let salary_array = Arc::new(salary_builder.finish()) as ArrayRef;

        // Build employment array
        let mut employment_builder = Int8Builder::new();
        employment_builder.append_value(1); // Father: Full-time employed
        employment_builder.append_value(3); // Mother: Part-time employed
        let employment_array = Arc::new(employment_builder.finish()) as ArrayRef;

        // Create record batch
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                pnr_array,
                total_income_array,
                salary_array,
                employment_array,
            ],
        )
        .unwrap()
    }

    fn create_test_parents() -> Vec<Parent> {
        // Create father Individual
        let father_individual = Individual::new(
            "1234567890".to_string(),
            Gender::Male,
            Some(NaiveDate::from_ymd_opt(1980, 1, 15).unwrap()),
        );

        // Create mother Individual
        let mother_individual = Individual::new(
            "2345678901".to_string(),
            Gender::Female,
            Some(NaiveDate::from_ymd_opt(1982, 5, 22).unwrap()),
        );

        // Create father Parent
        let father_parent = Parent::from_individual(Arc::new(father_individual));

        // Create mother Parent
        let mother_parent = Parent::from_individual(Arc::new(mother_individual));

        vec![father_parent, mother_parent]
    }

    #[test]
    fn test_ind_income_adapter() {
        let batch = create_test_batch();

        // Create adapter for year 2020
        let adapter = IndIncomeAdapter::new_without_cpi(2020);
        let result = adapter.from_record_batch(&batch);

        assert!(result.is_ok());
        let incomes = result.unwrap();

        // Should have 6 income records (total, salary, other for both parents)
        assert_eq!(incomes.len(), 6);

        // Check father's total income
        let father_total = incomes
            .iter()
            .find(|i| {
                i.individual_pnr == "1234567890"
                    && i.income_type == IncomeType::TotalPersonal.as_str()
            })
            .unwrap();

        assert_eq!(father_total.year, 2020);
        assert_eq!(father_total.amount, 500000.0);

        // Check mother's salary
        let mother_salary = incomes
            .iter()
            .find(|i| {
                i.individual_pnr == "2345678901" && i.income_type == IncomeType::Salary.as_str()
            })
            .unwrap();

        assert_eq!(mother_salary.year, 2020);
        assert_eq!(mother_salary.amount, 380000.0);

        // Check derived other income
        let father_other = incomes
            .iter()
            .find(|i| {
                i.individual_pnr == "1234567890" && i.income_type == IncomeType::Other.as_str()
            })
            .unwrap();

        assert_eq!(father_other.amount, 50000.0); // 500000 - 450000
    }

    #[test]
    fn test_parent_update() {
        let batch = create_test_batch();
        let mut parents = create_test_parents();

        // Create adapter for year 2020
        let adapter = IndIncomeAdapter::new_without_cpi(2020);
        let result = adapter.update_parents(&mut parents, &batch, 2020);

        assert!(result.is_ok());

        // Check father's update
        let father = &parents[0];
        assert!(father.employment_status);
        assert_eq!(father.job_situation, JobSituation::EmployedFullTime);
        assert_eq!(father.pre_exposure_income, Some(500000.0));

        // Check mother's update
        let mother = &parents[1];
        assert!(mother.employment_status);
        assert_eq!(mother.job_situation, JobSituation::EmployedPartTime);
        assert_eq!(mother.pre_exposure_income, Some(400000.0));
    }

    #[test]
    fn test_trajectory_creation() {
        // Create income records for multiple years
        let incomes = vec![
            Income::new(
                "1234567890".to_string(),
                2018,
                480000.0,
                IncomeType::TotalPersonal.as_str().to_string(),
            ),
            Income::new(
                "1234567890".to_string(),
                2019,
                490000.0,
                IncomeType::TotalPersonal.as_str().to_string(),
            ),
            Income::new(
                "1234567890".to_string(),
                2020,
                500000.0,
                IncomeType::TotalPersonal.as_str().to_string(),
            ),
            Income::new(
                "2345678901".to_string(),
                2018,
                380000.0,
                IncomeType::TotalPersonal.as_str().to_string(),
            ),
            Income::new(
                "2345678901".to_string(),
                2019,
                390000.0,
                IncomeType::TotalPersonal.as_str().to_string(),
            ),
            Income::new(
                "2345678901".to_string(),
                2020,
                400000.0,
                IncomeType::TotalPersonal.as_str().to_string(),
            ),
        ];

        // Create adapter
        let adapter = IndIncomeAdapter::new_without_cpi(2020);

        // Create family map
        let mut family_map = HashMap::new();
        family_map.insert(
            "FAM12345".to_string(),
            (
                Some("2345678901".to_string()),
                Some("1234567890".to_string()),
            ),
        );

        // Create trajectories
        let family_trajectories = adapter.create_family_trajectories(&family_map, &incomes);

        assert_eq!(family_trajectories.len(), 1);

        // Check family trajectory
        let family_trajectory = family_trajectories.get("FAM12345").unwrap();
        assert!(family_trajectory.mother_trajectory.is_some());
        assert!(family_trajectory.father_trajectory.is_some());

        // Check income for each year
        assert_eq!(family_trajectory.income_for_year(2018), Some(860000.0)); // 480000 + 380000
        assert_eq!(family_trajectory.income_for_year(2019), Some(880000.0)); // 490000 + 390000
        assert_eq!(family_trajectory.income_for_year(2020), Some(900000.0)); // 500000 + 400000
    }
}
