#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_income_creation() {
        let income = Income::new(
            "1234567890".to_string(),
            2015,
            350000.0,
            "salary".to_string(),
        );

        assert_eq!(income.individual_pnr, "1234567890");
        assert_eq!(income.year, 2015);
        assert_eq!(income.amount, 350000.0);
        assert_eq!(income.income_type, "salary");
    }

    #[test]
    fn test_income_trajectory() {
        let mut trajectory = IncomeTrajectory::new("1234567890".to_string(), "salary".to_string());

        // Add income for several years
        trajectory.add_income(2010, 300000.0);
        trajectory.add_income(2011, 320000.0);
        trajectory.add_income(2012, 335000.0);
        trajectory.add_income(2013, 342000.0);
        trajectory.add_income(2014, 355000.0);

        // Test basic properties
        assert_eq!(trajectory.individual_pnr, "1234567890");
        assert_eq!(trajectory.income_type, "salary");
        assert_eq!(trajectory.start_year, 2010);
        assert_eq!(trajectory.end_year, 2014);

        // Test retrieval
        assert_eq!(trajectory.income_for_year(2010), Some(300000.0));
        assert_eq!(trajectory.income_for_year(2012), Some(335000.0));
        assert_eq!(trajectory.income_for_year(2015), None);

        // Test statistics
        assert_eq!(trajectory.mean_income(), Some(330400.0));

        // Test trend (should be positive as income increases each year)
        let trend = trajectory.trend().unwrap();
        assert!(trend > 0.0);

        // Test pre-post difference
        let diff = trajectory.pre_post_difference(2012, 2, 2).unwrap();
        let pre_mean = (300000.0 + 320000.0) / 2.0;
        let post_mean = (335000.0 + 342000.0) / 2.0;
        assert_eq!(diff, post_mean - pre_mean);

        // Test interpolation
        let mut trajectory_with_gaps =
            IncomeTrajectory::new("1234567890".to_string(), "salary".to_string());
        trajectory_with_gaps.add_income(2010, 300000.0);
        trajectory_with_gaps.add_income(2014, 340000.0);

        // Should have gaps for 2011-2013
        assert_eq!(trajectory_with_gaps.income_for_year(2011), None);
        assert_eq!(trajectory_with_gaps.income_for_year(2012), None);
        assert_eq!(trajectory_with_gaps.income_for_year(2013), None);

        // Fill gaps
        trajectory_with_gaps.interpolate_missing();

        // Should now have interpolated values
        assert!(trajectory_with_gaps.income_for_year(2011).is_some());
        assert!(trajectory_with_gaps.income_for_year(2012).is_some());
        assert!(trajectory_with_gaps.income_for_year(2013).is_some());

        // Check if interpolation is reasonable (linear)
        let step = (340000.0 - 300000.0) / 4.0;
        assert!(
            (trajectory_with_gaps.income_for_year(2011).unwrap() - (300000.0 + step)).abs() < 0.001
        );
        assert!(
            (trajectory_with_gaps.income_for_year(2012).unwrap() - (300000.0 + 2.0 * step)).abs()
                < 0.001
        );
        assert!(
            (trajectory_with_gaps.income_for_year(2013).unwrap() - (300000.0 + 3.0 * step)).abs()
                < 0.001
        );
    }

    #[test]
    fn test_family_income_trajectory() {
        // Create individual trajectories
        let mut mother_trajectory =
            IncomeTrajectory::new("1111111111".to_string(), "salary".to_string());
        mother_trajectory.add_income(2010, 250000.0);
        mother_trajectory.add_income(2011, 260000.0);
        mother_trajectory.add_income(2012, 270000.0);

        let mut father_trajectory =
            IncomeTrajectory::new("2222222222".to_string(), "salary".to_string());
        father_trajectory.add_income(2010, 300000.0);
        father_trajectory.add_income(2011, 320000.0);
        father_trajectory.add_income(2012, 335000.0);

        // Create family trajectory
        let family_trajectory = FamilyIncomeTrajectory::new("FAM123".to_string())
            .with_mother_trajectory(mother_trajectory)
            .with_father_trajectory(father_trajectory);

        // Test combined income
        assert_eq!(family_trajectory.income_for_year(2010), Some(550000.0));
        assert_eq!(family_trajectory.income_for_year(2011), Some(580000.0));
        assert_eq!(family_trajectory.income_for_year(2012), Some(605000.0));

        // Test income gap
        assert_eq!(family_trajectory.income_gap(2010), Some(50000.0));
        assert_eq!(family_trajectory.income_gap(2011), Some(60000.0));
        assert_eq!(family_trajectory.income_gap(2012), Some(65000.0));

        // Test income gap trend (should be positive as gap increases)
        let gap_trend = family_trajectory.income_gap_trend().unwrap();
        assert!(gap_trend > 0.0);

        // Test primary earner share
        let share_2010 = family_trajectory.primary_earner_share(2010).unwrap();
        assert!((share_2010 - (300000.0 / 550000.0)).abs() < 0.001);

        // Test pre-post difference
        let diff = family_trajectory.pre_post_difference(2011, 1, 1).unwrap();
        assert_eq!(diff, 605000.0 - 550000.0);
    }

    #[test]
    fn test_income_collection() {
        let mut collection = IncomeCollection::new();

        // Add income records
        let income1 = Income::new(
            "1111111111".to_string(),
            2010,
            250000.0,
            "salary".to_string(),
        );

        let income2 = Income::new(
            "1111111111".to_string(),
            2011,
            260000.0,
            "salary".to_string(),
        );

        let income3 = Income::new(
            "2222222222".to_string(),
            2010,
            300000.0,
            "salary".to_string(),
        );

        collection.add_income(income1);
        collection.add_income(income2);
        collection.add_income(income3);

        // Test retrieval
        let incomes1 = collection.get_incomes("1111111111");
        assert_eq!(incomes1.len(), 2);

        let incomes2 = collection.get_incomes("2222222222");
        assert_eq!(incomes2.len(), 1);

        // Test trajectory retrieval
        let trajectory1 = collection.get_trajectory("1111111111", "salary").unwrap();
        assert_eq!(trajectory1.individual_pnr, "1111111111");
        assert_eq!(trajectory1.income_for_year(2010), Some(250000.0));
        assert_eq!(trajectory1.income_for_year(2011), Some(260000.0));

        // Test family trajectory calculation
        let family_map = HashMap::from([(
            "FAM1".to_string(),
            (
                Some("1111111111".to_string()),
                Some("2222222222".to_string()),
            ),
        )]);

        collection.build_family_trajectories(&family_map, "salary");

        let family_trajectory = collection.get_family_trajectory("FAM1").unwrap();
        assert_eq!(family_trajectory.income_for_year(2010), Some(550000.0));

        // Test counts
        assert_eq!(collection.record_count(), 3);
        assert_eq!(collection.individual_count(), 2);
        assert_eq!(collection.family_count(), 1);
    }
}
