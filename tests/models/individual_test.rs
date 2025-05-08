#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_individual_creation() {
        let pnr = "1234567890".to_string();
        let gender = Gender::Male;
        let birth_date = NaiveDate::from_ymd_opt(1990, 1, 1);

        let individual = Individual::new(pnr.clone(), gender, birth_date);

        assert_eq!(individual.pnr, pnr);
        assert_eq!(individual.gender, gender);
        assert_eq!(individual.birth_date, birth_date);
        assert_eq!(individual.death_date, None);
        assert_eq!(individual.origin, Origin::Unknown);
        assert_eq!(individual.education_level, EducationLevel::Unknown);
        assert_eq!(individual.municipality_code, None);
        assert!(!individual.is_rural);
        assert_eq!(individual.mother_pnr, None);
        assert_eq!(individual.father_pnr, None);
        assert_eq!(individual.family_id, None);
        assert_eq!(individual.emigration_date, None);
        assert_eq!(individual.immigration_date, None);
    }

    #[test]
    fn test_age_calculation() {
        let pnr = "1234567890".to_string();
        let gender = Gender::Male;
        let birth_date = Some(NaiveDate::from_ymd_opt(1990, 5, 15).unwrap());

        let individual = Individual::new(pnr, gender, birth_date);

        // Check age before birthday
        let reference_date1 = NaiveDate::from_ymd_opt(2020, 5, 14).unwrap();
        assert_eq!(individual.age_at(&reference_date1), Some(29));

        // Check age on birthday
        let reference_date2 = NaiveDate::from_ymd_opt(2020, 5, 15).unwrap();
        assert_eq!(individual.age_at(&reference_date2), Some(30));

        // Check age after birthday
        let reference_date3 = NaiveDate::from_ymd_opt(2020, 5, 16).unwrap();
        assert_eq!(individual.age_at(&reference_date3), Some(30));
    }

    #[test]
    fn test_alive_status() {
        let mut individual = Individual::new(
            "1234567890".to_string(),
            Gender::Female,
            Some(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()),
        );

        // Before birth
        let before_birth = NaiveDate::from_ymd_opt(1989, 12, 31).unwrap();
        assert!(!individual.was_alive_at(&before_birth));

        // At birth
        let at_birth = NaiveDate::from_ymd_opt(1990, 1, 1).unwrap();
        assert!(individual.was_alive_at(&at_birth));

        // After birth, before death
        let after_birth = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        assert!(individual.was_alive_at(&after_birth));

        // Set death date
        individual.death_date = Some(NaiveDate::from_ymd_opt(2010, 1, 1).unwrap());

        // Before death
        let before_death = NaiveDate::from_ymd_opt(2009, 12, 31).unwrap();
        assert!(individual.was_alive_at(&before_death));

        // At death
        let at_death = NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
        assert!(individual.was_alive_at(&at_death));

        // After death
        let after_death = NaiveDate::from_ymd_opt(2010, 1, 2).unwrap();
        assert!(!individual.was_alive_at(&after_death));
    }

    #[test]
    fn test_residency_status() {
        let mut individual = Individual::new(
            "1234567890".to_string(),
            Gender::Female,
            Some(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()),
        );

        // Initially resident after birth
        let date1 = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        assert!(individual.was_resident_at(&date1));

        // Set emigration date
        individual.emigration_date = Some(NaiveDate::from_ymd_opt(2005, 1, 1).unwrap());

        // Before emigration
        let before_emigration = NaiveDate::from_ymd_opt(2004, 12, 31).unwrap();
        assert!(individual.was_resident_at(&before_emigration));

        // At emigration
        let at_emigration = NaiveDate::from_ymd_opt(2005, 1, 1).unwrap();
        assert!(!individual.was_resident_at(&at_emigration));

        // After emigration
        let after_emigration = NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
        assert!(!individual.was_resident_at(&after_emigration));

        // Set immigration date after emigration
        individual.immigration_date = Some(NaiveDate::from_ymd_opt(2015, 1, 1).unwrap());

        // After emigration, before immigration
        let between_dates = NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
        assert!(!individual.was_resident_at(&between_dates));

        // At immigration
        let at_immigration = NaiveDate::from_ymd_opt(2015, 1, 1).unwrap();
        assert!(individual.was_resident_at(&at_immigration));

        // After immigration
        let after_immigration = NaiveDate::from_ymd_opt(2016, 1, 1).unwrap();
        assert!(individual.was_resident_at(&after_immigration));
    }
}
