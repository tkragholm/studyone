#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gender_from_string() {
        assert_eq!(Gender::from("M"), Gender::Male);
        assert_eq!(Gender::from("male"), Gender::Male);
        assert_eq!(Gender::from("F"), Gender::Female);
        assert_eq!(Gender::from("female"), Gender::Female);
        assert_eq!(Gender::from("unknown"), Gender::Unknown);
    }

    #[test]
    fn test_gender_from_int() {
        assert_eq!(Gender::from(1), Gender::Male);
        assert_eq!(Gender::from(2), Gender::Female);
        assert_eq!(Gender::from(0), Gender::Unknown);
    }

    #[test]
    fn test_age_calculation() {
        let individual = Individual {
            pnr: "1234567890".to_string(),
            gender: Gender::Male,
            birth_date: Some(NaiveDate::from_ymd_opt(1980, 6, 15).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::Medium,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: None,
            emigration_date: None,
            immigration_date: None,
        };

        // Test age on birthday
        assert_eq!(
            individual.age_at(&NaiveDate::from_ymd_opt(2020, 6, 15).unwrap()),
            Some(40)
        );

        // Test age day before birthday
        assert_eq!(
            individual.age_at(&NaiveDate::from_ymd_opt(2020, 6, 14).unwrap()),
            Some(39)
        );

        // Test age day after birthday
        assert_eq!(
            individual.age_at(&NaiveDate::from_ymd_opt(2020, 6, 16).unwrap()),
            Some(40)
        );
    }

    #[test]
    fn test_was_alive_at() {
        let mut individual = Individual {
            pnr: "1234567890".to_string(),
            gender: Gender::Male,
            birth_date: Some(NaiveDate::from_ymd_opt(1980, 6, 15).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::Medium,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: None,
            emigration_date: None,
            immigration_date: None,
        };

        // Test alive with no death date
        assert!(individual.was_alive_at(&NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()));

        // Test before birth
        assert!(!individual.was_alive_at(&NaiveDate::from_ymd_opt(1979, 1, 1).unwrap()));

        // Test with death date
        individual.death_date = Some(NaiveDate::from_ymd_opt(2010, 3, 10).unwrap());
        assert!(individual.was_alive_at(&NaiveDate::from_ymd_opt(2010, 3, 10).unwrap())); // Alive on death date
        assert!(!individual.was_alive_at(&NaiveDate::from_ymd_opt(2010, 3, 11).unwrap())); // Not alive after death
    }

    #[test]
    fn test_was_resident_at() {
        let mut individual = Individual {
            pnr: "1234567890".to_string(),
            gender: Gender::Male,
            birth_date: Some(NaiveDate::from_ymd_opt(1980, 6, 15).unwrap()),
            death_date: None,
            origin: Origin::Danish,
            education_level: EducationLevel::Medium,
            municipality_code: Some("101".to_string()),
            is_rural: false,
            mother_pnr: None,
            father_pnr: None,
            family_id: None,
            emigration_date: None,
            immigration_date: None,
        };

        // Test resident with no emigration
        assert!(individual.was_resident_at(&NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()));

        // Test with emigration
        individual.emigration_date = Some(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap());
        assert!(!individual.was_resident_at(&NaiveDate::from_ymd_opt(2000, 1, 1).unwrap())); // Not resident on emigration date

        // Test with emigration and immigration
        individual.immigration_date = Some(NaiveDate::from_ymd_opt(2010, 1, 1).unwrap());
        assert!(!individual.was_resident_at(&NaiveDate::from_ymd_opt(2005, 1, 1).unwrap())); // Not resident between emigration and immigration
        assert!(individual.was_resident_at(&NaiveDate::from_ymd_opt(2010, 1, 1).unwrap())); // Resident on immigration date
        assert!(individual.was_resident_at(&NaiveDate::from_ymd_opt(2015, 1, 1).unwrap())); // Resident after immigration
    }
}
