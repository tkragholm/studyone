#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use par_reader::models::child::Child;
    use par_reader::models::family::*;
    use par_reader::models::individual::{EducationLevel, Gender, Origin};
    use par_reader::models::parent::Parent;
    use par_reader::models::*;
    use std::sync::Arc;

    /// Create a test individual
    fn create_test_individual(pnr: &str, birth_year: i32, gender: Gender) -> Individual {
        Individual {
            pnr: pnr.to_string(),
            gender,
            birth_date: Some(NaiveDate::from_ymd_opt(birth_year, 1, 1).unwrap()),
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
        }
    }

    #[test]
    fn test_family_creation() {
        let valid_from = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let family = Family::new("FAM123".to_string(), FamilyType::TwoParent, valid_from);

        assert_eq!(family.family_id, "FAM123");
        assert_eq!(family.family_type, FamilyType::TwoParent);
        assert_eq!(family.valid_from, valid_from);
        assert!(family.valid_to.is_none());
        assert!(family.mother.is_none());
        assert!(family.father.is_none());
        assert!(family.children.is_empty());
    }

    #[test]
    fn test_family_validity() {
        let valid_from = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let valid_to = NaiveDate::from_ymd_opt(2010, 12, 31).unwrap();

        let mut family = Family::new("FAM123".to_string(), FamilyType::TwoParent, valid_from);
        family.valid_to = Some(valid_to);

        // Test dates within validity period
        assert!(family.was_valid_at(&NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()));
        assert!(family.was_valid_at(&NaiveDate::from_ymd_opt(2005, 6, 15).unwrap()));
        assert!(family.was_valid_at(&NaiveDate::from_ymd_opt(2010, 12, 31).unwrap()));

        // Test dates outside validity period
        assert!(!family.was_valid_at(&NaiveDate::from_ymd_opt(1999, 12, 31).unwrap()));
        assert!(!family.was_valid_at(&NaiveDate::from_ymd_opt(2011, 1, 1).unwrap()));
    }

    #[test]
    fn test_family_snapshot() {
        // Create individuals
        let mother_ind = create_test_individual("1234567890", 1970, Gender::Female);
        let father_ind = create_test_individual("0987654321", 1968, Gender::Male);
        let child1_ind = create_test_individual("1122334455", 2000, Gender::Male);
        let child2_ind = create_test_individual("5544332211", 2002, Gender::Female);

        // Create parents and children
        let mother = Arc::new(Parent::from_individual(Arc::new(mother_ind)));
        let father = Arc::new(Parent::from_individual(Arc::new(father_ind)));
        let child1 = Arc::new(Child::from_individual(Arc::new(child1_ind)));
        let child2 = Arc::new(Child::from_individual(Arc::new(child2_ind)));

        // Create family
        let valid_from = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let mut family = Family::new("FAM123".to_string(), FamilyType::TwoParent, valid_from)
            .with_mother(mother)
            .with_father(father);

        family.add_child(child1);
        family.add_child(child2);

        // Create snapshot
        let snapshot_date = NaiveDate::from_ymd_opt(2005, 6, 15).unwrap();
        let snapshot = family.snapshot_at(&snapshot_date).unwrap();

        // Verify snapshot
        assert_eq!(snapshot.family_id, "FAM123");
        assert_eq!(snapshot.family_type, FamilyType::TwoParent);
        assert_eq!(snapshot.children.len(), 2);
        assert!(snapshot.mother.is_some());
        assert!(snapshot.father.is_some());
        assert_eq!(snapshot.snapshot_date, snapshot_date);
    }

    #[test]
    fn test_family_collection() {
        let mut collection = FamilyCollection::new();

        // Create families
        let family1 = Family::new(
            "FAM1".to_string(),
            FamilyType::TwoParent,
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
        );

        let family2 = Family::new(
            "FAM2".to_string(),
            FamilyType::SingleMother,
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
        );

        // Add families to collection
        collection.add_family(family1);
        collection.add_family(family2);

        // Verify collection
        assert_eq!(collection.family_count(), 2);
        assert!(collection.get_family("FAM1").is_some());
        assert!(collection.get_family("FAM2").is_some());
        assert!(collection.get_family("FAM3").is_none());

        // Get families by type
        let two_parent_families = collection.get_families_by_type(FamilyType::TwoParent);
        assert_eq!(two_parent_families.len(), 1);
        assert_eq!(two_parent_families[0].family_id, "FAM1");

        let single_mother_families = collection.get_families_by_type(FamilyType::SingleMother);
        assert_eq!(single_mother_families.len(), 1);
        assert_eq!(single_mother_families[0].family_id, "FAM2");
    }
}
