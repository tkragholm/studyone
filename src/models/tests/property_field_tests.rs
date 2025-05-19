//! Tests for the `PropertyField` derive macro
//!
//! This module contains tests for the `PropertyField` derive macro.

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use macros::PropertyField;
    use std::collections::HashMap;
    
    /// Test struct with `PropertyField` derive
    #[derive(Debug, PropertyField)]
    struct TestPerson {
        #[property(name = "person_id")]
        id: String,
        
        #[property(name = "full_name")]
        name: String,
        
        #[property(name = "date_of_birth", registry = "BEF")]
        dob: Option<NaiveDate>,
        
        #[property(name = "age")]
        age: Option<i32>,
        
        /// Properties map for storing dynamic properties
        pub properties: Option<HashMap<String, Box<dyn std::any::Any + Send + Sync>>>,
    }
    
    impl TestPerson {
        /// Create a new `TestPerson`
        fn new(id: &str, name: &str) -> Self {
            Self {
                id: id.to_string(),
                name: name.to_string(),
                dob: None,
                age: None,
                properties: Some(HashMap::new()),
            }
        }
    }
    
    #[test]
    fn test_property_field_setter() {
        let mut person = TestPerson::new("123", "John Doe");
        
        // Test setting a regular string field
        person.set_property_field("full_name", Box::new("Jane Doe".to_string()));
        assert_eq!(person.name, "Jane Doe");
        
        // Test setting an option field
        let birth_date = NaiveDate::from_ymd_opt(1990, 1, 1).unwrap();
        person.set_property_field("date_of_birth", Box::new(Some(birth_date)));
        assert_eq!(person.dob, Some(birth_date));
        
        // Test setting another option field
        person.set_property_field("age", Box::new(Some(30)));
        assert_eq!(person.age, Some(30));
        
        // Test storing in properties map
        person.set_property_field("custom_field", Box::new("custom value".to_string()));
        assert!(person.properties.is_some());
        
        let props = person.properties.as_ref().unwrap();
        assert!(props.contains_key("custom_field"));
        
        if let Some(val) = props.get("custom_field") {
            if let Some(str_val) = val.downcast_ref::<String>() {
                assert_eq!(str_val, "custom value");
            } else {
                panic!("Value is not a String");
            }
        } else {
            panic!("Property not found");
        }
    }
}