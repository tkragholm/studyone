use crate::models::core::individual::Individual;
use crate::models::core::registry_traits::VndsFields;
use chrono::NaiveDate;

// Implement VndsFields for Individual
impl VndsFields for Individual {
    fn emigration_date(&self) -> Option<NaiveDate> {
        // Use event_date if event_type indicates emigration
        if let Some(event_type) = &self.event_type {
            if event_type == "U" {
                // Assuming "U" means emigration
                return self.event_date;
            }
        }
        None
    }

    fn set_emigration_date(&mut self, value: Option<NaiveDate>) {
        if value.is_some() {
            self.event_date = value;
            self.event_type = Some("U".to_string()); // Set event type to emigration
        }
    }

    fn immigration_date(&self) -> Option<NaiveDate> {
        // Use event_date if event_type indicates immigration
        if let Some(event_type) = &self.event_type {
            if event_type == "I" {
                // Assuming "I" means immigration
                return self.event_date;
            }
        }
        None
    }

    fn set_immigration_date(&mut self, value: Option<NaiveDate>) {
        if value.is_some() {
            self.event_date = value;
            self.event_type = Some("I".to_string()); // Set event type to immigration
        }
    }

    fn emigration_country(&self) -> Option<&str> {
        // Look for emigration country code in properties
        if let Some(props) = &self.properties() {
            if let Some(cached) = props.get("emigration_country") {
                if let Some(s) = cached.downcast_ref::<String>() {
                    return Some(s.as_str());
                }
            }
        }
        None
    }

    fn set_emigration_country(&mut self, value: Option<String>) {
        if let Some(v) = value {
            self.set_property("emigration_country", Box::new(v));
        }
    }

    fn immigration_country(&self) -> Option<&str> {
        // Look for immigration country code in properties
        if let Some(props) = &self.properties() {
            if let Some(cached) = props.get("immigration_country") {
                if let Some(s) = cached.downcast_ref::<String>() {
                    return Some(s.as_str());
                }
            }
        }
        None
    }

    fn set_immigration_country(&mut self, value: Option<String>) {
        if let Some(v) = value {
            self.set_property("immigration_country", Box::new(v));
        }
    }
}
