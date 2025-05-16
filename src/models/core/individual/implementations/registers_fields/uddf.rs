use crate::models::core::individual::Individual;
use crate::models::core::registry_traits::UddfFields;
use chrono::NaiveDate;

// Implement UddfFields for Individual
impl UddfFields for Individual {
    fn education_institution(&self) -> Option<&str> {
        // Converting i32 to String for the trait interface
        self.education_institution.map(|val| {
            // Use the properties map to store a temporary String representation
            if let Some(props) = &self.properties() {
                if let Some(cached) = props.get("cached_education_institution") {
                    if let Some(s) = cached.downcast_ref::<String>() {
                        return s.as_str();
                    }
                }
            }

            // If not cached, return a placeholder
            "_"
        })
    }

    fn set_education_institution(&mut self, value: Option<String>) {
        // Try to convert to i32 if possible
        self.education_institution = value.as_ref().and_then(|s| s.parse::<i32>().ok());

        // Store the original string in the properties map
        if let Some(v) = value {
            self.set_property("cached_education_institution", Box::new(v));
        }
    }

    fn education_start_date(&self) -> Option<NaiveDate> {
        self.education_valid_to
    }

    fn set_education_start_date(&mut self, value: Option<NaiveDate>) {
        self.education_valid_to = value;
    }

    fn education_completion_date(&self) -> Option<NaiveDate> {
        self.education_valid_from
    }

    fn set_education_completion_date(&mut self, value: Option<NaiveDate>) {
        self.education_valid_from = value;
    }

    fn education_program_code(&self) -> Option<&str> {
        // Converting u16 to String for the trait interface
        self.education_code.map(|_| {
            // Use the properties map to store a temporary String representation
            if let Some(props) = &self.properties() {
                if let Some(cached) = props.get("cached_education_program_code") {
                    if let Some(s) = cached.downcast_ref::<String>() {
                        return s.as_str();
                    }
                }
            }

            // If not cached, return a placeholder
            "_"
        })
    }

    fn set_education_program_code(&mut self, value: Option<String>) {
        // Try to convert to u16 if possible
        self.education_code = value.as_ref().and_then(|s| s.parse::<u16>().ok());

        // Store the original string in the properties map
        if let Some(v) = value {
            self.set_property("cached_education_program_code", Box::new(v));
        }
    }
}
