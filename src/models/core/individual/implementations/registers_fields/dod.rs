use crate::models::core::individual::consolidated::Individual;
use crate::models::core::registry_traits::DodFields;
use chrono::NaiveDate;

// Implement the DodFields trait for Individual
impl DodFields for Individual {
    fn death_date(&self) -> Option<NaiveDate> {
        self.death_date
    }

    fn set_death_date(&mut self, value: Option<NaiveDate>) {
        self.death_date = value;
    }

    fn death_cause(&self) -> Option<&str> {
        self.death_cause.as_deref()
    }

    fn set_death_cause(&mut self, value: Option<String>) {
        self.death_cause = value;
    }

    fn underlying_death_cause(&self) -> Option<&str> {
        self.underlying_death_cause.as_deref()
    }

    fn set_underlying_death_cause(&mut self, value: Option<String>) {
        self.underlying_death_cause = value;
    }
}
