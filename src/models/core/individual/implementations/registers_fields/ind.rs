use crate::models::core::individual::consolidated::Individual;
use crate::models::core::registry_traits::*;

// Implement the IndFields trait for Individual
impl IndFields for Individual {
    fn annual_income(&self) -> Option<f64> {
        self.annual_income
    }

    fn set_annual_income(&mut self, value: Option<f64>) {
        self.annual_income = value;
    }

    fn disposable_income(&self) -> Option<f64> {
        None // Not implemented in the Individual struct yet
    }

    fn set_disposable_income(&mut self, _value: Option<f64>) {
        // Not implemented in the Individual struct yet
    }

    fn employment_income(&self) -> Option<f64> {
        self.employment_income
    }

    fn set_employment_income(&mut self, value: Option<f64>) {
        self.employment_income = value;
    }

    fn self_employment_income(&self) -> Option<f64> {
        None // Not implemented in the Individual struct yet
    }

    fn set_self_employment_income(&mut self, _value: Option<f64>) {
        // Not implemented in the Individual struct yet
    }

    fn capital_income(&self) -> Option<f64> {
        None // Not implemented in the Individual struct yet
    }

    fn set_capital_income(&mut self, _value: Option<f64>) {
        // Not implemented in the Individual struct yet
    }

    fn transfer_income(&self) -> Option<f64> {
        None // Not implemented in the Individual struct yet
    }

    fn set_transfer_income(&mut self, _value: Option<f64>) {
        // Not implemented in the Individual struct yet
    }

    fn income_year(&self) -> Option<i32> {
        self.income_year
    }

    fn set_income_year(&mut self, value: Option<i32>) {
        self.income_year = value;
    }
}
