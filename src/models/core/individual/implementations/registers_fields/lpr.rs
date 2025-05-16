use crate::models::core::individual::consolidated::Individual;
use crate::models::core::registry_traits::*;
use chrono::NaiveDate;

// Implement the LprFields trait for Individual
impl LprFields for Individual {
    fn diagnoses(&self) -> Option<&[String]> {
        self.diagnoses.as_deref()
    }

    fn set_diagnoses(&mut self, value: Option<Vec<String>>) {
        self.diagnoses = value;
    }

    fn add_diagnosis(&mut self, diagnosis: String) {
        if let Some(diagnoses) = &mut self.diagnoses {
            diagnoses.push(diagnosis);
        } else {
            self.diagnoses = Some(vec![diagnosis]);
        }
    }

    fn procedures(&self) -> Option<&[String]> {
        self.procedures.as_deref()
    }

    fn set_procedures(&mut self, value: Option<Vec<String>>) {
        self.procedures = value;
    }

    fn add_procedure(&mut self, procedure: String) {
        if let Some(procedures) = &mut self.procedures {
            procedures.push(procedure);
        } else {
            self.procedures = Some(vec![procedure]);
        }
    }

    fn hospital_admissions(&self) -> Option<&[NaiveDate]> {
        self.hospital_admissions.as_deref()
    }

    fn set_hospital_admissions(&mut self, value: Option<Vec<NaiveDate>>) {
        self.hospital_admissions = value;
    }

    fn add_hospital_admission(&mut self, date: NaiveDate) {
        if let Some(admissions) = &mut self.hospital_admissions {
            admissions.push(date);
        } else {
            self.hospital_admissions = Some(vec![date]);
        }
    }

    fn discharge_dates(&self) -> Option<&[NaiveDate]> {
        self.discharge_dates.as_deref()
    }

    fn set_discharge_dates(&mut self, value: Option<Vec<NaiveDate>>) {
        self.discharge_dates = value;
    }

    fn add_discharge_date(&mut self, date: NaiveDate) {
        if let Some(dates) = &mut self.discharge_dates {
            dates.push(date);
        } else {
            self.discharge_dates = Some(vec![date]);
        }
    }

    fn length_of_stay(&self) -> Option<i32> {
        self.length_of_stay
    }

    fn set_length_of_stay(&mut self, value: Option<i32>) {
        self.length_of_stay = value;
    }
}
