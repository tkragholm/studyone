//! Registry trait implementations for Individual
//!
//! This module implements various registry-specific traits for the Individual model,
//! defining how registry data is accessed and manipulated.

use crate::models::core::individual::consolidated::Individual;
use crate::models::core::registry_traits::*;

// Implement the MfrFields trait for Individual
impl MfrFields for Individual {
    fn birth_weight(&self) -> Option<i32> {
        self.birth_weight
    }

    fn set_birth_weight(&mut self, value: Option<i32>) {
        self.birth_weight = value;
    }

    fn birth_length(&self) -> Option<i32> {
        self.birth_length
    }

    fn set_birth_length(&mut self, value: Option<i32>) {
        self.birth_length = value;
    }

    fn gestational_age(&self) -> Option<i32> {
        self.gestational_age
    }

    fn set_gestational_age(&mut self, value: Option<i32>) {
        self.gestational_age = value;
    }

    fn apgar_score(&self) -> Option<i32> {
        self.apgar_score
    }

    fn set_apgar_score(&mut self, value: Option<i32>) {
        self.apgar_score = value;
    }

    fn birth_order(&self) -> Option<i32> {
        self.birth_order
    }

    fn set_birth_order(&mut self, value: Option<i32>) {
        self.birth_order = value;
    }

    fn plurality(&self) -> Option<i32> {
        self.plurality
    }

    fn set_plurality(&mut self, value: Option<i32>) {
        self.plurality = value;
    }
}
