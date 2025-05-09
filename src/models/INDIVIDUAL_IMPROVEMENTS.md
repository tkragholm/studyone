# Individual Model Improvements

## Current Understanding

Looking at the codebase, I see that:

1. The `Individual` model is the core entity from which other models (`Child`, `Parent`, `Family`) are derived
2. The data arrives as rows where each row represents an individual, and we extract those into `Individual` instances
3. `Child` and `Parent` models wrap an `Individual` (using `Arc<Individual>`)
4. `Family` models are constructed by grouping individuals and identifying their relationships

## Proposed Improvements

### 1. Enhance Role Classification in Individual

Add a clearer way to determine a person's role in the study:

```rust
/// Role of an individual in the study context
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Child role (subject of study)
    Child,
    /// Parent role (mother or father)
    Parent,
    /// Both child and parent roles
    ChildAndParent,
    /// Other role (relative, etc.)
    Other,
}

impl Individual {
    /// Determine if this individual is a child based on age at reference date
    pub fn is_child(&self, reference_date: &NaiveDate) -> bool {
        if let Some(age) = self.age_at(reference_date) {
            age < 18
        } else {
            false
        }
    }
    
    /// Determine if this individual is a parent based on relations
    pub fn is_parent(&self) -> bool {
        // An individual is a parent if they are referenced as a parent by another individual
        // This would require a lookup in the full dataset
        // Could be implemented as a method that takes the full dataset as a parameter
        false // Default implementation
    }
    
    /// Get the role of this individual at a reference date
    pub fn role_at(&self, reference_date: &NaiveDate, all_individuals: &[Individual]) -> Role {
        let is_child = self.is_child(reference_date);
        let is_parent = self.is_parent_in_dataset(all_individuals);
        
        match (is_child, is_parent) {
            (true, true) => Role::ChildAndParent,
            (true, false) => Role::Child,
            (false, true) => Role::Parent,
            (false, false) => Role::Other,
        }
    }
    
    /// Check if this individual is a parent in the given dataset
    pub fn is_parent_in_dataset(&self, all_individuals: &[Individual]) -> bool {
        all_individuals.iter().any(|ind| {
            ind.mother_pnr.as_ref().map_or(false, |pnr| pnr == &self.pnr) ||
            ind.father_pnr.as_ref().map_or(false, |pnr| pnr == &self.pnr)
        })
    }
}
```

### 2. Add Derived Model Creation Methods

Add factory methods to create specialized models directly from Individual:

```rust
impl Individual {
    /// Create a Child model from this Individual
    pub fn to_child(&self) -> Child {
        Child::from_individual(Arc::new(self.clone()))
    }
    
    /// Create a Parent model from this Individual  
    pub fn to_parent(&self) -> Parent {
        Parent::from_individual(Arc::new(self.clone()))
    }
}
```

### 3. Add Batch Processing Utilities

Add methods to process batches of individuals efficiently:

```rust
impl Individual {
    /// Group individuals by family ID
    pub fn group_by_family(individuals: &[Self]) -> HashMap<String, Vec<&Self>> {
        let mut family_map: HashMap<String, Vec<&Self>> = HashMap::new();
        
        for individual in individuals {
            if let Some(family_id) = &individual.family_id {
                family_map
                    .entry(family_id.clone())
                    .or_default()
                    .push(individual);
            }
        }
        
        family_map
    }
    
    /// Create families from a collection of individuals
    pub fn create_families(individuals: &[Self], reference_date: &NaiveDate) -> Vec<Family> {
        let family_groups = Self::group_by_family(individuals);
        let mut families = Vec::new();
        
        for (family_id, members) in family_groups {
            // Identify family members by role
            let mut mothers = Vec::new();
            let mut fathers = Vec::new();
            let mut children = Vec::new();
            
            for member in &members {
                if member.is_child(reference_date) {
                    children.push(member);
                } else if member.gender == Gender::Female {
                    mothers.push(member);
                } else if member.gender == Gender::Male {
                    fathers.push(member);
                }
            }
            
            // Determine family type
            let family_type = match (mothers.len(), fathers.len()) {
                (1.., 1..) => FamilyType::TwoParent,
                (1.., 0) => FamilyType::SingleMother,
                (0, 1..) => FamilyType::SingleFather,
                (0, 0) => FamilyType::NoParent,
            };
            
            // Create family object
            let family = Family::new(family_id, family_type, *reference_date);
            // Additional setup for the family would be needed here
            
            families.push(family);
        }
        
        families
    }
    
    /// Create Child models for all children in the dataset
    pub fn create_children(individuals: &[Self], reference_date: &NaiveDate) -> Vec<Child> {
        individuals
            .iter()
            .filter(|ind| ind.is_child(reference_date))
            .map(|ind| ind.to_child())
            .collect()
    }
    
    /// Create Parent models for all parents in the dataset
    pub fn create_parents(individuals: &[Self]) -> Vec<Parent> {
        let parent_pnrs: HashSet<&String> = individuals
            .iter()
            .filter_map(|ind| ind.mother_pnr.as_ref())
            .chain(individuals.iter().filter_map(|ind| ind.father_pnr.as_ref()))
            .collect();
            
        individuals
            .iter()
            .filter(|ind| parent_pnrs.contains(&ind.pnr))
            .map(|ind| ind.to_parent())
            .collect()
    }
}
```

### 4. Enhance from_registry_record to Extract More Data

Improve the conversion from registry data to Individual to extract as much information as possible at this stage:

```rust
impl RegistryAware for Individual {
    // Existing implementation...
    
    fn from_registry_record(batch: &RecordBatch, row: usize) -> Result<Option<Self>> {
        // Enhanced implementation that extracts more fields from different registry types
        // This would consolidate knowledge about registry formats
        // And ensure we extract maximum data about an individual in one pass
    }
}
```

### 5. Add Serialization for Enhanced Portability

Ensure serde_arrow support is comprehensive so that:
- Individual models can be efficiently serialized/deserialized
- This facilitates saving processed individuals and loading them later
- Reduces the need for repeated registry data parsing

## Benefits of These Improvements

1. **Centralization**: Makes Individual the clear foundation model
2. **Efficiency**: Reduces code duplication in derived models
3. **Consistency**: Ensures derived models reflect the same underlying data
4. **Performance**: Batch processing utilities optimize operations on collections
5. **Flexibility**: Role-based classification allows for more nuanced analysis

## Implementation Priority

1. Role classification and role detection methods
2. Batch processing utilities 
3. Enhanced registry conversion
4. Factory methods for derived models
5. Serialization improvements

This approach ensures the Individual model becomes a more powerful foundation for your domain model hierarchy while maintaining the existing architecture's strengths.