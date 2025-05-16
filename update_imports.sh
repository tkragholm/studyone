#!/bin/bash

# Find all imports that need to be updated
echo "Finding imports that need to be updated..."

echo "Checking for array_utils imports..."
grep -r "use crate::utils::array_utils" src --include="*.rs" | grep -v "src/utils/mod.rs"

echo "Checking for arrow_utils imports..."
grep -r "use crate::utils::arrow_utils" src --include="*.rs" | grep -v "src/utils/mod.rs"

echo "Checking for field_extractors imports..."
grep -r "use crate::utils::field_extractors" src --include="*.rs" | grep -v "src/utils/mod.rs"

echo "Checking for logging imports..."
grep -r "use crate::utils::logging" src --include="*.rs" | grep -v "src/utils/mod.rs"

echo "Checking for progress imports..."
grep -r "use crate::utils::progress" src --include="*.rs" | grep -v "src/utils/mod.rs"

echo "Checking for registry_utils imports..."
grep -r "use crate::utils::registry_utils" src --include="*.rs" | grep -v "src/utils/mod.rs"

echo "Checking for test_utils imports..."
grep -r "use crate::utils::test_utils" src --include="*.rs" | grep -v "src/utils/mod.rs"

echo "Done!"
echo "To update the imports, you can use:"
echo "sed -i 's/use crate::utils::array_utils/use crate::utils::arrow::array_utils/g' src/path/to/file.rs"
echo "sed -i 's/use crate::utils::arrow_utils/use crate::utils::arrow::conversion/g' src/path/to/file.rs"
echo "sed -i 's/use crate::utils::field_extractors/use crate::utils::arrow::extractors/g' src/path/to/file.rs"
echo "sed -i 's/use crate::utils::progress/use crate::utils::logging::progress/g' src/path/to/file.rs"
echo "sed -i 's/use crate::utils::registry_utils/use crate::utils::registry::integration/g' src/path/to/file.rs"
echo "sed -i 's/use crate::utils::test_utils/use crate::utils::test/g' src/path/to/file.rs"