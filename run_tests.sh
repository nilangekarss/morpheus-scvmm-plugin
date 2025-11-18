#!/bin/bash

echo "Running TemplatesSyncSpec tests..."

# Navigate to project directory
cd "$(dirname "$0")"

# Run the specific test class
./gradlew test --tests "*TemplatesSyncSpec*" --stacktrace --info

# Show test results
echo "Test execution completed."
echo "Check build/reports/tests/test/index.html for detailed results."
