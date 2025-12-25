# Test Runner Script

## Overview

The `run_tests.py` script is a comprehensive test runner that validates your code before pushing. It performs multiple checks to catch issues early.

## Usage

```bash
# From the market_data_poc directory
python run_tests.py
```

## What It Checks

### Phase 1: Basic Validation
- **Python Syntax**: Validates all Python files have correct syntax
- **Module Imports**: Ensures all source modules can be imported
- **Dependencies**: Checks that required packages are installed

### Phase 2: Code Quality
- **Code Formatting (Black)**: Checks code follows Black formatting standards
- **Linting (flake8)**: Finds critical code quality issues
- **Type Checking (mypy)**: Validates type hints (if configured)

### Phase 3: Unit Tests
- **Pytest Execution**: Runs all unit tests in the `tests/` directory
- **Test Results**: Reports passed/failed tests with details

### Phase 4: Test Coverage
- **Coverage Report**: Checks that test coverage meets minimum threshold (60%)
- **Coverage Details**: Shows which modules need more tests

## Status Codes

- **[PASS]**: Check passed successfully
- **[FAIL]**: Critical issue that should be fixed before pushing
- **[WARN]**: Non-critical issue (e.g., platform-specific, formatting)
- **[SKIP]**: Check skipped (e.g., tool not installed)

## Platform-Specific Notes

### Windows Development

Some checks may show warnings on Windows due to platform-specific issues:

- **PySpark**: PySpark has known issues on Windows (UnixStreamServer). This is expected and code will work fine in Databricks.
- **Delta Lake**: Similar platform-specific issues on Windows.

These warnings can be safely ignored when developing on Windows, as the code will run correctly in the Databricks environment.

## Installing Missing Tools

If checks are skipped due to missing tools:

```bash
# Install all development dependencies
pip install black flake8 mypy pytest pytest-cov

# Or install from requirements.txt (if it includes dev dependencies)
pip install -r requirements.txt
```

## Fixing Common Issues

### Syntax Errors
- Review the error message and line number
- Fix the syntax issue in the reported file

### Import Errors
- Ensure all dependencies are installed: `pip install -r requirements.txt`
- Check that module paths are correct
- Verify `__init__.py` files exist in package directories

### Test Failures
- Review the test output for specific failure details
- Run individual tests: `pytest tests/test_utils.py -v`
- Check that test data and mocks are set up correctly

### Coverage Issues
- Add tests for uncovered code
- Aim for at least 60% coverage
- Focus on critical business logic first

### Formatting Issues
- Auto-fix with: `black src/ tests/`
- Review changes before committing

### Linting Issues
- Review flake8 output for specific issues
- Fix critical errors (E9, F63, F7, F82)
- Consider fixing warnings for better code quality

## Integration with CI/CD

This script is designed to catch issues locally before pushing. The GitHub Actions CI pipeline runs similar checks automatically.

### Pre-Push Workflow

1. Run `python run_tests.py`
2. Fix any **[FAIL]** status checks
3. Review **[WARN]** status checks (fix if needed)
4. Push your changes

### CI Pipeline

The CI pipeline will run:
- All unit tests
- Code quality checks
- Coverage reporting

## Troubleshooting

### Script Won't Run
- Ensure you're in the `market_data_poc` directory
- Check Python version: `python --version` (should be 3.10+)
- Verify script is executable: `chmod +x run_tests.py` (Unix/Mac)

### Tests Time Out
- Some tests may take longer on slower machines
- Increase timeout in script if needed (default: 5 minutes)
- Consider running specific test files: `pytest tests/test_utils.py`

### Import Errors on Windows
- PySpark and Delta Lake have known Windows compatibility issues
- These are expected and won't affect Databricks execution
- Consider using WSL or Docker for local development if needed

### Coverage Too Low
- Add tests for uncovered functions
- Focus on business logic and critical paths
- Use `pytest --cov=src --cov-report=html` to see detailed coverage

## Extending the Script

To add new checks:

1. Add a new method to the `TestRunner` class
2. Call it from `run_all_checks()`
3. Use `_add_result()` to report status

Example:
```python
def check_custom_validation(self) -> None:
    """Check custom validation."""
    print("Running custom check...")
    # Your validation logic here
    if validation_passed:
        self._add_result("Custom Check", CheckStatus.PASS, "Validation passed")
    else:
        self._add_result("Custom Check", CheckStatus.FAIL, "Validation failed")
```

## Best Practices

1. **Run Before Committing**: Always run the test script before committing
2. **Fix Failures First**: Address **[FAIL]** status checks immediately
3. **Review Warnings**: Don't ignore warnings - they may indicate real issues
4. **Keep Coverage High**: Maintain at least 60% test coverage
5. **Format Code**: Use Black to maintain consistent formatting
6. **Type Hints**: Add type hints for better code quality and IDE support

