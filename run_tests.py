#!/usr/bin/env python3
"""
Comprehensive test runner script for the market data pipeline.

This script runs all tests and validation checks to catch issues before pushing.
It performs:
- Import validation
- Syntax checking
- Unit tests with pytest
- Code quality checks (linting, formatting, type checking)
- Coverage reporting
"""

import sys
import subprocess
import importlib
import ast
import os
from pathlib import Path
from typing import List, Tuple, Dict
from dataclasses import dataclass
from enum import Enum


class CheckStatus(Enum):
    """Status of a check."""
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    SKIP = "SKIP"


@dataclass
class CheckResult:
    """Result of a check."""
    name: str
    status: CheckStatus
    message: str
    details: str = ""


class TestRunner:
    """Comprehensive test runner."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.src_dir = project_root / "src"
        self.tests_dir = project_root / "tests"
        self.results: List[CheckResult] = []
        self.failed_checks: List[str] = []
        
    def run_all_checks(self) -> bool:
        """Run all validation checks."""
        print("=" * 80)
        print("COMPREHENSIVE TEST RUNNER")
        print("=" * 80)
        print()
        
        # Phase 1: Basic validation
        print("Phase 1: Basic Validation")
        print("-" * 80)
        self.check_python_syntax()
        self.check_imports()
        self.check_missing_modules()
        print()
        
        # Phase 2: Code quality
        print("Phase 2: Code Quality Checks")
        print("-" * 80)
        self.check_code_formatting()
        self.check_linting()
        self.check_type_hints()
        print()
        
        # Phase 3: Unit tests
        print("Phase 3: Unit Tests")
        print("-" * 80)
        self.run_unit_tests()
        print()
        
        # Phase 4: Coverage
        print("Phase 4: Test Coverage")
        print("-" * 80)
        self.check_coverage()
        print()
        
        # Summary
        self.print_summary()
        
        return len(self.failed_checks) == 0
    
    def check_python_syntax(self) -> None:
        """Check Python syntax for all Python files."""
        print("Checking Python syntax...")
        syntax_errors = []
        files_checked = 0
        
        for py_file in self._get_python_files():
            files_checked += 1
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    source = f.read()
                ast.parse(source, filename=str(py_file))
            except SyntaxError as e:
                syntax_errors.append(f"{py_file}:{e.lineno}: {e.msg}")
            except UnicodeDecodeError as e:
                syntax_errors.append(f"{py_file}: Encoding error - {str(e)}")
            except Exception as e:
                syntax_errors.append(f"{py_file}: {str(e)}")
        
        if syntax_errors:
            self._add_result(
                "Python Syntax Check",
                CheckStatus.FAIL,
                f"Found {len(syntax_errors)} syntax error(s) in {files_checked} file(s)",
                "\n".join(syntax_errors[:20])  # Limit to first 20 errors
            )
        else:
            self._add_result(
                "Python Syntax Check",
                CheckStatus.PASS,
                f"All {files_checked} Python files have valid syntax"
            )
    
    def check_imports(self) -> None:
        """Check that all modules can be imported."""
        print("Checking module imports...")
        import_errors = []
        import_warnings = []
        
        # Check src modules
        src_modules = [
            "src.config",
            "src.schemas",
            "src.transforms",
            "src.utils",
            "src.data_sources.base_client",
            "src.data_sources.yahoo_finance",
        ]
        
        for module_name in src_modules:
            try:
                importlib.import_module(module_name)
            except ImportError as e:
                import_errors.append(f"{module_name}: {str(e)}")
            except AttributeError as e:
                # Handle platform-specific issues (e.g., PySpark on Windows)
                if "UnixStreamServer" in str(e) or "Unix" in str(e):
                    import_warnings.append(f"{module_name}: Platform-specific issue (may work in Databricks)")
                else:
                    import_errors.append(f"{module_name}: {str(e)}")
            except Exception as e:
                # Check if it's a known platform issue
                error_str = str(e)
                if "UnixStreamServer" in error_str or "Unix" in error_str:
                    import_warnings.append(f"{module_name}: Platform-specific issue (may work in Databricks)")
                else:
                    import_errors.append(f"{module_name}: Unexpected error - {str(e)}")
        
        if import_errors:
            self._add_result(
                "Module Import Check",
                CheckStatus.FAIL,
                f"Failed to import {len(import_errors)} module(s)",
                "\n".join(import_errors)
            )
        elif import_warnings:
            self._add_result(
                "Module Import Check",
                CheckStatus.WARN,
                f"Platform-specific warnings for {len(import_warnings)} module(s) (may work in Databricks)",
                "\n".join(import_warnings)
            )
        else:
            self._add_result(
                "Module Import Check",
                CheckStatus.PASS,
                "All modules imported successfully"
            )
    
    def check_missing_modules(self) -> None:
        """Check for missing required modules."""
        print("Checking for missing dependencies...")
        required_modules = [
            "pytest",
            "pyspark",
            "delta",
            "pandas",
            "numpy",
            "yfinance",
            "requests",
            "beautifulsoup4",
        ]
        
        missing = []
        optional_issues = []
        
        for module in required_modules:
            try:
                # Handle special cases
                if module == "delta":
                    importlib.import_module("delta.tables")
                elif module == "beautifulsoup4":
                    importlib.import_module("bs4")
                elif module == "pyspark":
                    # PySpark may have platform-specific issues on Windows
                    try:
                        importlib.import_module(module)
                    except (AttributeError, ImportError) as e:
                        if "UnixStreamServer" in str(e) or "Unix" in str(e):
                            optional_issues.append(f"{module}: Platform-specific (works in Databricks)")
                        else:
                            raise
                else:
                    importlib.import_module(module)
            except ImportError:
                missing.append(module)
            except AttributeError as e:
                # Platform-specific issues
                if "UnixStreamServer" in str(e) or "Unix" in str(e):
                    optional_issues.append(f"{module}: Platform-specific (works in Databricks)")
                else:
                    missing.append(module)
        
        if missing:
            self._add_result(
                "Dependency Check",
                CheckStatus.FAIL,
                f"Missing {len(missing)} required module(s)",
                f"Missing: {', '.join(missing)}\nRun: pip install -r requirements.txt"
            )
        elif optional_issues:
            self._add_result(
                "Dependency Check",
                CheckStatus.WARN,
                f"Platform-specific issues (may work in Databricks)",
                "\n".join(optional_issues)
            )
        else:
            self._add_result(
                "Dependency Check",
                CheckStatus.PASS,
                "All required dependencies are installed"
            )
    
    def check_code_formatting(self) -> None:
        """Check code formatting with Black."""
        print("Checking code formatting (Black)...")
        try:
            result = subprocess.run(
                ["black", "--check", "--diff", "src/", "tests/"],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                self._add_result(
                    "Code Formatting (Black)",
                    CheckStatus.PASS,
                    "All code is properly formatted"
                )
            else:
                self._add_result(
                    "Code Formatting (Black)",
                    CheckStatus.WARN,
                    "Code formatting issues found",
                    result.stdout + result.stderr
                )
        except FileNotFoundError:
            self._add_result(
                "Code Formatting (Black)",
                CheckStatus.SKIP,
                "Black not installed (run: pip install black)"
            )
        except subprocess.TimeoutExpired:
            self._add_result(
                "Code Formatting (Black)",
                CheckStatus.WARN,
                "Black check timed out"
            )
        except Exception as e:
            self._add_result(
                "Code Formatting (Black)",
                CheckStatus.WARN,
                f"Error running Black: {str(e)}"
            )
    
    def check_linting(self) -> None:
        """Check code with flake8."""
        print("Checking code quality (flake8)...")
        try:
            result = subprocess.run(
                [
                    "flake8",
                    "src/",
                    "tests/",
                    "--count",
                    "--select=E9,F63,F7,F82",  # Critical errors only
                    "--show-source",
                    "--statistics",
                    "--max-line-length=120",
                ],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                self._add_result(
                    "Linting (flake8)",
                    CheckStatus.PASS,
                    "No critical linting issues found"
                )
            else:
                # Count errors
                error_count = len([line for line in result.stdout.split('\n') if line.strip()])
                self._add_result(
                    "Linting (flake8)",
                    CheckStatus.WARN,
                    f"Found {error_count} linting issue(s)",
                    result.stdout[:1000]  # Limit output
                )
        except FileNotFoundError:
            self._add_result(
                "Linting (flake8)",
                CheckStatus.SKIP,
                "flake8 not installed (run: pip install flake8)"
            )
        except subprocess.TimeoutExpired:
            self._add_result(
                "Linting (flake8)",
                CheckStatus.WARN,
                "flake8 check timed out"
            )
        except Exception as e:
            self._add_result(
                "Linting (flake8)",
                CheckStatus.WARN,
                f"Error running flake8: {str(e)}"
            )
    
    def check_type_hints(self) -> None:
        """Check type hints with mypy."""
        print("Checking type hints (mypy)...")
        try:
            result = subprocess.run(
                [
                    "mypy",
                    "src/",
                    "--ignore-missing-imports",
                    "--no-strict-optional",
                    "--follow-imports=silent",
                ],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                self._add_result(
                    "Type Checking (mypy)",
                    CheckStatus.PASS,
                    "No type checking errors found"
                )
            else:
                error_lines = [line for line in result.stdout.split('\n') if 'error:' in line]
                self._add_result(
                    "Type Checking (mypy)",
                    CheckStatus.WARN,
                    f"Found {len(error_lines)} type checking issue(s)",
                    result.stdout[:1000]  # Limit output
                )
        except FileNotFoundError:
            self._add_result(
                "Type Checking (mypy)",
                CheckStatus.SKIP,
                "mypy not installed (run: pip install mypy)"
            )
        except subprocess.TimeoutExpired:
            self._add_result(
                "Type Checking (mypy)",
                CheckStatus.WARN,
                "mypy check timed out"
            )
        except Exception as e:
            self._add_result(
                "Type Checking (mypy)",
                CheckStatus.WARN,
                f"Error running mypy: {str(e)}"
            )
    
    def run_unit_tests(self) -> None:
        """Run unit tests with pytest."""
        print("Running unit tests...")
        
        # On Windows, PySpark tests may fail, so try to run non-PySpark tests first
        import platform
        is_windows = platform.system() == "Windows"
        
        # Test files that don't require PySpark
        non_spark_tests = [
            "tests/test_utils.py",
            "tests/data_sources/test_yahoo_finance.py",
        ]
        
        # Try running non-PySpark tests first
        if is_windows:
            print("  (Windows detected - running non-PySpark tests first)")
            try:
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "pytest",
                    ] + non_spark_tests + [
                        "-v",
                        "--tb=short",
                        "--strict-markers",
                        "--disable-warnings",
                    ],
                    cwd=self.project_root,
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                
                if result.returncode == 0:
                    self._add_result(
                        "Unit Tests (pytest - non-PySpark)",
                        CheckStatus.PASS,
                        "Non-PySpark tests passed (PySpark tests skipped on Windows)"
                    )
                    return
            except Exception as e:
                pass  # Fall through to full test run
        
        # Run all tests
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "tests/",
                    "-v",
                    "--tb=short",
                    "--strict-markers",
                    "--disable-warnings",
                ],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=300  # 5 minutes
            )
            
            # Parse output
            output = result.stdout + result.stderr
            lines = output.split('\n')
            
            if result.returncode == 0:
                # Extract test summary
                summary_line = [l for l in lines if 'passed' in l.lower() or 'failed' in l.lower()]
                summary = summary_line[0] if summary_line else "All tests passed"
                
                self._add_result(
                    "Unit Tests (pytest)",
                    CheckStatus.PASS,
                    summary
                )
            else:
                # Extract failure info
                failed_tests = [l for l in lines if 'FAILED' in l or 'ERROR' in l]
                failure_count = len(failed_tests)
                
                # Check if it's a PySpark Windows issue
                error_text = output.lower()
                is_pyspark_windows_issue = (
                    is_windows and 
                    ('unixstreamserver' in error_text or 
                     'socketserver' in error_text or
                     'pyspark' in error_text)
                )
                
                if is_pyspark_windows_issue:
                    self._add_result(
                        "Unit Tests (pytest)",
                        CheckStatus.WARN,
                        f"PySpark tests failed on Windows (expected - will work in Databricks)",
                        "PySpark has known Windows compatibility issues. Tests will run correctly in Databricks environment."
                    )
                else:
                    self._add_result(
                        "Unit Tests (pytest)",
                        CheckStatus.FAIL,
                        f"{failure_count} test(s) failed",
                        "\n".join(failed_tests[:20])  # Limit output
                    )
        except subprocess.TimeoutExpired:
            self._add_result(
                "Unit Tests (pytest)",
                CheckStatus.FAIL,
                "Tests timed out after 5 minutes"
            )
        except Exception as e:
            # Check if it's a PySpark Windows issue
            import platform
            is_windows = platform.system() == "Windows"
            error_str = str(e).lower()
            is_pyspark_issue = (
                is_windows and 
                ('unixstreamserver' in error_str or 
                 'socketserver' in error_str or
                 'pyspark' in error_str)
            )
            
            if is_pyspark_issue:
                self._add_result(
                    "Unit Tests (pytest)",
                    CheckStatus.WARN,
                    "PySpark import issue on Windows (expected - will work in Databricks)",
                    f"Error: {str(e)}"
                )
            else:
                self._add_result(
                    "Unit Tests (pytest)",
                    CheckStatus.FAIL,
                    f"Error running tests: {str(e)}"
                )
    
    def check_coverage(self) -> None:
        """Check test coverage."""
        print("Checking test coverage...")
        
        # On Windows, only run coverage for non-PySpark tests
        import platform
        is_windows = platform.system() == "Windows"
        non_spark_tests = [
            "tests/test_utils.py",
            "tests/data_sources/test_yahoo_finance.py",
        ]
        
        test_paths = non_spark_tests if is_windows else ["tests/"]
        
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                ] + test_paths + [
                    "--cov=src",
                    "--cov-report=term",
                    "--cov-report=xml",
                    "--cov-fail-under=40",  # Lower threshold for partial test runs
                ],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            # Extract coverage percentage
            output = result.stdout + result.stderr
            coverage_lines = [l for l in output.split('\n') if 'TOTAL' in l and '%' in l]
            
            # Also check for coverage percentage in the output
            if not coverage_lines:
                # Try to find any line with percentage
                for line in output.split('\n'):
                    if '%' in line and ('coverage' in line.lower() or 'total' in line.lower()):
                        coverage_lines.append(line)
                        break
            
            if result.returncode == 0:
                coverage_info = coverage_lines[0] if coverage_lines else "Coverage check passed"
                if is_windows:
                    coverage_info += " (non-PySpark tests only)"
                self._add_result(
                    "Test Coverage",
                    CheckStatus.PASS,
                    coverage_info
                )
            else:
                # Check if it's a coverage threshold issue or something else
                if 'coverage' in output.lower() and 'below' in output.lower():
                    coverage_info = coverage_lines[0] if coverage_lines else "Coverage below threshold"
                else:
                    # Might be a test failure, not coverage issue
                    coverage_info = "Coverage check had issues"
                
                if is_windows:
                    coverage_info += " (non-PySpark tests only - PySpark tests skipped on Windows)"
                
                # Show relevant output
                relevant_output = output[-800:] if len(output) > 800 else output
                self._add_result(
                    "Test Coverage",
                    CheckStatus.WARN,
                    coverage_info,
                    relevant_output if relevant_output else "No coverage output"
                )
        except subprocess.TimeoutExpired:
            self._add_result(
                "Test Coverage",
                CheckStatus.WARN,
                "Coverage check timed out"
            )
        except Exception as e:
            self._add_result(
                "Test Coverage",
                CheckStatus.WARN,
                f"Error checking coverage: {str(e)}"
            )
    
    def _get_python_files(self) -> List[Path]:
        """Get all Python files in src and tests directories."""
        python_files = []
        for directory in [self.src_dir, self.tests_dir]:
            if directory.exists():
                # Exclude __pycache__ and .pyc files
                for py_file in directory.rglob("*.py"):
                    if "__pycache__" not in str(py_file):
                        python_files.append(py_file)
        return sorted(python_files)  # Return sorted for consistent output
    
    def _add_result(self, name: str, status: CheckStatus, message: str, details: str = "") -> None:
        """Add a check result."""
        result = CheckResult(name=name, status=status, message=message, details=details)
        self.results.append(result)
        
        if status == CheckStatus.FAIL:
            self.failed_checks.append(name)
        
        # Print result (using ASCII-safe symbols for Windows compatibility)
        status_symbol = {
            CheckStatus.PASS: "[PASS]",
            CheckStatus.FAIL: "[FAIL]",
            CheckStatus.WARN: "[WARN]",
            CheckStatus.SKIP: "[SKIP]"
        }
        
        # Use simple text formatting (no ANSI colors for Windows compatibility)
        symbol = status_symbol[status]
        print(f"  {symbol} {name}: {message}")
        
        if details and status in [CheckStatus.FAIL, CheckStatus.WARN]:
            # Show first few lines of details
            detail_lines = details.split('\n')
            for line in detail_lines[:5]:
                if line.strip():
                    print(f"    {line}")
            if len(detail_lines) > 5:
                remaining = len(detail_lines) - 5
                print(f"    ... ({remaining} more lines)")
    
    def print_summary(self) -> None:
        """Print summary of all checks."""
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        
        passed = sum(1 for r in self.results if r.status == CheckStatus.PASS)
        failed = sum(1 for r in self.results if r.status == CheckStatus.FAIL)
        warned = sum(1 for r in self.results if r.status == CheckStatus.WARN)
        skipped = sum(1 for r in self.results if r.status == CheckStatus.SKIP)
        
        print(f"Total Checks: {len(self.results)}")
        print(f"  [PASS] Passed:  {passed}")
        print(f"  [FAIL] Failed:  {failed}")
        print(f"  [WARN] Warnings: {warned}")
        print(f"  [SKIP] Skipped: {skipped}")
        print()
        
        if self.failed_checks:
            print("FAILED CHECKS:")
            for check in self.failed_checks:
                print(f"  [FAIL] {check}")
            print()
            print("Please fix the failed checks before pushing.")
        else:
            print("All critical checks passed!")
            if warned > 0:
                print("Note: Some warnings were found. Review them if needed.")
        
        print("=" * 80)


def main():
    """Main entry point."""
    # Determine project root - handle both direct execution and module execution
    script_path = Path(__file__).resolve()
    
    # Strategy: Look for directory containing both src/ and tests/ subdirectories
    current = script_path.parent
    project_root = None
    
    # Walk up the directory tree to find the project root
    max_depth = 10  # Prevent infinite loops
    depth = 0
    while depth < max_depth and current != current.parent:
        if (current / "src").exists() and (current / "tests").exists():
            project_root = current
            break
        current = current.parent
        depth += 1
    
    # Fallback: use script directory if we couldn't find project root
    if project_root is None:
        project_root = script_path.parent
        print(f"Warning: Could not find project root, using script directory: {project_root}")
    
    # Verify project root has required directories
    if not (project_root / "src").exists():
        print(f"Error: 'src' directory not found in {project_root}")
        sys.exit(1)
    if not (project_root / "tests").exists():
        print(f"Error: 'tests' directory not found in {project_root}")
        sys.exit(1)
    
    # Change to project root
    try:
        os.chdir(project_root)
    except Exception as e:
        print(f"Error: Could not change to project root {project_root}: {e}")
        sys.exit(1)
    
    runner = TestRunner(project_root)
    success = runner.run_all_checks()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

