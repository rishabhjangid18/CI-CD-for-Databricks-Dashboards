#!/usr/bin/env python3

import os
import sys
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_dashboard_file(file_path):
    """Validate a single .lvdash.json file"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            dashboard_config = json.load(f)

        # Basic validation for Lakeview dashboard structure
        if not isinstance(dashboard_config, dict):
            logger.error(f"✗ Invalid structure in {file_path}: must be a JSON object")
            return False

        # Check if it looks like a Lakeview dashboard
        if "pages" not in dashboard_config and "datasets" not in dashboard_config:
            logger.warning(f"⚠ {file_path} may not match Lakeview dashboard format")

        logger.info(f"✓ Valid dashboard file: {file_path}")
        return True

    except json.JSONDecodeError as e:
        logger.error(f"✗ Invalid JSON in {file_path}: {str(e)}")
        return False
    except FileNotFoundError:
        logger.error(f"✗ File not found: {file_path}")
        return False
    except Exception as e:
        logger.error(f"✗ Error validating {file_path}: {str(e)}")
        return False


def main():
    """Validate all .lvdash.json files in root directory"""
    valid_files = 0
    total_files = 0

    try:
        current_dir = os.getcwd()
        logger.info(f"Checking directory: {current_dir}")

        for file_name in os.listdir("."):
            if file_name.endswith(".lvdash.json"):
                total_files += 1
                logger.info(f"Found dashboard file: {file_name}")
                if validate_dashboard_file(file_name):
                    valid_files += 1

        if total_files == 0:
            logger.warning("⚠ No .lvdash.json files found in root directory")
            all_files = [f for f in os.listdir(".") if os.path.isfile(f)]
            logger.info(f"Files in directory: {all_files}")
            sys.exit(0)  # Exit gracefully if no files found

        logger.info(f"Validation complete: {valid_files}/{total_files} files valid")

        if valid_files != total_files:
            sys.exit(1)

    except Exception as e:
        logger.error(f"✗ Error during validation: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
