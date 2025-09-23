#!/usr/bin/env python3

import os
import json
import argparse
import logging
import time
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DashboardTester:
    def __init__(self, environment):
        self.environment = environment.lower()
        self.client = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        )
        if not self.client:
            logger.error("Databricks client not initialized. Check DATABRICKS_HOST and DATABRICKS_TOKEN.")
            raise ValueError("Databricks client not initialized")

    def get_dashboard_files(self):
        """Get all .lvdash.json files from root directory"""
        return [f for f in os.listdir('.') if f.endswith('.lvdash.json')]

    def get_dashboard_name_from_file(self, file_path):
        return os.path.basename(file_path).replace('.lvdash.json', '')

    def test_dashboard_accessibility(self, dashboard_id):
        """Test if dashboard is accessible and can be loaded"""
        try:
            dashboard = self.client.lakeview.get(dashboard_id)
            logger.info(f"✓ Dashboard {dashboard_id} is accessible")
            return True
        except Exception as e:
            logger.error(f"✗ Dashboard {dashboard_id} is not accessible: {str(e)}")
            return False

    def run_tests(self):
        dashboard_files = self.get_dashboard_files()
        if not dashboard_files:
            logger.warning("No .lvdash.json files found for testing")
            return

        all_passed = True
        for file_path in dashboard_files:
            dashboard_name = f"{self.get_dashboard_name_from_file(file_path)}_{self.environment}"

            try:
                # Search for dashboard by name
                dashboards = self.client.lakeview.list()
                matched = [d for d in dashboards if d.display_name == dashboard_name]

                if not matched:
                    logger.error(f"✗ Dashboard {dashboard_name} not found in environment {self.environment}")
                    all_passed = False
                    continue

                dashboard_id = matched[0].dashboard_id
                logger.info(f"Testing dashboard: {dashboard_name} (ID: {dashboard_id})")

                # Accessibility test
                if not self.test_dashboard_accessibility(dashboard_id):
                    all_passed = False

                # Optionally, add query execution tests if your dashboards contain queries
                # e.g., iterate over dataset queries and run them
                # For now, we skip query execution

            except Exception as e:
                logger.error(f"Error testing dashboard {dashboard_name}: {str(e)}")
                all_passed = False

        if not all_passed:
            logger.error("Some dashboard tests failed")
            exit(1)
        else:
            logger.info(f"All dashboards passed accessibility tests in {self.environment}")

def main():
    parser = argparse.ArgumentParser(description="Test Databricks dashboards")
    parser.add_argument("--environment", required=True, choices=["dev", "uat", "prod"])
    args = parser.parse_args()

    tester = DashboardTester(args.environment)
    tester.run_tests()

if __name__ == "__main__":
    main()
