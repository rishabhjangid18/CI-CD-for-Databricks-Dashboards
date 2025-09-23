import os
import json
import argparse
from databricks.sdk import WorkspaceClient
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DashboardTester:
    def __init__(self, environment):
        self.environment = environment
        self.client = WorkspaceClient(
            host=os.getenv('DATABRICKS_HOST'),
            client_id=os.getenv('DATABRICKS_CLIENT_ID'),
            client_secret=os.getenv('DATABRICKS_CLIENT_SECRET')
        )

    def test_dashboard_accessibility(self, dashboard_id):
        """Test if dashboard is accessible and can be loaded"""
        try:
            dashboard = self.client.lakeview.get(dashboard_id)
            logger.info(f"✓ Dashboard {dashboard_id} is accessible")
            return True
        except Exception as e:
            logger.error(f"✗ Dashboard {dashboard_id} is not accessible: {str(e)}")
            return False

    def test_query_execution(self, query_id):
        """Test if query executes successfully"""
        try:
            # Execute query and wait for results
            query_run =
