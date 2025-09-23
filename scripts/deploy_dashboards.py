#!/usr/bin/env python3

import os
import json
import argparse
import logging
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DashboardDeployer:
    def __init__(self, environment):
        self.environment = environment.lower()
        self.client = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        )

    def get_dashboard_files(self):
        return [f for f in os.listdir('.') if f.endswith('.lvdash.json')]

    def parse_dashboard_file(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            dashboard_content = json.load(f)
        dashboard_name = os.path.basename(file_path).replace('.lvdash.json', '')
        return {'name': dashboard_name, 'content': dashboard_content}

    def get_warehouse_id_for_env(self):
        warehouse_mapping = {
            'dev': os.getenv('DEV_WAREHOUSE_ID', 'default-dev-warehouse'),
            'uat': os.getenv('UAT_WAREHOUSE_ID', 'default-uat-warehouse'),
            'prod': os.getenv('PROD_WAREHOUSE_ID', 'default-prod-warehouse')
        }
        return warehouse_mapping.get(self.environment, 'default-warehouse')

    def deploy_dashboards(self):
        dashboard_files = self.get_dashboard_files()
        if not dashboard_files:
            logger.info("No .lvdash.json files found")
            return

        for file in dashboard_files:
            try:
                data = self.parse_dashboard_file(file)
                env_name = f"{data['name']}_{self.environment}"
                warehouse_id = self.get_warehouse_id_for_env()

                # Correct SDK call
                dashboard = self.client.lakeview.create_dashboard(
                    name=env_name,
                    serialized_dashboard=json.dumps(data['content']),
                    warehouse_id=warehouse_id
                )

                logger.info(f"Deployed dashboard: {env_name} (ID: {dashboard.id})")

            except Exception as e:
                logger.error(f"Failed to deploy {file}: {e}")
                continue

def main():
    parser = argparse.ArgumentParser(description="Deploy Databricks dashboards")
    parser.add_argument('--environment', required=True, choices=['dev', 'uat', 'prod'])
    args = parser.parse_args()

    deployer = DashboardDeployer(args.environment)
    deployer.deploy_dashboards()
    logger.info(f"Deployment to {args.environment.upper()} completed")

if __name__ == "__main__":
    main()
