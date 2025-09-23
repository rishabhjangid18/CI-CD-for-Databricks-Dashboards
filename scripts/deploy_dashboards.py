
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
        if not self.client:
            logger.error("Databricks client not initialized. Check DATABRICKS_HOST and DATABRICKS_TOKEN.")
            raise ValueError("Databricks client not initialized")

    def get_dashboard_files(self):
        """Get all .lvdash.json files from the root directory"""
        return [f for f in os.listdir('.') if f.endswith('.lvdash.json')]

    def parse_dashboard_file(self, file_path):
        """Parse Databricks Lakeview dashboard file"""
        with open(file_path, 'r', encoding='utf-8') as f:
            dashboard_content = json.load(f)
        dashboard_name = os.path.basename(file_path).replace('.lvdash.json', '')
        return {
            'name': dashboard_name,
            'content': dashboard_content,
            'file_path': file_path
        }

    def get_warehouse_id_for_env(self):
        """Get warehouse ID based on environment"""
        warehouse_mapping = {
            'dev': os.getenv('DEV_WAREHOUSE_ID', 'default-dev-warehouse'),
            'uat': os.getenv('UAT_WAREHOUSE_ID', 'default-uat-warehouse'),
            'prod': os.getenv('PROD_WAREHOUSE_ID', 'default-prod-warehouse')
        }
        return warehouse_mapping.get(self.environment, 'default-warehouse')

    def set_default_permissions(self, dashboard_id):
        """Set default permissions - optional in CI/CD"""
        try:
            logger.info(f"Skipping permission setup for dashboard {dashboard_id} in CI/CD environment")
        except Exception as e:
            logger.warning(f"Failed to set permissions for dashboard {dashboard_id}: {str(e)}")

    def deploy_dashboards(self):
        """Deploy dashboards to Databricks"""
        dashboard_files = self.get_dashboard_files()
        if not dashboard_files:
            logger.info("No .lvdash.json files found in root directory")
            return

        for dashboard_file in dashboard_files:
            try:
                dashboard_data = self.parse_dashboard_file(dashboard_file)
                env_dashboard_name = f"{dashboard_data['name']}_{self.environment}"
                warehouse_id = self.get_warehouse_id_for_env()

                # Corrected: use `name` instead of `display_name` and proper API path
                dashboard = self.client.lakeview.dashboards.create(
                    name=env_dashboard_name,
                    serialized_dashboard=json.dumps(dashboard_data['content']),
                    warehouse_id=warehouse_id
                )
                logger.info(f"Deployed dashboard: {env_dashboard_name} (ID: {dashboard.id})")

                # Optional: set permissions
                self.set_default_permissions(dashboard.id)

            except Exception as e:
                logger.error(f"Failed to deploy {dashboard_file}: {str(e)}")
                continue


def main():
    parser = argparse.ArgumentParser(description='Deploy Databricks dashboards')
    parser.add_argument('--environment', required=True, choices=['dev', 'uat', 'prod'], help='Target environment')
    args = parser.parse_args()

    deployer = DashboardDeployer(args.environment)
    deployer.deploy_dashboards()
    logger.info(f"Deployment to {args.environment.upper()} completed successfully")


if __name__ == "__main__":
    main()
