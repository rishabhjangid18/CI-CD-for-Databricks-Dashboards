import os
import json
import argparse
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DashboardDeployer:
    def __init__(self, environment):
        self.environment = environment
        self.client = WorkspaceClient(
            host=os.getenv(f'DATABRICKS_HOST'),
            client_id=os.getenv(f'DATABRICKS_CLIENT_ID'),
            client_secret=os.getenv(f'DATABRICKS_CLIENT_SECRET')
        )
        
    def get_dashboard_files(self):
        """Get all .lvdash.json files from root directory"""
        dashboard_files = []
        for file in os.listdir('.'):
            if file.endswith('.lvdash.json'):
                dashboard_files.append(file)
        return dashboard_files
        
    def parse_dashboard_file(self, file_path):
        """Parse Databricks Lakeview dashboard file"""
        with open(file_path, 'r') as f:
            dashboard_content = json.load(f)
        
        # Extract dashboard name from filename
        dashboard_name = os.path.basename(file_path).replace('.lvdash.json', '')
        
        return {
            'name': dashboard_name,
            'content': dashboard_content,
            'file_path': file_path
        }
        
    def deploy_dashboards(self):
        """Deploy dashboards to Databricks"""
        dashboard_files = self.get_dashboard_files()
        
        if not dashboard_files:
            logger.info("No .lvdash.json files found in root directory")
            return
            
        for dashboard_file in dashboard_files:
            try:
                dashboard_data = self.parse_dashboard_file(dashboard_file)
                dashboard_name = dashboard_data['name']
                dashboard_content = dashboard_data['content']
                
                # Add environment suffix to dashboard name
                env_dashboard_name = f"{dashboard_name}_{self.environment}"
                
                # Get warehouse_id from environment or use default
                warehouse_id = self.get_warehouse_id_for_env()
                
                # Create or update dashboard using Lakeview API
                dashboard = self.client.lakeview.create(
                    display_name=env_dashboard_name,
                    serialized_dashboard=json.dumps(dashboard_content),
                    warehouse_id=warehouse_id
                )
                
                logger.info(f"Deployed dashboard: {env_dashboard_name} (ID: {dashboard.dashboard_id})")
                
                # Set default permissions for the environment
                self.set_default_permissions(dashboard.dashboard_id)
                
            except Exception as e:
                logger.error(f"Failed to deploy dashboard {dashboard_file}: {str(e)}")
                # Continue with other dashboards instead of failing completely
                continue
                
    def get_warehouse_id_for_env(self):
        """Get warehouse ID based on environment"""
        warehouse_mapping = {
            'dev': os.getenv('DEV_WAREHOUSE_ID', 'default-dev-warehouse'),
            'uat': os.getenv('UAT_WAREHOUSE_ID', 'default-uat-warehouse'),
            'prod': os.getenv('PROD_WAREHOUSE_ID', 'default-prod-warehouse')
        }
        return warehouse_mapping.get(self.environment, 'default-warehouse')
        
    def set_default_permissions(self, dashboard_id):
        """Set default permissions based on environment"""
        try:
            # Default permissions per environment
            permission_mapping = {
                'dev': [
                    {'group_name': 'developers', 'permission_level': 'CAN_MANAGE'},
                    {'group_name': 'analysts', 'permission_level': 'CAN_VIEW'}
                ],
                'uat': [
                    {'group_name': 'testers', 'permission_level': 'CAN_RUN'},
                    {'group_name': 'business_users', 'permission_level': 'CAN_VIEW'}
                ],
                'prod': [
                    {'group_name': 'business_users', 'permission_level': 'CAN_VIEW'},
                    {'group_name': 'dashboard_admins', 'permission_level': 'CAN_MANAGE'}
                ]
            }
            
            permissions = permission_mapping.get(self.environment, [])
            if permissions:
                from databricks.sdk.service import sql
                access_control_list = []
                
                for perm in permissions:
                    access_control_list.append(
                        sql.AccessControl(
                            group_name=perm.get('group_name'),
                            permission_level=sql.PermissionLevel(perm.get('permission_level', 'CAN_VIEW'))
                        )
                    )
                
                self.client.dashboard_permissions.set(
                    dashboard_id=dashboard_id,
                    access_control_list=access_control_list
                )
                logger.info(f"Set permissions for dashboard {dashboard_id}")
                
        except Exception as e:
            logger.warning(f"Could not set permissions for dashboard {dashboard_id}: {str(e)}")
            # Don't fail deployment if permissions can't be set

def main():
    parser = argparse.ArgumentParser(description='Deploy Databricks dashboards')
    parser.add_argument('--environment', required=True, choices=['dev', 'uat', 'prod'])
    args = parser.parse_args()
    
    deployer = DashboardDeployer(args.environment)
    
    # Deploy dashboards from root directory
    deployer.deploy_dashboards()
    
    logger.info(f"Deployment to {args.environment} completed")

if __name__ == "__main__":
    main()

---
