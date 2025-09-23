import os
import json
import argparse
from databricks.sdk import WorkspaceClient
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DashboardBackup:
    def __init__(self, environment):
        self.environment = environment
        self.client = WorkspaceClient(
            host=os.getenv('DATABRICKS_HOST'),
            client_id=os.getenv('DATABRICKS_CLIENT_ID'),
            client_secret=os.getenv('DATABRICKS_CLIENT_SECRET')
        )
        self.backup_dir = f"backups/{environment}/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(self.backup_dir, exist_ok=True)

    def backup_dashboards(self):
        """Backup all dashboards for the environment"""
        try:
            dashboards = self.client.lakeview.list()
            backed_up_count = 0
            
            for dashboard in dashboards:
                # Check if dashboard belongs to current environment
                if f"_{self.environment}" in dashboard.display_name:
                    # Get full dashboard definition
                    full_dashboard = self.client.lakeview.get(dashboard.dashboard_id)
                    
                    # Clean filename for backup
                    safe_name = dashboard.display_name.replace(" ", "_").replace("/", "_")
                    backup_file = os.path.join(self.backup_dir, f"{safe_name}.json")
                    
                    with open(backup_file, 'w') as f:
                        json.dump({
                            'id': dashboard.dashboard_id,
                            'display_name': dashboard.display_name,
                            'serialized_dashboard': full_dashboard.serialized_dashboard,
                            'warehouse_id': full_dashboard.warehouse_id,
                            'backup_timestamp': datetime.now().isoformat()
                        }, f, indent=2)
                    
                    logger.info(f"Backed up dashboard: {dashboard.display_name}")
                    backed_up_count += 1
            
            if backed_up_count == 0:
                logger.warning(f"No dashboards found to backup for environment: {self.environment}")
            else:
                logger.info(f"Backup completed: {backed_up_count} dashboards backed up to {self.backup_dir}")
            
        except Exception as e:
            logger.error(f"Backup failed: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Backup Databricks dashboards')
    parser.add_argument('--environment', required=True, choices=['dev', 'uat', 'prod'])
    args = parser.parse_args()
    
    backup = DashboardBackup(args.environment)
    backup.backup_dashboards()

if __name__ == "__main__":
    main()
