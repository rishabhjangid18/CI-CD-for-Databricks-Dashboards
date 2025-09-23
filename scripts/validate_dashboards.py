import os
import json
import jsonschema
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Dashboard schema for validation
DASHBOARD_SCHEMA = {
    "type": "object",
    "properties": {
        "version": {"type": "string"},
        "datasets": {"type": "array"},
        "pages": {"type": "array"},
        "filters": {"type": "array"}
    },
    "additionalProperties": True
}

def validate_dashboard_file(file_path):
    """Validate a single .lvdash.json file"""
    try:
        with open(file_path, 'r') as f:
            dashboard_config = json.load(f)
        
        # Basic validation for Lakeview dashboard structure
        if not isinstance(dashboard_config, dict):
            logger.error(f"✗ Invalid dashboard structure in {file_path}: must be JSON object")
            return False
            
        # Check if it looks like a Lakeview dashboard
        if 'pages' not in dashboard_config and 'datasets' not in dashboard_config:
            logger.warning(f"⚠ {file_path} may not be a valid Lakeview dashboard format")
        
        logger.info(f"✓ Valid dashboard file: {file_path}")
        return True
        
    except json.JSONDecodeError as e:
        logger.error(f"✗ Invalid JSON in {file_path}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"✗ Error validating {file_path}: {str(e)}")
        return False

def main():
    """Validate all .lvdash.json files in root directory"""
    valid_files = 0
    total_files = 0
    
    # Check root directory for .lvdash.json files
    for file_name in os.listdir('.'):
        if file_name.endswith('.lvdash.json'):
            total_files += 1
            if validate_dashboard_file(file_name):
                valid_files += 1
    
    if total_files == 0:
        logger.warning("No .lvdash.json files found in root directory")
        return
        
    logger.info(f"Validation complete: {valid_files}/{total_files} files valid")
    
    if valid_files != total_files:
        exit(1)

if __name__ == "__main__":
    main()

---
