import os
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_sql_file(file_path):
    """Basic SQL validation"""
    try:
        with open(file_path, 'r') as f:
            sql_content = f.read()
        
        # Basic checks
        if not sql_content.strip():
            logger.error(f"✗ Empty SQL file: {file_path}")
            return False
            
        # Check for basic SQL syntax issues
        if not re.search(r'\bSELECT\b', sql_content, re.IGNORECASE):
            logger.warning(f"⚠ No SELECT statement found in: {file_path}")
            
        # Check for potentially dangerous operations in non-dev environments
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER']
        for keyword in dangerous_keywords:
            if re.search(rf'\b{keyword}\b', sql_content, re.IGNORECASE):
                logger.warning(f"⚠ Potentially dangerous keyword '{keyword}' found in: {file_path}")
        
        logger.info(f"✓ Valid SQL file: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error validating {file_path}: {str(e)}")
        return False

def main():
    """Validate all SQL query files"""
    valid_files = 0
    total_files = 0
    
    # Check root directory for SQL files (if any)
    for file_name in os.listdir('.'):
        if file_name.endswith('.sql'):
            total_files += 1
            if validate_sql_file(file_name):
                valid_files += 1
    
    if total_files == 0:
        logger.info("No SQL files found in root directory - skipping SQL validation")
        return
        
    logger.info(f"SQL validation complete: {valid_files}/{total_files} files valid")
    
    if valid_files != total_files:
        exit(1)

if __name__ == "__main__":
    main()

---
