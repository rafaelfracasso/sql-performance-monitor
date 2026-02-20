#!/usr/bin/env python3
"""
Script to generate config/databases.json from .env file.
Reads environment variables and creates a configuration file for the monitor.
"""
import json
import os
from pathlib import Path
from dotenv import load_dotenv

def generate_config():
    # Load environment variables from .env file
    env_path = Path('.') / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print(f"Loaded environment variables from {env_path}")
    else:
        print(f"Warning: {env_path} not found. Using current environment variables.")

    databases = []

    # SQL Server
    if os.getenv('SQL_SERVER'):
        print("Found SQL Server configuration in .env")
        db_config = {
            "name": "SQL Server (From .env)",
            "type": "SQLSERVER",
            "enabled": True,
            "credentials": {
                "server": os.getenv('SQL_SERVER'),
                "port": str(os.getenv('SQL_PORT', 1433)),
                "database": os.getenv('SQL_DATABASE', 'master'),
                "username": os.getenv('SQL_USERNAME'),
                "driver": os.getenv('SQL_DRIVER', 'ODBC Driver 18 for SQL Server'),
                "trust_server_certificate": True
            }
        }
        # Use variable reference for password if it exists in env, otherwise leave empty or literal
        if os.getenv('SQL_PASSWORD'):
            db_config['credentials']['password'] = "${SQL_PASSWORD}"
        
        databases.append(db_config)
    
    # PostgreSQL
    if os.getenv('PG_SERVER'):
        print("Found PostgreSQL configuration in .env")
        db_config = {
            "name": "PostgreSQL (From .env)",
            "type": "POSTGRESQL",
            "enabled": True,
            "credentials": {
                "server": os.getenv('PG_SERVER'),
                "port": str(os.getenv('PG_PORT', 5432)),
                "database": os.getenv('PG_DATABASE', 'postgres'),
                "username": os.getenv('PG_USERNAME')
            }
        }
        if os.getenv('PG_PASSWORD'):
            db_config['credentials']['password'] = "${PG_PASSWORD}"
            
        databases.append(db_config)

    # HANA
    if os.getenv('HANA_SERVER'):
        print("Found HANA configuration in .env")
        db_config = {
            "name": "SAP HANA (From .env)",
            "type": "HANA",
            "enabled": True,
            "credentials": {
                "server": os.getenv('HANA_SERVER'),
                "port": str(os.getenv('HANA_PORT', 30015)),
                "database": os.getenv('HANA_DATABASE', 'SYSTEMDB'),
                "username": os.getenv('HANA_USERNAME')
            }
        }
        if os.getenv('HANA_PASSWORD'):
            db_config['credentials']['password'] = "${HANA_PASSWORD}"
            
        databases.append(db_config)

    if not databases:
        print("No database configuration found in .env (looking for SQL_SERVER, PG_SERVER, HANA_SERVER).")
        return

    config_data = {
        "databases": databases,
        "_generated_by": "scripts/generate_config_from_env.py",
        "_comments": {
            "security": "Passwords referenced as ${VAR} are loaded from environment variables at runtime."
        }
    }

    output_path = Path('config/databases.json')
    
    # Ensure config directory exists
    output_path.parent.mkdir(exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(config_data, f, indent=2)
    
    print(f"Successfully generated {output_path} with {len(databases)} database(s).")
    print("Verify the content of the generated file to ensure it is correct.")

if __name__ == "__main__":
    generate_config()
