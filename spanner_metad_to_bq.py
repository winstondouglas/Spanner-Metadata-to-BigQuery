import os
import time
from typing import List, Dict, Any

# You will need to install these libraries:
# pip install google-cloud-spanner google-cloud-bigquery google-cloud-resourcemanager

from google.cloud import spanner
from google.cloud import spanner_admin_instance_v1
from google.cloud import spanner_admin_database_v1
from google.cloud.spanner_admin_database_v1.types import ListDatabasesRequest

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, PermissionDenied, DeadlineExceeded
# Note: Resource Manager client imports are commented out as org-level access is highly restricted.
# from google.cloud import resourcemanager_v3 as resourcemanager

# --- CONFIGURATION ---

# List of target projects where Spanner instances might exist.
# NOTE: Replace this placeholder list with the actual projects in your organization.
# You can generate this list using:
# gcloud projects list --organization="YOUR_ORG_ID" --format="value(projectId)"
TARGET_PROJECTS: List[str] = [
    "Project1",
    "Project2"
]

# BigQuery Destination Configuration
BQ_PROJECT_ID: str = os.environ.get("BQ_PROJECT_ID", "bqprojectid") # Project where the BQ dataset lives
BQ_DATASET_ID: str = "spanner_metadata"
BQ_TABLE_ID: str = "spanner_is_columns_bq"

# --- BQ SCHEMA DEFINITION ---
# Schema for the resulting BigQuery table (focusing on Columns metadata)
BQ_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("instance_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("database_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("table_catalog", "STRING"),
    bigquery.SchemaField("table_schema", "STRING"),
    bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ordinal_position", "INTEGER"),
    bigquery.SchemaField("column_default", "STRING"),
    bigquery.SchemaField("is_nullable", "STRING"),
    bigquery.SchemaField("spanner_data_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("is_generated", "STRING"),
    bigquery.SchemaField("generation_expression", "STRING"),
]

# Query to extract the desired metadata from Spanner's INFORMATION_SCHEMA
METADATA_QUERY_C: str = """
    SELECT
        table_catalog,
        table_schema,
        table_name,
        column_name,
        ordinal_position,
        column_default,
        is_nullable,
        spanner_type,
        is_generated,
        generation_expression
    FROM
        INFORMATION_SCHEMA.COLUMNS
    limit 1 
"""

METADATA_QUERY: str = """
    SELECT
        table_catalog,
        table_schema,
        table_name,
        column_name,
        ordinal_position,
        column_default,
        is_nullable,
        spanner_type,
        is_generated,
        generation_expression
    FROM
        INFORMATION_SCHEMA.COLUMNS
    WHERE
        table_schema = ''
"""

def get_spanner_metadata(
    project_id: str,
    instance_id: str,
    database_id: str
) -> List[Dict[str, Any]]:
    """Connects to a Spanner database, runs the query, and extracts metadata."""
    try:
        spanner_client = spanner.Client(project=project_id)
        instance = spanner_client.instance(instance_id)
        database = instance.database(database_id)
        
        print(f"  -> Querying database: {database_id}...")

        with database.snapshot(multi_use=True) as snapshot:
            results = snapshot.execute_sql(METADATA_QUERY_C)
            
            # Extract column names from the result set
            for row in results:
                break

            column_names = [col.name for col in results.fields]
            
            results = snapshot.execute_sql(METADATA_QUERY)
            metadata_rows = []
            for row in results:
                # Create a dictionary from column names and row values
                row_dict = dict(zip(column_names, row))
                
                # Enrich the row with context metadata (Project, Instance, Database IDs)
                row_dict['project_id'] = project_id
                row_dict['instance_id'] = instance_id
                row_dict['database_id'] = database_id
                
                # Rename the Spanner data type column to avoid conflicts if needed, 
                # and match the BQ schema field name
                row_dict['spanner_data_type'] = row_dict.pop('spanner_type')

                metadata_rows.append(row_dict)

            print(f"  -> Extracted {len(metadata_rows)} metadata rows from {database_id}.")
            return metadata_rows

    except NotFound:
        print(f"  -> Skipped: Instance or database not found in project {project_id}.")
    except PermissionDenied:
        print(f"  -> Skipped: Permission denied for Spanner API in project {project_id}.")
    except Exception as e:
        print(f"  -> An error occurred while processing {project_id}/{instance_id}/{database_id}: {e}")
    return []

def list_spanner_resources(project_id: str) -> List[Dict[str, str]]:
    """Lists all instances and databases in a given project."""
    resources = []
    project_id2 = f"projects/{project_id}"
    try:
        #spanner_client = spanner.Client(project=project_id)
        spanner_client = spanner_admin_instance_v1.InstanceAdminClient()
        database_client = spanner_admin_database_v1.DatabaseAdminClient()

        instances = spanner_client.list_instances(parent=project_id2)
        
        for instance in instances:
            print(f"Instance 1 in instance '{instance.name}':")
            parent = database_client.instance_path(project_id, instance.name)
            request = ListDatabasesRequest(parent=instance.name)
            #databases = instance.list_databases()
            databases = database_client.list_databases(request=request)
            for db in databases:
                # We only care about regional/multi-regional databases, not backups
                print(f"Instance in instance '{instance.name}':")
                print(f"Databases in instance '{db.name}':")
                if '/' in db.name and 'backups' not in db.name:
                    resources.append({
                       # "instance_id": instance.name,
                       "instance_id": instance.name.split('/')[-1],
                       # "database_id": db.name
                       "database_id": db.name.split('/')[-1]
                    })
        
        return resources
        
    except NotFound:
        print(f"Project {project_id} not found.")
    except PermissionDenied:
        print(f"Permission denied to list Spanner resources in project {project_id}.")
    except Exception as e:
        print(f"Error listing resources in {project_id}: {e}")
        
    return resources

def setup_bigquery_table(client: bigquery.Client):
    """Ensures the BQ dataset and table exist."""
    dataset_ref = client.dataset(BQ_DATASET_ID)
    table_ref = dataset_ref.table(BQ_TABLE_ID)

    try:
        # Check if the dataset exists
        client.get_dataset(dataset_ref)
    except NotFound:
        print(f"Creating BigQuery Dataset: {BQ_DATASET_ID}")
        client.create_dataset(dataset_ref)

    try:
        # Check if the table exists
        client.get_table(table_ref)
        # If it exists, delete all rows to prepare for fresh load (optional, but cleaner)
        print(f"Clearing existing data from table: {BQ_TABLE_ID}")
        client.query(f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`").result()
    except NotFound:
        # If the table doesn't exist, create it
        print(f"Creating BigQuery Table: {BQ_TABLE_ID}")
        table = bigquery.Table(table_ref, schema=BQ_SCHEMA)
        client.create_table(table)


def main():
    """Main function to orchestrate the discovery, extraction, and loading process."""
    print("--- Starting Spanner Metadata Extraction ---")
    
    # 1. Initialize BigQuery Client and set up the destination table
    bq_client = bigquery.Client(project=BQ_PROJECT_ID)
    setup_bigquery_table(bq_client)

    all_metadata_rows: List[Dict[str, Any]] = []

    # 2. Iterate through projects and discover Spanner resources
    total_projects = len(TARGET_PROJECTS)
    
    for i, project_id in enumerate(TARGET_PROJECTS):
        print(f"\n[{i + 1}/{total_projects}] Processing Project: {project_id}")
        
        # Get list of (instance, database) pairs in the current project
        spanner_resources = list_spanner_resources(project_id)
        
        if not spanner_resources:
            print(f"  -> No Spanner databases found or accessible in {project_id}. Skipping.")
            continue
            
        print(f"  -> Found {len(spanner_resources)} databases to process.")

        # 3. Extract metadata from each database
        for resource in spanner_resources:
            rows = get_spanner_metadata(
                project_id,
                resource['instance_id'],
                resource['database_id']
            )
            all_metadata_rows.extend(rows)

        # 4. Batch load to BQ every 5 projects (or more frequently if data volume is high)
        if (i + 1) % 5 == 0 or (i + 1) == total_projects:
            if all_metadata_rows:
                print(f"\n--- Loading {len(all_metadata_rows)} rows to BigQuery ---")
                table_ref = bq_client.dataset(BQ_DATASET_ID).table(BQ_TABLE_ID)
                
                # Insert the rows into BigQuery
                errors = bq_client.insert_rows_json(table_ref, all_metadata_rows)
                
                if errors:
                    print(f"Error during BQ insertion: {errors}")
                else:
                    print("Successfully loaded batch to BigQuery.")
                
                # Clear the batch buffer
                all_metadata_rows = []
            else:
                print("No new metadata to load in this batch.")


if __name__ == "__main__":
    # Ensure you are authenticated (e.g., gcloud auth application-default login)
    # and have permissions for Resource Manager, Spanner, and BigQuery across all projects.
    main()
