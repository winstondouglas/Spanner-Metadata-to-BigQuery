This python program was created to load the Metadata (INFORMATION_SCHEMA) for Spanner databases across multiple projects into a central dataset/table in BQ for query processing. 
This is a quick easy customizable solution. For a more Scalable solution consider Google Cloud Dataplex Univeral Catalog .
In cases where there are many Spanner Instances in an Org this provides a solution to be able to query the Metadata in a central location. 
This will save time locating and connecting to the right database. 
It can potentially be used as a baseline to lookup and compare schemas across multiple Spanner databases in the absence of 3rd party vendor tooling (eg: Liquibase) that could be used for version control and schema comparisons. 
Google Cloud Dataplex Univeral Catalog is a scalable automated service that can be used as a central metadata solution.

**Running the Python Program:
**     - Use Python3 to execute the code in a Python environment with the Spanner and BQ libraries.
     - Before executing ensure the service account has access to all the target projects
