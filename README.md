# Delta Live Table (DLT) & Change Data Capture (CDC)

DLT architecture centers around a net-new philosophy. The building blocks of the DLT framework are streaming tables and materialized views. Streaming tables are append-only tables; if a row is created, it cannot be modified. Materialized views contain the results of queries; rows are refreshed to showcase updates from the latest data at each pipeline run.

To set up a data pipeline, you only need to specify how to move the data around - DLT will figure out dependencies and optimization strategies for you under the hood. At each run, pipeline will take into account only the net new information. The net new ingestion and transformation happen in the backend so there's no need for developers to build custom code to optimize the data flow. To illustrate the net-new framework:

To ingest raw data, we use the autoloader feature, which enables the pipeline to scan for net-new information in a cloud storage container, ingest it, and leave the existing information untouched.

To transform, aggregate, and join data from different sources, we create materialized views. Each time we run the pipeline to refresh a view, pipeline analyzes the net-new information from the upstream, comes up with approaches to integrating the information to the target table, and uses the most efficient approach.

Of course, there's the CDC feature.

### How CDC fits into the architecture

One of the goals of CDC is to efficiently retrieve the historical state of a table at any given point. It tracks changes to the table, allowing downstream pipelines to query only the changes rather than the entire table, thereby optimizing further transformations.

DLT, following the net-new framework, functions akin to a substantial CDC engine itself. Given that streaming tables and materialized views are optimized to incorporate only new information into the downstream flow, it is unnecessary to explicitly implement a CDC machine to make the pipeline more efficient.

Nonetheless, CDC is still valuable for tracking the historical evolution of a table. Implementing it at upstream layers, such as bronze to silver, is preferable over downstream layers like silver to gold. This choice is due to the nature of the APPLY_CHANGES API in the DLT framework, which creates a target streaming table from a source streaming table. Since streaming tables are append-only, this design ensures that new data streams update the existing "source of truth" from the same data source. However, it's impractical to use this API for transformed or aggregated data from multiple sources, as identifying changes becomes complex in an append-only framework. Databricks reinforces this architecture by mandating that every table using the target of an APPLY_CHANGES API must be a materialized view. Creating an append-only table from a source table with modified rows is considered malpractice.

### Best practices for data pipelines, as I see it

Firstly, we begin by ingesting raw data as streaming tables. All incoming data is directly appended to the data warehouse in its original form by the respective data sources.

Secondly, we implement a CDC layer to track the historical progression of the raw table. We can dynamically apply filters to capture specific snapshots of the table at defined time intervals for downstream transformation purposes.

Thirdly, we execute transformations, aggregations, and joins to generate materialized views tailored to meet business requirements. With each refresh, these materialized views incorporate "net new" information from the source tables and seamlessly integrate it into the final view.

### Running the demo

*Disclaimer: the demo uses Azure Databricks.*

This demo aims to illustrate the following features of DLT:
- Autoloading for full & incremental loads
- Change data capture
- DLT pipelines

First, run `notebooks/1_ingest_mock_full_load.py` to ingest an initial load of data to hive metastore.

Second, create a DLT pipeline for `mock_data_pipeline.py`. This notebook specifies the structure of the data flow (as well as dependencies of tables). Note that you can't populate any tables by running the notebook; tables are only created and modified via pipeline runs.

Run the DLT pipeline, and examine the result of the initial load using `notebooks/2_result_from_initial_pipeline_run.py`.

Next, run `notebooks/3_create_mock_incremental_load.py` to ingest a subsequent load of data to the same directories that contain the initial load.

Run the DLT pipeline again, and examine the result of the subsequent load using `notebooks/4_result_from_second_pipeline_run.py`. Note how the raw and CDC tables have been updated since the initial run.
