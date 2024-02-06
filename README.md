# DLT architecture, with change data capture (CDC)

DLT architecture centers around a net-new philosophy. The building blocks of the DLT framework are streaming tables and materialized views. Streaming tables are append-only tables; if a row is created, it cannot be modified. Materialized views contain the results of queries; rows are refreshed to showcase updates from the latest data at each pipeline run.

To set up a data pipeline, you only need to specify how to move the data around - DLT will figure out dependencies and optimization strategies for you under the hood. At each run, pipeline will take into account only the net new information. The net new ingestion and transformation happen in the backend so there's no need for developers to build custom code to optimize the data flow. To illustrate the net-new framework:

To ingest raw data, we use the autoloader feature, which enables the pipeline to scan for net-new information in a cloud storage container, ingest it, and leave the existing information untouched.

To transform, aggregate, and join data from different sources, we create materialized views. Each time we run the pipeline to refresh a view, pipeline analyzes the net-new information from the upstream, comes up with approaches to integrating the information to the target table, and uses the most efficient approach.

Of course, there's the CDC feature.

### How does CDC fit into the architecture

One of the goals of CDC is to efficiently retrieve the historical state of a table at any given point. It tracks changes to the table, allowing downstream pipelines to query only the changes rather than the entire table, thereby optimizing further transformations.

DLT, following the net-new framework, functions akin to a substantial CDC engine itself. Given that streaming tables and materialized views are optimized to incorporate only new information into the downstream flow, it is unnecessary to explicitly implement a CDC machine to make the pipeline more efficient.

Nonetheless, CDC is still valuable for tracking the historical evolution of a table. Implementing it at upstream layers, such as bronze to silver, is preferable over downstream layers like silver to gold. This choice is due to the nature of the APPLY_CHANGES API in the DLT framework, which creates a target streaming table from a source streaming table. Since streaming tables are append-only, this design ensures that new data streams update the existing "source of truth" from the same data source. However, it's impractical to use this API for transformed or aggregated data from multiple sources, as identifying changes becomes complex in an append-only framework. Databricks reinforces this architecture by mandating that every table using the target of an APPLY_CHANGES API must be a materialized view. Creating an append-only table from a source table with modified rows is considered malpractice.

### Best practices for data pipelines, as I see it

Firstly, we begin by ingesting raw data as streaming tables. All incoming data is directly appended to the data warehouse in its original form by the respective data sources.

Secondly, we implement a CDC layer to track the historical progression of the raw table. We can dynamically apply filters to capture specific snapshots of the table at defined time intervals for downstream transformation purposes.

Thirdly, we execute transformations, aggregations, and joins to generate materialized views tailored to meet business requirements. With each refresh, these materialized views incorporate "net new" information from the source tables and seamlessly integrate it into the final view.