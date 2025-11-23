# Tests Prerequisites

## Databricks connection
In *src/poweretl/databricks/helpers* create **.databricks.conf.json** in format: 
```json
{
  "host": "<host_address>",
  "token": "<pat_token>",
  "cluster_id": "<custer_id>"
}
```

## Databricks environment
- Create catalog: **workspace** and schema: **poweretl_tests**
- Create volume: **workspace.poweretl_tests.meta**
- Create volume: **workspace.poweretl_tests.unit_tests**
- Create volume: **workspace.poweretl_tests.model**
