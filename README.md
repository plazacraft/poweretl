
# Overview
**poweretl** is library to support *etl* processes and tasks. 

# High Level Design

| Package                       | Description 
| ----------------------------- | --------------------------------------------------------------- |
| **poweretl.utils**            | Independent package for tools. |
| **poweretl.defs**             | Definition of data classes used to communication between providers and executors. Contains also interfaces for providers. |
| **poweretl.common**           | Implementation of processes (executors). |
| **poweretl.databricks**       | Implementation of providers for databricks. |

Examples of usage can be find in poweretl repostiory, project - *examples* folder *tests* (implemented as tests).

```mermaid
---
config:
  flowchart:
    htmlLabels: false
---
flowchart BT
subgraph "**poweretl**"
  direction BT
  defs("**defs**
  Contains definitions and interfaces.") 
  common("**common**
  Standard implementation of poweretl processes.")
  databricks@{shape: procs, label: "**databricks**
  Implementations of providers for databricks and other data lake/ware houses." }
  utils("`**utils**
  Independed helpers.`")  

  common -- "Uses" --> defs
  databricks -- "Uses" --> defs
end
code("**User code**
Execution of common classes with proper data ware/lake house providers.")
code -- "Used for process execution" --> common
code -- "Used for target data ware/lake house selection" --> databricks
```