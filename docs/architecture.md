# CSV Generation Architecture

The system follows a "Metadata-Driven" approach. Instead of hardcoding columns, it reads the variable definitions and builds the export query at runtime.



### Data Lineage
```mermaid
graph TD
    Raw[Log Buffers] --> Factory[DATA_PROCESS_VIEWS]
    Factory --> Staging[(PROCESS_RESULTS)]
    Staging --> Write{CSV_FILES.write_file}
    Write --> TSV[[TSV File on Server]]
    
    subgraph Cleanup
    TSV --> Delete[Delete_job > 181 Days]
    end
