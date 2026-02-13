# Superox-CSV-Generation
This repository contains the logic for extracting industrial process data and generating physical files (TSV/CSV) on the S.OX file server.

## ⚙️ Core Components
1. **The Dispatcher (`CSV_FILES.initial_job`):** Monitors the `run_journal` and triggers exports for new or updated runs.
2. **The Factory (`DATA_PROCESS_VIEWS`):** A meta-programming view that generates dynamic SQL based on process variables.
3. **The Engine (`CSV_FILES.write_file`):** Uses Oracle `UTL_FILE` to write data to the `CSV_DIR` path on the server.
4. **The Utility (`DUMP_TABLE_TO_CSV`):** A generic procedure to export any database table to a comma-separated file.

## 🛠 Prerequisites
- **Oracle Directory:** Must have an existing directory object named `CSV_DIR`.
- **Server Access:** The database server must have write permissions to the physical path mapped to `CSV_DIR` (e.g., `10.177.3.63/EXPORT/`).
