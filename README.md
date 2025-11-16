# tj_assignment

## System Architecture
The system architecture:
![Alt text here](System-Architecture.drawio.svg)

The data flow of the system as follow:
1. CSV files from source directory loaded into tmp layer table (Done)
2. The data from tmp layer table transformed and cleaned then loaded into stg layer table
3. The data from stg layer table aggregated then loaded into dw layer table
4. The data from dw layer table extracted as CSV and saved to destination directory