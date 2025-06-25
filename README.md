# ETL_Midterm_Mitchel_413
Project that show full ETL pipeline

## Project Overview
An entire ETL (Extract, Transform, Load) pipeline for sales order data is demonstrated in this project.  Gathering raw sales data from multiple sources (CSV files), cleaning and enriching it, and then loading the refined data into an effective storage format (Parquet files) so it can be further analysed and reported on are the main objectives.

The pipeline involves:
- Obtaining incremental and raw datasets from CSV files.
-  Data cleansing, enrichment, filtering, and or structuring.
- For effective storage and querying, the converted data should be saved in Parquet format.

The output include two optimized datasets:
- `transformed_full.paraquet` - based on historic full data
- `transformed_incremental.paraquet`- based on newly added data

## ETL Phases
### 1) Extraction
File name : `etl_extraction.ipynb`

- Loaded both the `raw_data.csv` and `incremental_data.csv` datasets provide using **Pandas**
- Used `.head()` to display the first few records and `.info()` to get information on the two datasets.
- Made observations on the data,found 88 and 12 missing values on raw data and incremental respectively. Found 1 in raw data and 0 in incremental data duplicates. Using `.isnull()`,`.duplicated()` and `.sum()` functions.
- Made a directory `Data` and save both data there.

### 2) Transformation
File name: `etl_transform.ipynb`
For `raw_data`:

**Cleaning**:
- 