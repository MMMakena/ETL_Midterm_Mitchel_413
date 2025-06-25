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
- Dropped the duplicted rows.
- Filled the categorical columns `customer_name`,`product`and `region` with `Unknown`
- Filled the numerical columns `quantity` and `unit_price` with their mean.

**Enrichment**: Added a column `total_price`by multiplying `quantity` and `unit_price`.

**Structural**: converted the `order_date`to `datetime`.

**Filtering**: Filtered out the `region`row.

For `incremental_data`:
- Applied similar cleaning method.
- Added the `total_price`column
- Change `order_date` to `datetime`
- Filter out the `customer_name` column

- Made a directory `Transformed` and saved the CSV files 

### 3) Loading
Flie name : `etl_load.ipynb`
- Loaded the transformed CSV files
- Saved the transformed datasets in **Parquet** format using `pandas.to_parquet()`
- Saved into a structured directory:`/Loaded/`
  -`transformed_full.parquet`
  -`transfoemed_incremental.parquet`
- Previewed the result using: `pd.read_parquet().head()`

## Tool Used
| Tool/Libray | Use |
|----------|----------|
| Python | Programming Language |
| Pandas | Essential library for data manipulation and analysis | 
| PyArrow | Parquet file support in Pandas |
| Parquet | Columnar storage file format |
| Jupyter Notebook | Development Environment |

## How to run the Project
Follow these steps to set up and run the ETL project
**1. Clone the repository**
```bash
git clone https://github.com/MMMakena/ETL-Midterm-Mitchel-413.git
cd ETL_MIDTERM_MITCHEL_413
```

**2. Install Dependencies**
``` bash
pip install pandas pyarrow
```

**3. Place the data files**
Ensure the data files (`raw_data.csv`and `incremental_data.csv`) are located in a designated `Data` directory within the project folder.

**4. Launch Jupyter Notebook**
``` bash
jupyter notebook
```

**5. Execute notebooks**
- `etl_extract.ipynb`: Run all cells to extract data and perform initial loading.
- `etl_transform.ipynb`: Run all cells to apply data cleaning and transformations.
- `etl_load.ipynb`: Run all cells to load the final transformed data into Parquet files.


