## ETL Pipeline for Airbnb Data

### Overview

This project is an ETL (Extract, Transform, Load) pipeline designed to process Airbnb data. The pipeline includes steps to:

1. Ingest data from a CSV file within a ZIP archive.
2. Load the data into a PostgreSQL database.
3. Normalize the data into structured tables.
4. Transform the data to calculate average prices, identify top listings, and generate heatmaps.

### Prerequisites
Before you run the ETL pipeline, make sure you have the following installed:

1. Python 3.7 or later
2. PostgreSQL
3. Pip (Python package installer)

### Setup
Follow these steps to set up the project:

1. **Clone the respository**

   ```
   git clone https://your-repository-url.git
   cd your-repository-directory
   ```

2. **Create a virtual environment**

   ```
   python -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**

   ```
   pip install -r requirements.txt
   ```

4. **Setup PostgreSQL**

  sql
  ```
   CREATE DATABASE create_your_own_db;
   CREATE USER create_user WITH ENCRYPTED PASSWORD 'create_your_password';
   GRANT ALL PRIVILEGES ON DATABASE create_your_own_db TO create_user;
   ```

   python
   ```
   self.database_name = "create_your_own_db"
   self.user = "create_user"
   self.password = "create_your_password"
   self.host = "localhost"
   self.port = "5432"
   ```

5. **Prepare the ZIP File**

  Place the ZIP file (kaggle_download.zip) containing the CSV file in the same directory as the script. Ensure the
  CSV file is the first entry in the ZIP archive.

### Running the ETL Pipeline

To run the ETL pipeline, use the following command:

```
python etl_file.py run
```
### File Outputs

1. Excel File: he pipeline will generate an avg_prices.xlsx file with the following sheets:
   a) avg_price: Average price per neighborhood.
   b) avg_price_group: Average price per neighborhood group.
   c) top_listings: Top 3 most expensive listings by neighborhood group and room type.
   d) pop_listings: Top 3 listings scaled by popularity within each neighborhood group.

2. PNG File: The pipeline will create a heatmap.png showing the average price of Airbnb listings over NYC.

### Troubleshooting

1. Database Connection Issues: Ensure that PostgreSQL is running and that the connection parameters in the script are correct.
2. Missing Packages: Ensure all dependencies are installed. Check requirements.txt and re-run pip install -r requirements.txt if necessary.
3. File Not Found: Ensure the ZIP file and the CSV file inside it are correctly named and located in the script’s directory.

### FastAPI Service
In addition to the ETL pipeline, this repository includes a FastAPI service to interact with the processed Airbnb data. The FastAPI service provides endpoints to query average prices based on different criteria. find more documentation under api_documentation.md file.

