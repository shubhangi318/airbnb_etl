## Documentation

The ETL (Extract, Transform, Load) process for this project involves extracting data from a CSV file inside a ZIP archive, transforming the data by normalizing and calculating various metrics, and loading the results into a PostgreSQL database. Below is a step-by-step breakdown of the ETL process and data transformations.


### 1. Data Extraction

- Step: start
- Description: This step extracts data from a CSV file contained within a ZIP archive.
- Actions:
  - Opens the ZIP file and reads the CSV file into a pandas DataFrame.
  - Loads the DataFrame into a PostgreSQL table named airbnb.raw_data.
 
### 2. Data Loading into PostgreSQL

- Step: start
- Description: The data from the CSV file is loaded into a PostgreSQL table.
- Actions:
  - Connects to the PostgreSQL database.
  - Drops the schema airbnb if it exists and recreates it.
  - Creates a table airbnb.raw_data with columns that correspond to the CSV file structure.
  - Uses the SQL COPY command to efficiently load data from the CSV buffer into the PostgreSQL table.
 
### 3. Data Normalization

- Step: normalize_data
- Description: This step normalizes the raw data by creating several new tables to store distinct values, thereby reducing redundancy and improving data integrity.
- Actions:
  - Creates and populates the following normalized tables:
    1. airbnb.host_data
       - Description: Stores distinct host IDs and host names.
       - Columns:
         - host_id (INT): Unique identifier for each host.
         - host_name (TEXT): Name of the host.

    2. airbnb.neighbourhood_group_data
       - Description: Stores distinct neighbourhood groups.
       - Columns:
         - neighbourhood_group_id (SERIAL): Auto-incrementing unique identifier for each neighbourhood group.
         - neighbourhood_group (TEXT): Name of the neighbourhood group.

    3. airbnb.neighbourhood_data
       - Description: Stores distinct neighbourhoods along with their associated neighbourhood group IDs.
       - Columns:
         - neighbourhood_id (SERIAL): Auto-incrementing unique identifier for each neighbourhood.
         - neighbourhood (TEXT): Name of the neighbourhood.
         - neighbourhood_group_id (INT): Foreign key referencing neighbourhood_group_id from           airbnb.neighbourhood_group_data.

    4. airbnb.room_type_data
       - Description: Stores distinct room types.
       - Columns:
         - room_type_id (SERIAL): Auto-incrementing unique identifier for each room type.
         - room_type (TEXT): Type of the room (e.g., entire home, private room).
  
    5. airbnb.raw_data_normalised
       - Description: Stores the normalized data from airbnb.raw_data, linking to other normalized tables.
       - Columns:
         - id (INT): Unique identifier for each listing.
         - name (TEXT): Name of the listing.
         - host_id (INT): Foreign key referencing host_id from airbnb.host_data.
         - neighbourhood_id (INT): Foreign key referencing neighbourhood_id from airbnb.neighbourhood_data.
         - latitude (FLOAT): Latitude of the listing.
         - longitude (FLOAT): Longitude of the listing.
         - room_type_id (INT): Foreign key referencing room_type_id from airbnb.room_type_data.
         - price (INT): Price of the listing.
         - minimum_nights (INT): Minimum number of nights required to book.
         - number_of_reviews (INT): Number of reviews.
         - last_review (DATE): Date of the last review.
         - reviews_per_month (FLOAT): Average number of reviews per month.
         - calculated_host_listings_count (INT): Number of listings by the host.
         - availability_365 (INT): Number of days the listing is available in a year.

### 4. Data Transformation

1. Step: transform_price_per_neighborhood
   - Description: Calculates the average price per neighborhood and saves the result in an Excel file.
   - Actions:
     - Queries the database to calculate the average price per neighborhood.
     - Saves the results to an Excel file named avg_prices.xlsx with a sheet named avg_price.

2. Step: transform_price_per_neighborhood_group
   - Description: Calculates the average price per neighborhood group and appends the result to the existing Excel file.
   - Actions:
     - Queries the database to calculate the average price per neighborhood group.
     - Appends the results to the same Excel file, creating a new sheet named avg_price_group.

3. Step: transform_listing_type
   - Description: Retrieves the top 3 most expensive listings by neighborhood group and room type, then saves the results to the Excel file.
   - Actions:
     - Queries the database to retrieve and rank listings by price.
     - Filters the top 3 listings per neighborhood group and room type.
     - Saves the results to the Excel file under a new sheet named top_listings.
     - 
4. Step: transform_pop_score
   - Description: Calculates popularity scores based on the number of reviews and reviews per month. Scales the scores and retrieves the top 3 listings per neighborhood group.
   - Actions:
     - Calculates a raw popularity score and scales it from 1 to 10.
     - Ranks the listings by the scaled popularity score within each neighborhood group.
     - Filters the top 3 listings per neighborhood group.
     - Saves the results to the Excel file under a new sheet named pop_listings.
       
### 5. Data Visualization

 1. Step: aggregate_heatmap
- Description: Aggregates price data based on latitude and longitude bins to prepare for a heatmap visualization.
- Actions:
  - Queries the database for price data along with latitude and longitude.
  - Aggregates the data into bins to compute mean prices.
  - Prepares a pivot table suitable for heatmap generation.

  2. Step: plot_heatmap
     - Description: Generates and saves a heatmap visualization of average prices over NYC.
     - Actions:
       - Creates a heatmap using seaborn and matplotlib.
       - Saves the heatmap as a PNG image named heatmap.png.
