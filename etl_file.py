import io
import zipfile
import logging

from metaflow import FlowSpec, step
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from sklearn.preprocessing import MinMaxScaler
import seaborn as sns
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLFlow(FlowSpec):
    # Define the ZIP file path and PostgreSQL connection parameters. User can change the values for these parameters.
    zip_file_path = "kaggle_download.zip"
    table_name = "airbnb.raw_data"
    database_name = "teal_data_scraped_raw"
    user = "shubhangi_bhatia"
    password = "123"
    host = "localhost"
    port = "5432"

    def dbConnection(self):
        """
        A function to establish a connection to a PostgreSQL database.
        """
        return psycopg2.connect(
            # 'postgresql://postgres:fSceYFwpAKnrziJgaaNJKLcDWKWToMkn@roundhouse.proxy.rlwy.net:27914/railway'
            dbname=self.database_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    @step
    def start(self):
        """
        Step for data ingestion and loading into PostgreSQL.
        This step reads a CSV file from a ZIP archive, ingests the data into a pandas DataFrame, and then loads the data into a PostgreSQL table.
        Parameters:
            None
        Returns:
            None
        Raises:
            Exception: If there is an error loading the data into the PostgreSQL table.
        Side Effects:
            - Prints a success message if the data is ingested successfully.
            - Prints an error message if there is an error loading the data.
            - Calls the `next` method to transition to the next step in the flow.
        """
        try:
            # Open the ZIP file and read the CSV file contained within it
            with zipfile.ZipFile(self.zip_file_path, 'r') as zip_ref:
                with zip_ref.open(zip_ref.namelist()[0]) as csv_file:
                    self.df = pd.read_csv(csv_file)

            logger.info("Data ingested successfully.")

            csv_buffer = io.StringIO()
            self.df.to_csv(csv_buffer, index=False, header=True)
            csv_buffer.seek(0)

            conn = self.dbConnection()
            cur = conn.cursor()

            create_schema_sql = "CREATE SCHEMA IF NOT EXISTS airbnb"
            cur.execute(create_schema_sql)

            cur.execute("DROP TABLE IF EXISTS airbnb.raw_data CASCADE")
            create_table_sql = """
                CREATE TABLE airbnb.raw_data (
                    id INTEGER NOT NULL PRIMARY KEY,
                    name TEXT,
                    host_id INTEGER,
                    host_name TEXT,
                    neighbourhood_group TEXT,
                    neighbourhood TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    room_type TEXT,
                    price INTEGER,
                    minimum_nights INTEGER,
                    number_of_reviews INTEGER,
                    last_review DATE,
                    reviews_per_month DOUBLE PRECISION,
                    calculated_host_listings_count INTEGER,
                    availability_365 INTEGER
                );
            """
            cur.execute(create_table_sql)

            # Prepare the SQL COPY command to load data from CSV buffer to the PostgreSQL table
            copy_sql = f"""
            COPY {self.table_name} (id, name, host_id, host_name, neighbourhood_group, neighbourhood, latitude, longitude, room_type, price, minimum_nights, number_of_reviews, last_review, reviews_per_month, calculated_host_listings_count, availability_365) 
            FROM STDIN WITH (FORMAT CSV, HEADER);
            """

            cur.copy_expert(sql=copy_sql, file=csv_buffer)
            conn.commit()

            logger.info(
                f"Data loaded successfully into table {self.table_name}")

        except Exception as e:
            logger.error(f"Error loading data: {e}")

        self.next(self.query_data)

    @step
    def query_data(self):
        """
        A step for querying data from a PostgreSQL database and loading it into a pandas DataFrame.
        """
        try:
            # Create a cursor with RealDictCursor to fetch results as dictionaries
            conn = self.dbConnection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            query = 'SELECT * FROM airbnb.raw_data;'
            cur.execute(query)

            rows = cur.fetchall()
            self.df = pd.DataFrame(rows)
            logger.info("Data queried successfully.")

        except Exception as e:
            logger.error(f"Error querying data: {e}")

        self.next(self.normalize_data)

    @step
    def normalize_data(self):
        """
        Normalizes the data in the database by creating and populating several normalized tables.
        This function executes a series of SQL commands to create and populate several normalized tables in the database. The tables are created to store normalized versions of the data from the `airbnb.raw_data` table.
        The function first creates the `airbnb.host_data` table and inserts distinct `host_id` and `host_name` values from the `airbnb.raw_data` table.
        Next, the function creates the `airbnb.neighbourhood_group_data` table and inserts distinct `neighbourhood_group` values from the `airbnb.raw_data` table.
        Then, the function creates the `airbnb.neighbourhood_data` table and inserts distinct `neighbourhood` and `neighbourhood_group_id` values from the `airbnb.raw_data` and `airbnb.neighbourhood_group_data` tables.
        After that, the function creates the `airbnb.room_type_data` table and inserts distinct `room_type` values from the `airbnb.raw_data` table.
        Finally, the function creates the `airbnb.raw_data_normalised` table and inserts normalized data from the `airbnb.raw_data`, `airbnb.neighbourhood_group_data`, `airbnb.neighbourhood_data`, and `airbnb.room_type_data` tables.
        """
        try:
            with self.dbConnection() as conn:
                with conn.cursor() as cur:
                    # Create table for normalized host data
                    cur.execute("""
                        CREATE TABLE airbnb.host_data (
                            host_id INT PRIMARY KEY,
                            host_name TEXT
                            )
                    """)

                    cur.execute("""
                        CREATE INDEX idx_host_name
                        ON airbnb.host_data (host_name)
                    """)

                    cur.execute("""
                        INSERT INTO airbnb.host_data (host_id, host_name)
                        SELECT DISTINCT host_id, host_name
                        FROM airbnb.raw_data
                    """)

                    # Create table for normalized neighbourhood groups
                    cur.execute("""
                        CREATE TABLE airbnb.neighbourhood_group_data (
                            neighbourhood_group_id SERIAL PRIMARY KEY,
                            neighbourhood_group TEXT
                            )
                    """)

                    cur.execute("""
                        CREATE INDEX idx_neighbourhood_group
                        ON airbnb.neighbourhood_group_data (neighbourhood_group)
                    """)

                    cur.execute("""
                        INSERT INTO airbnb.neighbourhood_group_data (neighbourhood_group)
                        SELECT DISTINCT neighbourhood_group
                        FROM airbnb.raw_data
                    """)

                    # Create table for normalized neighbourhood data with foreign key to neighbourhood_group_data
                    cur.execute("""
                        CREATE TABLE airbnb.neighbourhood_data (
                            neighbourhood_id SERIAL PRIMARY KEY,
                            neighbourhood TEXT,
                            neighbourhood_group_id INT,
                            FOREIGN KEY (neighbourhood_group_id) REFERENCES airbnb.neighbourhood_group_data(neighbourhood_group_id)
                        )
                    """)

                    cur.execute("""
                        CREATE INDEX idx_neighbourhood_group_id
                        ON airbnb.neighbourhood_data (neighbourhood_group_id)
                    """)

                    # Create an index on neighbourhood column
                    cur.execute("""
                        CREATE INDEX idx_neighbourhood
                        ON airbnb.neighbourhood_data (neighbourhood)
                    """)

                    cur.execute("""
                        INSERT INTO airbnb.neighbourhood_data (neighbourhood, neighbourhood_group_id)
                        SELECT DISTINCT neighbourhood, neighbourhood_group_id
                        FROM airbnb.raw_data
                        JOIN airbnb.neighbourhood_group_data
                        ON airbnb.raw_data.neighbourhood_group = airbnb.neighbourhood_group_data.neighbourhood_group
                    """)

                    # Create table for normalized room type data
                    cur.execute("""
                        CREATE TABLE airbnb.room_type_data (
                            room_type_id SERIAL PRIMARY KEY,
                            room_type TEXT
                        )
                    """)

                    cur.execute("""
                        CREATE INDEX idx_room_type
                        ON airbnb.room_type_data (room_type)
                    """)

                    cur.execute("""
                        INSERT INTO airbnb.room_type_data (room_type)
                        SELECT DISTINCT room_type
                        FROM airbnb.raw_data
                    """)

                    # Create table for normalized raw data with foreign keys to other normalized tables
                    cur.execute("""
                        CREATE TABLE airbnb.raw_data_normalised (
                            id INT PRIMARY KEY,
                            name TEXT,
                            host_id INT,
                            neighbourhood_id INT,
                            latitude FLOAT,
                            longitude FLOAT,
                            room_type_id INT,
                            price INT,
                            minimum_nights INT,
                            number_of_reviews INT,
                            last_review DATE,
                            reviews_per_month FLOAT,
                            calculated_host_listings_count INT,
                            availability_365 INT,
                            FOREIGN KEY (host_id) REFERENCES airbnb.host_data(host_id),
                            FOREIGN KEY (neighbourhood_id) REFERENCES airbnb.neighbourhood_data(neighbourhood_id),
                            FOREIGN KEY (room_type_id) REFERENCES airbnb.room_type_data(room_type_id)
                        )
                    """)

                    cur.execute("""
                        CREATE INDEX idx_host_id
                        ON airbnb.raw_data_normalised (host_id)
                    """)

                    # Create an index on neighbourhood_id
                    cur.execute("""
                        CREATE INDEX idx_neighbourhood_id
                        ON airbnb.raw_data_normalised (neighbourhood_id)
                    """)

                    # Create an index on room_type_id
                    cur.execute("""
                        CREATE INDEX idx_room_type_id
                        ON airbnb.raw_data_normalised (room_type_id)
                    """)

                    cur.execute("""
                        INSERT INTO airbnb.raw_data_normalised (id, name, host_id, neighbourhood_id, latitude, longitude, room_type_id, price, minimum_nights, number_of_reviews, last_review, reviews_per_month, calculated_host_listings_count, availability_365)
                        SELECT airbnb.raw_data.id, airbnb.raw_data.name, airbnb.raw_data.host_id, airbnb.neighbourhood_data.neighbourhood_id, airbnb.raw_data.latitude, airbnb.raw_data.longitude, airbnb.room_type_data.room_type_id, airbnb.raw_data.price, airbnb.raw_data.minimum_nights, airbnb.raw_data.number_of_reviews, airbnb.raw_data.last_review, airbnb.raw_data.reviews_per_month, airbnb.raw_data.calculated_host_listings_count, airbnb.raw_data.availability_365
                        FROM airbnb.raw_data
                        JOIN airbnb.neighbourhood_group_data
                        ON airbnb.raw_data.neighbourhood_group = airbnb.neighbourhood_group_data.neighbourhood_group
                        JOIN airbnb.neighbourhood_data
                        ON airbnb.raw_data.neighbourhood = airbnb.neighbourhood_data.neighbourhood
                        JOIN airbnb.room_type_data
                        ON airbnb.raw_data.room_type = airbnb.room_type_data.room_type
                    """)

                conn.commit()
                logger.info("Data normalization completed successfully.")

        except Exception as e:
            logger.error(f"Error during data normalization: {e}")

        self.next(self.transform_price_per_neighborhood)

    @step
    def transform_price_per_neighborhood(self):
        """
        Step for data transformation and calculating metrics.
        This function connects to a PostgreSQL database, executes a query to calculate the average price per neighborhood,
        and saves the result in an Excel file.
        Raises:
            Exception: If there is an error during data transformation.
        """
        try:
            conn = self.dbConnection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # SQL query to calculate the average price per neighborhood
            query = """
            SELECT 
                n.neighbourhood, 
                AVG(r.price) AS average_price
            FROM 
                airbnb.raw_data_normalised r
            JOIN 
                airbnb.neighbourhood_data n
            ON 
                r.neighbourhood_id = n.neighbourhood_id
            GROUP BY 
                n.neighbourhood
            ORDER BY 
                average_price;
            """
            cur.execute(query)
            rows = cur.fetchall()
            df_avg_price_per_neighborhood = pd.DataFrame(rows)

            file_name = 'avg_prices.xlsx'
            # Write the DataFrame to an Excel file
            with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
                df_avg_price_per_neighborhood.to_excel(
                    writer, sheet_name='avg_price', index=False
                )

            logger.info(
                "Average price per neighborhood table created and populated successfully.")

        except Exception as e:
            logger.error(f"Error during data transformation: {e}")

        self.next(self.transform_price_per_neighborhood_group)

    @step
    def transform_price_per_neighborhood_group(self):
        """
        This function connects to a PostgreSQL database and executes a query to calculate the average price per neighborhood group. The query joins the `raw_data_normalised` table with the `neighbourhood_data` and `neighbourhood_group_data` tables to retrieve the necessary data. The result is then stored in a pandas DataFrame and written to an Excel file named 'avg_prices.xlsx'.
        Raises:
            Exception: If there is an error during the data transformation process.
        """
        try:
            conn = self.dbConnection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # SQL query to calculate the average price per neighborhood group
            query = """
            SELECT 
                ng.neighbourhood_group,
                AVG(r.price) AS average_price
            FROM 
                airbnb.raw_data_normalised r
            JOIN 
                airbnb.neighbourhood_data nd
            ON 
                r.neighbourhood_id = nd.neighbourhood_id
            JOIN 
                airbnb.neighbourhood_group_data ng
            ON 
                nd.neighbourhood_group_id = ng.neighbourhood_group_id
            GROUP BY 
                ng.neighbourhood_group
            ORDER BY 
                average_price;
            """
            cur.execute(query)
            rows = cur.fetchall()
            df_avg_price_per_neighbourhood_group = pd.DataFrame(rows)

            file_name = 'avg_prices.xlsx'
            with pd.ExcelWriter(file_name, engine='openpyxl', mode='a') as writer:
                df_avg_price_per_neighbourhood_group.to_excel(
                    writer, sheet_name='avg_price_group', index=False
                )

            logger.info(
                "Average price per neighborhood group table created and populated successfully.")

        except Exception as e:
            logger.error(
                f"Error during processing average price per neighborhood group: {e}")

        self.next(self.transform_listing_type)

    @step
    def transform_listing_type(self):
        """
        Retrieves the top 3 most expensive listings by neighborhood group and room type from the 'raw_data_normalised' table, joins it with the 'neighbourhood_data', 'neighbourhood_group_data', and 'room_type_data' tables. Then, it calculates the rank of each listing based on its price and selects the top 3 listings for each neighborhood group and room type.
        Raises:
            Exception: If there is an error during the data transformation process.
        """

        try:
            conn = self.dbConnection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            query = """
            SELECT 
                ng.neighbourhood_group,
                rt.room_type,
                r.id,
                r.name,
                r.price
            FROM 
                airbnb.raw_data_normalised r
            JOIN 
                airbnb.neighbourhood_data nd ON r.neighbourhood_id = nd.neighbourhood_id
            JOIN 
                airbnb.neighbourhood_group_data ng ON nd.neighbourhood_group_id = ng.neighbourhood_group_id
            JOIN 
                airbnb.room_type_data rt ON r.room_type_id = rt.room_type_id
            """

            df = pd.read_sql(query, conn)

            # Calculate the top 3 most expensive listings by neighborhood group and room type
            df['rank'] = df.groupby(['neighbourhood_group', 'room_type'])[
                'price'].rank(method='first', ascending=False)
            df_top3 = df[df['rank'] <= 3]
            df_top3 = df_top3.sort_values(
                by=['neighbourhood_group', 'room_type', 'price'], ascending=[True, True, False])

            df_top3 = df_top3.drop('rank', axis=1)

            file_name = 'avg_prices.xlsx'
            with pd.ExcelWriter(file_name, engine='openpyxl', mode='a') as writer:
                df_top3.to_excel(
                    writer, sheet_name='top_listings', index=False
                )

            logger.info(
                "Table 'top_3_most_expensive_listings' created and populated successfully.")

        except Exception as e:
            logger.error(f"Error during processing and table creation: {e}")

        self.next(self.transform_pop_score)

    @step
    def transform_pop_score(self):
        """
        Step for calculating, scaling popularity scores, and getting top 3 listings by neighbourhood group.
        This function connects to a PostgreSQL database, executes a query to retrieve data including neighbourhood_group, and calculates popularity scores for each listing. The popularity scores are then scaled from 1 to 10 using the MinMaxScaler. The function ranks listings by scaled popularity score within each neighbourhood_group and filters the top 3 listings per neighbourhood_group.
        Raises:
            Exception: If there is an error during the processing and table creation.
        """
        try:
            conn = self.dbConnection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Query to get data including neighbourhood_group
            query_data = """
            WITH ListingsWithGroup AS (
                SELECT 
                    r.id,
                    r.name,
                    r.price,
                    r.number_of_reviews,
                    r.reviews_per_month,
                    nd.neighbourhood_group_id
                FROM 
                    airbnb.raw_data_normalised r
                JOIN 
                    airbnb.neighbourhood_data nd
                ON 
                    r.neighbourhood_id = nd.neighbourhood_id
            ),
            ListingsWithPopularity AS (
                SELECT 
                    l.id,
                    l.name,
                    l.price,
                    l.number_of_reviews,
                    l.reviews_per_month,
                    (l.number_of_reviews + COALESCE(l.reviews_per_month, 0) * 10) AS popularity_score,
                    l.neighbourhood_group_id
                FROM 
                    ListingsWithGroup l
            )
            SELECT 
                ngd.neighbourhood_group,
                l.id,
                l.name,
                l.price,
                l.number_of_reviews,
                l.reviews_per_month,
                l.popularity_score
            FROM 
                ListingsWithPopularity l
            JOIN 
                airbnb.neighbourhood_group_data ngd
            ON 
                l.neighbourhood_group_id = ngd.neighbourhood_group_id
            ORDER BY 
                ngd.neighbourhood_group, l.popularity_score DESC;
            """

            cur.execute(query_data)
            rows = cur.fetchall()
            df = pd.DataFrame(rows)

            # Calculate popularity score
            df['popularity_score'] = df['number_of_reviews'] + \
                df['reviews_per_month'].fillna(0) * 10

            # Scale the score form 1 to 10
            scaler = MinMaxScaler(feature_range=(1, 10))
            df['scaled_popularity_score'] = scaler.fit_transform(
                df[['popularity_score']])

            # Rank listings by scaled popularity score within each neighbourhood_group
            df['rank'] = df.groupby('neighbourhood_group')[
                'scaled_popularity_score'].rank(ascending=False, method='first')

            # Filter top 3 listings per neighbourhood_group
            df_top_3 = df[df['rank'] <= 3]

            file_name = 'avg_prices.xlsx'
            with pd.ExcelWriter(file_name, engine='openpyxl', mode='a') as writer:
                df_top_3.to_excel(
                    writer, sheet_name='pop_listings', index=False
                )

            logger.info(
                "Table 'top_listings_scaled_by_neighbourhood_group' created and populated successfully.")

        except Exception as e:
            logger.error(f"Error during processing and table creation: {e}")

        self.next(self.aggregate_heatmap)

    @step
    def aggregate_heatmap(self):
        """
        Generate a heatmap by aggregating price data based on latitude and longitude bins.
        """

        conn = self.dbConnection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Query to get latitude, longitude, and price data
        query = """
        SELECT 
                latitude,
                longitude,
                price
        FROM 
                airbnb.raw_data_normalised
        WHERE
                latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND price IS NOT NULL;
        """

        cur.execute(query)
        rows = cur.fetchall()
        df = pd.DataFrame(rows)

        """Aggregate data and prepare for heatmap"""
        aggregated_df = df.groupby(['latitude', 'longitude']).agg({
            'price': 'mean'}).reset_index()

        # Define bin size
        lat_bin_size = 0.007
        lon_bin_size = 0.007

        # Bin latitude and longitude
        aggregated_df['lat_bin'] = (
            aggregated_df['latitude'] // lat_bin_size) * lat_bin_size
        aggregated_df['lon_bin'] = (
            aggregated_df['longitude'] // lon_bin_size) * lon_bin_size

        # Calculate the mean price for each binned latitude and longitude
        binned_df = aggregated_df.groupby(['lat_bin', 'lon_bin']).agg({
            'price': 'mean'}).reset_index()

        # Create a pivot table for the heatmap
        self.heatmap_data = binned_df.pivot_table(
            index='lat_bin', columns='lon_bin', values='price')

        logger.info(
            "Heatmap data created and populated successfully.")

        self.next(self.plot_heatmap)

    @step
    def plot_heatmap(self):
        """
        Plot a heatmap based on the provided heatmap data and save it as a PNG image.
        """
        plt.figure(figsize=(12, 8))
        ax = sns.heatmap(self.heatmap_data, cmap='viridis', linewidths=0.1,
                         vmin=self.heatmap_data.stack().quantile(0.025),
                         vmax=self.heatmap_data.stack().quantile(0.975))

        # Customize the plot and save it as png
        plt.title('Average price of airbnbs over the city of NYC')

        ax.set_xticks([])
        ax.set_yticks([])
        plt.xlabel('')
        plt.ylabel('')

        plt.savefig('heatmap.png', bbox_inches='tight')
        plt.show()

        logger.info(
            "Plot created and saved successfully.")

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == '__main__':
    ETLFlow()
