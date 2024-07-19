from fastapi import FastAPI, HTTPException
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

DATABASE_URL = "postgresql://postgres:fSceYFwpAKnrziJgaaNJKLcDWKWToMkn@roundhouse.proxy.rlwy.net:27914/railway"

def getDBConnection():
    return psycopg2.connect(DATABASE_URL)

schema_name = "airbnb"

@app.get("/average-price")
async def getAverageNeighbourhoodPrice(neighbourhood: str = None, room_type: str = None):
    try:
        # Connect to PostgreSQL
        conn = getDBConnection()
        cur = conn.cursor()

        # Average price per neighborhood / room type
        query = """
        SELECT
            AVG(r.price) AS average_price
        FROM
            airbnb.raw_data_normalised r
        JOIN
            airbnb.neighbourhood_data nd
        ON
            r.neighbourhood_id = nd.neighbourhood_id
        """

        if neighbourhood:
            query += " WHERE nd.neighbourhood = '{}'".format(neighbourhood)
        
        if room_type:
            if neighbourhood:
                query += " AND "
            else:
                query += " WHERE "
            query += " r.room_type_id = (SELECT room_type_id FROM airbnb.room_type_data WHERE room_type = '{}')".format(room_type)
        
        # group by neighbourhood if neighbourhood is provided
        if neighbourhood:
            query += " GROUP BY nd.neighbourhood"
        
        # group by room type if room type is provided
        if room_type:
            if neighbourhood:
                query += ", "
            else:
                query += " GROUP BY "
            query += "r.room_type_id"



        cur.execute(query)
        row = cur.fetchone()

        if row:
            return row[0]
        else:
            # return 404
            raise HTTPException(status_code=404, detail="Neighbourhood or Room Type not found")

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error during processing average price: {e}")
    

    try:
        # Connect to PostgreSQL
        conn = getDBConnection()
        cur = conn.cursor()

        # Average price per room type
        query = """
        SELECT 
            AVG(r.price) AS average_price
        FROM 
            airbnb.raw_data_normalised r
        JOIN 
            airbnb.room_type_data rt
        ON 
            r.room_type_id = rt.room_type_id
        WHERE 
            rt.room_type = '{}'
        GROUP BY 
            rt.room_type
        """.format(room_type)

        cur.execute(query)
        row = cur.fetchone()

        if row:
            return row[0]
        else:
            # return 404
            raise HTTPException(status_code=404, detail="Room type not found")

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error during processing average price per room type: {e}")
