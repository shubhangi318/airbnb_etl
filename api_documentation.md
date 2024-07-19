## API Documentation

This section provides details on the FastAPI service endpoints available in this project. The API provides endpoints to retrieve average prices for Airbnb listings based on different criteria.

### Prerequisites

Ensure that you have the following setup:

1. Database Connection: Make sure the DATABASE_URL is correctly set in your .env file or directly in the script. The URL should point to your PostgreSQL database.
2. Environment Variables: Load environment variables using dotenv. Create a .env file in your project directory with the required variables, such as DATABASE_URL.

### Running the FastAPI Service

To run the FastAPI service, execute the following command in your terminal from the project root directory:

  ```
  uvicorn fast_api:app --reload
  ```

### API Endpoints

1. Get Average Price by Neighbourhood and Room Type
Endpoint: /average-price

Method: 'GET'

Description: Calculates the average price of Airbnb listings based on the provided neighbourhood and/or room type.

Parameters:
- neighbourhood (optional): The name of the neighbourhood.
- room_type (optional): The type of room (e.g., Entire home/apt, Private room).

Example Request:
```
curl -X 'GET' 'http://127.0.0.1:8000/average-price?neighbourhood=Downtown&room_type=Entire%20home/apt'
```

Response:
- Success: Returns the average price as a numeric value.
- Error 404: If the neighbourhood or room type is not found.
- Error 500: If there's an error processing the request.

json
```
{
    "average_price": 120.50
}
```

Similarly you can get the average price by room type

Example Request:
```
curl -X 'GET' 'http://127.0.0.1:8000/average-price?room_type=Entire%20home/apt'
```

2. Get Average Price by Latitude and Longitude
Endpoint: /average-price-lat-long

Method: 'GET'

Description: Calculates the average price of Airbnb listings within a 0.01 latitude and longitude range around the provided coordinates.

Parameters:
- latitude: The latitude of the center point.
- longitude: The longitude of the center point.
- 
Example Request:
```
curl -X 'GET' 'http://127.0.0.1:8000/average-price-lat-long?latitude=40.7128&longitude=-74.0060'
```

Response:
- Success: Returns the average price as a numeric value.
- Error 404: If no listings are found within the range.
- Error 500: If there's an error processing the request.

json
```
{
    "average_price": 150.25
}
```

### Notes

Ensure that the PostgreSQL server is running and accessible from the machine running the FastAPI service.
Make sure that the psycopg2 and python-dotenv packages are installed in your virtual environment.
