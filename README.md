# gustavo-almeida-de-task

## Data Retrieval

For retrieving the data, I used the `requests` library to make a GET request to the URL provided in the task description. The data is in JSON format, so I used the `json` library to parse it.
Since the API has a limit of 5 request per minute, I had to make multiple requests to get all the data. I used the time.sleep as it is mentioned in the API FAQ.

## Data Modeling
For data modeling I used the One Big Table design, taking in consideration that modern Data Warehouses such as Redshift, BigQuery and Snowflake are columnar databases, which are optimized for reading large amounts of data. This design is also easier to implement and maintain, and it is more flexible for querying.

## Data Loading

I'm using DuckDB as the database for this task. DuckDB is an embeddable SQL OLAP database management system. It is designed to be easy to install, with no external dependencies, and it is optimized for analytical queries. It is also compatible with the pandas library, which makes it easier to work with the data.

## Running the code

I created a Dockerfile to run the code. To build the image, run the following command:

```docker build -t task .```

To run the container, run the following command:

```docker run task```

The code will retrieve the data, create the database and load the data into it.

Also I created a Jupyter Notebook to run the code in a more interactive way.
