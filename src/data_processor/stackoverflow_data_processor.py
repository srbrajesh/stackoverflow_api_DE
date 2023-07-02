# !pip install pyspark

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

class StackOverflowDataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder.master('local[*]').appName('testApp').getOrCreate()

    def fetch_stackoverflow_data(self, tag, from_date, page_size=100, page=1):
        """
        Fetches Stack Overflow data for a specific tag and from a given date.

        Args:
            tag (str): The tag to filter the Stack Overflow questions.
            from_date (str): The starting date (YYYY-MM-DD) for the data retrieval.

        Returns:
            dict: The JSON response containing the Stack Overflow data.
                Returns None if the API request fails.
        """
        url = f'''https://api.stackexchange.com/2.3/questions?
                order=desc&sort=activity&tagged={tag}&fromdate={from_date}&site=stackoverflow&page={page}&pagesize={page_size}'''

        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for non-200 status codes
            return response.json()
        except (requests.RequestException, ValueError) as e:
            print(f"An error occurred while fetching Stack Overflow data: {e}")
            return None

    def get_top_trending_tags(self, data, count):
        """
        Extracts the top trending tags from Stack Overflow data.

        Args:
            data (dict): The Stack Overflow data in JSON format.
            count (int): The number of top tags to extract.

        Returns:
            pyspark.sql.DataFrame: DataFrame containing the top trending tags.
        """
        trending_df = (
            self.spark.createDataFrame(data['items'])
            .select(explode('tags').alias('tags'))
            .groupBy('tags').count()
            .orderBy(col('count').desc()).limit(count)
        )

        return trending_df

    def write_into_lake(self, data, destination_path):
        """
        Writes Stack Overflow data into a lake using Spark.

        Args:
            data (dict): The Stack Overflow data in JSON format.
            destination_path (str): The path to write the data in the lake.

        Returns:
            pyspark.sql.DataFrame: DataFrame containing the selected columns.
        """
        try:
            stackoverflow_df = self.spark.createDataFrame(data['items'])
            stackoverflow_df = stackoverflow_df.select('question_id', 'title', 'tags')

            stackoverflow_df\
                .write\
                .mode('overwrite')\
                .format('parquet')\
                .save(destination_path)

        except Exception as e:
            print(f"An error occurred while writing data to the lake: {e}")
            return None

