# ETL_on_Crypto
Extraction process
This PySpark script fetches cryptocurrency asset data from the CoinCap API, processes it by cleaning and transforming several columns, and saves the output as a CSV file in Databricks File System (DBFS). Initially, the script retrieves data from the API and checks if the response is successful. If the API call is successful, it loads the JSON data into a Spark DataFrame. The data is cleaned by handling null values in columns such as maxSupply, volumeUsd24Hr, priceUsd, and others. Each of these columns is cast to FloatType and formatted to two decimal places for better readability.

A new column, 'new', is added with a static value (2024), and unnecessary columns like explorer, changePercent24Hr, and vwap24Hr are dropped. The final DataFrame is then filtered based on a positive maxSupply value. The processed data is saved as a CSV file to DBFS for further use.

In the future, the script is intended to load data into AWS Redshift for advanced querying and analytics. This initial transformation step serves as a basis for a more robust ETL (Extract, Transform, Load) pipeline, where Redshift will be used as the data warehousing solution.
