import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,StringType,FloatType,TimestampType
from pyspark.sql.functions import from_json,col,to_date,split


input_file = "/Users/macos/Documents/Python/Project-CV/Application_of_sales_analysis/Datasource"
output_file = "/Users/macos/Documents/Python/Project-CV/Application_of_sales_analysis/Prepared_Data"


def setup_logger():
    """
    setup the logger for for the application
    this function configure the logging format and level
    Returns:
    None
    """
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )

def init_spark_session():
    """
    Initialize a Spark Session

    This function sets up a Spark Session with specific configuration.

    Returns:
        SparkSession: The configured Spark session.
    """

    logger = logging.getLogger(__name__)
    logger.info("Setting up Spark session...")
    spark = SparkSession.builder \
            .appName("KafkaStructeredStreaming") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
            .config("spark.jars", "libs/postgresql-42.2.20.jar") \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def read_kafka_data(spark,kafka_topic,kafka_bootstrap_servers):
    """
    Read data from kafka:

    Args:
        spark (SparkSession): The Spark Session
        kafka_topic (str): The kafka topic to subcribe to
        kafka_bootstrap_servers: address of kafka bootstrap server
    Returns:
        DataFrame: DataFrame containing Kafka data
    
    """

    logger = logging.getLogger(__name__)
    logger.info('Setting up Kafka source for Structured Streaming...')

    sales_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",kafka_bootstrap_servers) \
        .option("subscribe",kafka_topic) \
        .option("startingOffsets","latest") \
        .load()
    
    sales_df1 = sales_df.selectExpr("CAST(value AS STRING)")
    return sales_df1

def process_data(spark,kafka_topic,kafka_bootstrap_servers, stock_filepath, postgresql_jdbc_url, postgres_config):

    """
    Process and analyze data from Kafka and external files, and write results to various sinks

    Args:
        spark (sparkSession): The Spark Session
        kafka_topic (str): The kafka topic to subscribe to
        kafka_bootstrap_servers (str): address of kafka bootstrap server
        stock_filepath (str): the file path of stock data.
        postgresql_jdbc_url (str): The JDBC URL for Postgresql
        postgres_config (dict): configuration parameters for posgresql

    Returns:
        None 

    """

    logger = logging.getLogger(__name__)

    sales_df = read_kafka_data(spark,kafka_topic,kafka_bootstrap_servers)

    sales_schema = StructType()\
        .add("sale_id", IntegerType()) \
        .add("product", StringType()) \
        .add("quantity_sold", IntegerType()) \
        .add("each_price",FloatType()) \
        .add("sale_date",TimestampType())\
        .add("sales", FloatType())
    sales_df = sales_df.select(from_json(col('value'),sales_schema).alias('sales_data'))
    sales_df = sales_df.select('sales_data.*')

    sales_df = sales_df \
        .withColumn("date",to_date(col("sale_date"),"yyyy-MM-dd")) \
        .withColumn("day", split(col("Date"),"-").getItem(2)) \
        .withColumn("month", split(col("Date"),"-").getItem(1)) \
        .withColumn("year", split(col("Date"),"-").getItem(0)) \
        .drop("sale_date") \
        .withColumn("day",col("day").cast(IntegerType()))\
        .withColumn("month",col("month").cast(IntegerType()))\
        .withColumn("year",col("year").cast(IntegerType()))
    
    stocks_df = spark.read.csv(stock_filepath,header=True, inferSchema=True)

    stocks_df = stocks_df \
            .withColumnRenamed("quantity_sold","quantity_sold_stocks") \
            .join(
                sales_df
                .drop('sale_id')
                .drop('sale_date')
                .drop('sales'), on = 'product', how='inner')\
            .withColumnRenamed("quantity_sold","quantity_sold_sales")
    
    stocks_df = stocks_df.groupBy('product','stock_quantity','each_price') \
        .agg({'quantity_sold_sales':'sum'}) \
        .select('product','stock_quantity','each_price',col('sum(quantity_sold_sales)').alias('total_quantity_sold'),
                (col('sum(quantity_sold_sales)')*col('each_price')).alias('total_sales'))
    
    query = stocks_df.writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode('update') \
        .option('truncate','true') \
        .format('console') \
        .start()
        
    sales_df.writeStream \
        .trigger(processingTime = '5 seconds') \
        .outputMode('update')\
        .foreachBatch(lambda df, epoch_id: df.write.jdbc(url=postgresql_jdbc_url, table='sales', mode='append', properties=postgres_config))\
        .start()\

    stocks_df.writeStream \
        .trigger(processingTime = '10 seconds') \
        .outputMode('complete')\
        .option('truncate', 'true') \
        .foreachBatch(lambda df, epoch_id: df.write.jdbc(url=postgresql_jdbc_url, table='stocks', mode='overwrite', properties=postgres_config))\
        .start()   \


    query.awaitTermination()

def main():
    """
    Main function to orchestrate the execution of the Spark Structured Streaming application.

    This function sets up the logger, initializes the Spark Session, defines configuration parameters,
    and calls the `process_data` function to execute the ETC process.
    
    """
    setup_logger()
    spark = init_spark_session()

    KAFKA_TOPIC_NAME = "sales"
    KAFKA_BOOTSTRAP_SERVER = "localhost:9092,localhost:9093,localhost:9094"

    postgres_config = {
        'user': 'postgres',
        'password':'postgres',
        'driver':'org.postgresql.Driver'
    }
    posgres_jdbc_url = 'jdbc:postgresql://localhost:5433/sales'
    stock_filepath = f"{output_file}/stock_quantity.csv"
    process_data(spark,KAFKA_TOPIC_NAME,KAFKA_BOOTSTRAP_SERVER,stock_filepath,posgres_jdbc_url,postgres_config)

if __name__ == '__main__':
    main()


