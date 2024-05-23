import time
from kafka import KafkaProducer
import json
import pandas as pd
file_path = "/Users/macos/Documents/Python/Project-CV/Application_of_sales_analysis/Prepared_Data"
KAFKA_TOPIC_NAME = 'sales'
def generate_data():
    producer = KafkaProducer(
    bootstrap_servers = "localhost:9092,localhost:9093,localhost:9094",
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )
    sales_df = pd.read_csv(f"{file_path}/processed_file.csv")
    sales_list = sales_df.to_dict(orient='records')
    for message in sales_list:
        print(message)
        producer.send(KAFKA_TOPIC_NAME,message)
        producer.flush()
        time.sleep(1)


if __name__ == '__main__':
    generate_data()