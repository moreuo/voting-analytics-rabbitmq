import io
import json
import logging
import os
import zipfile

import pandas as pd
import pika
import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def download_dataset(url, extract_to="dataset"):
    os.makedirs(extract_to, exist_ok=True)

    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip:
        for file in zip.namelist():
            if file.endswith(".csv"):
                zip.extract(file, extract_to)


def create_dataframe(csv_filename, url, csv_path="dataset"):
    csv = os.path.join(csv_path, csv_filename)

    if not os.path.exists(csv):
        try:
            download_dataset(url)
        except Exception as e:
            print(e)

    return pd.read_csv(csv, delimiter=";", encoding="latin-1")


def rabbitmq_connect(username, password, queue_name="default"):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            "localhost", credentials=pika.PlainCredentials(username, password)
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue_name)
    return channel


def send_message(channel, dataframe, queue_name="default"):
    try:
        random_row = dataframe.sample(n=1).to_dict(orient="records")[0]
        message = json.dumps(random_row, ensure_ascii=False)
        channel.basic_publish(exchange="", routing_key=queue_name, body=message)
        logging.info(f"[x] Sent: {message}")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    # Create Dataframe
    dataframe = create_dataframe(
        os.getenv("DATASET_FILENAME"), os.getenv("DATASET_URL")
    )

    # Connect to RabbitMQ
    channel = rabbitmq_connect(
        os.getenv("RABBITMQ_USERNAME"),
        os.getenv("RABBITMQ_PASSWORD"),
        os.getenv("RABBITMQ_QUEUE_NAME"),
    )

    # Send Messages to Voting Queue
    try:
        while True:
            send_message(channel, dataframe, os.getenv("RABBITMQ_QUEUE_NAME"))
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt")
    finally:
        channel.close()
