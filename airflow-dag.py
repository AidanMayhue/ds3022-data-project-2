from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin  
from datetime import datetime, timedelta
import boto3
import requests
import time


# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

#This provides several necessary functionalities for logging within Airflow tasks.
@dag(
    dag_id='sqs_message_collector_dag',
    default_args=default_args,
    description='Trigger API, monitor SQS messages, and submit solution',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['sqs', 'api', 'boto3']
)
#This task defines the function that monitors the SQS queue.
def sqs_monitor_dag():

    # ✅ Create a logger from Airflow’s internal system
    logger = LoggingMixin().log
    #This task triggers the API to get the SQS URL.
    @task()
    def trigger_api():
        """Trigger the API and return the SQS queue URL."""
        url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/xdw9vp"
        logger.info("Triggering API to get SQS URL...")
        payload = requests.post(url).json()
        sqs_url = payload["sqs_url"]
        logger.info(f"SQS Queue URL: {sqs_url}")
        return sqs_url

    #This task receives initial messages from the SQS queue to verify the connection.
    @task()
    def receive_initial_messages(sqs_url: str):
        """Receive initial messages from SQS to verify connection."""
        sqs = boto3.client("sqs", region_name="us-east-1")

        response = sqs.receive_message(
            QueueUrl=sqs_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
            VisibilityTimeout=30
        )

        messages = response.get("Messages", [])
        logger.info(f"Received {len(messages)} initial message(s).")

        for msg in messages:
            logger.info(f"\nMessage ID: {msg['MessageId']}\nBody: {msg['Body']}")

        logger.info("Initial connection test complete.")

    #This task monitors the SQS queue until all expected messages are received.
    @task()
    def monitor_sqs_queue(sqs_url: str, expected_count: int = 21):
        """Monitor SQS queue until all expected messages are received."""
        sqs = boto3.client("sqs", region_name="us-east-1")
        collected_data = {}
        received_ids = set()

        logger.info("Now monitoring queue to receive ALL messages...")
        #This makes sure we keep checking the queue until we have all expected messages.
        while len(collected_data) < expected_count:
            attrs = sqs.get_queue_attributes(
                QueueUrl=sqs_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ],
            )["Attributes"]

            available = int(attrs["ApproximateNumberOfMessages"])
            in_flight = int(attrs["ApproximateNumberOfMessagesNotVisible"])
            delayed = int(attrs["ApproximateNumberOfMessagesDelayed"])

            logger.info(
                f"\nCollected {len(collected_data)}/{expected_count} messages | "
                f"Available: {available}, In-flight: {in_flight}, Delayed: {delayed}"
            )

            if available > 0:
                try:
                    response = sqs.receive_message(
                        QueueUrl=sqs_url,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=5,
                        MessageAttributeNames=["All"],
                    )

                    messages = response.get("Messages", [])
                    for msg in messages:
                        msg_id = msg["MessageId"]
                        if msg_id in received_ids:
                            continue

                        attributes = msg.get("MessageAttributes", {})
                        if "order_no" in attributes and "word" in attributes:
                            order_no = attributes["order_no"]["StringValue"]
                            word = attributes["word"]["StringValue"]

                            collected_data[order_no] = word
                            received_ids.add(msg_id)
                            logger.info(f"Received order_no={order_no}, word={word}")

                        # delete the message now that it’s processed
                            try:
                                sqs.delete_message(
                                    QueueUrl=sqs_url,
                                    ReceiptHandle=msg["ReceiptHandle"]
        )
                                logger.info(f"Deleted message ID: {msg_id}")
                            except Exception as e:
                                logger.error(f"Failed to delete message {msg_id}: {e}")
                        else:
                            logger.warning(f"Message missing expected attributes: {msg}")


                except Exception as e:
                    logger.exception(f"Error receiving messages: {e}")
            else:
                logger.info("No messages available, waiting 10 seconds...")
                time.sleep(10)

        logger.info("\nAll messages received successfully.")
        return collected_data

    #This task sends the final solution message back to SQS.
    @task()
    def send_solution(uvaid: str, collected_data: dict, platform: str):
        """Send the final solution message back to SQS."""
        sqs = boto3.client("sqs", region_name="us-east-1")
        try:
            phrase = " ".join(collected_data[str(i)] for i in sorted(map(int, collected_data.keys())))
            response = sqs.send_message(
                QueueUrl="https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit",
                MessageBody="Submitting solution",
                MessageAttributes={
                    'uvaid': {'DataType': 'String', 'StringValue': uvaid},
                    'phrase': {'DataType': 'String', 'StringValue': phrase},
                    'platform': {'DataType': 'String', 'StringValue': platform}
                }
            )
            logger.info(f"Response: {response}")
            logger.info("Solution sent successfully.")
        except Exception:
            logger.exception("Error sending solution message.")

    #double check to make sure everything sent
    @task()
    def summarize_results(collected_data: dict):
        """Print the final results."""
        logger.info("\nFinal results:")
        for order_no in sorted(collected_data, key=lambda x: int(x)):
            logger.info(f"{order_no}: {collected_data[order_no]}")


    # order of dag tasks
    sqs_url = trigger_api()
    receive_initial_messages(sqs_url)
    collected_data = monitor_sqs_queue(sqs_url)
    send_solution("xdw9vp", collected_data, "airflow")
    summarize_results(collected_data)


# Instantiate the DAG
dag = sqs_monitor_dag()


