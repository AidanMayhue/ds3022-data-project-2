import boto3
import requests
import time
from prefect import flow, task, get_run_logger

#this task triggers the api to access the populate api and place the messages in the sqs queue
@task
def trigger_api() -> str:
    """Trigger the API and return the SQS queue URL."""
    logger = get_run_logger()
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/xdw9vp"

    logger.info("Triggering API to get SQS URL...")
    payload = requests.post(url).json()
    sqs_url = payload["sqs_url"]
    logger.info(f"SQS Queue URL: {sqs_url}")
    return sqs_url

#This task receives initial messages from the SQS queue to verify the connection.
@task
def receive_initial_messages(sqs_url: str):
    """Receive initial messages from SQS to verify connection."""
    logger = get_run_logger()
    sqs = boto3.client("sqs", region_name="us-east-1")

    response = sqs.receive_message(
        QueueUrl=sqs_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=5,
        VisibilityTimeout=30
    )

    messages = response.get("Messages", [])
    logger.info(f"Received {len(messages)} initial message(s).")
    #makes sure to log each message received
    for msg in messages:
        logger.info(f"\nMessage ID: {msg['MessageId']}\nBody: {msg['Body']}")

    logger.info("Initial connection test complete.")

#This task monitors the SQS queue until all expected messages are received.
@task
def monitor_sqs_queue(sqs_url: str, expected_count: int = 21):
    """Monitor SQS queue until all expected messages are received."""
    logger = get_run_logger()
    sqs = boto3.client("sqs", region_name="us-east-1")  

    collected_data = {}
    received_ids = set()

    logger.info("Now monitoring queue to receive ALL messages...")
    #This makes sure we keep checking the queue until we have all expected messages.
    while len(collected_data) < expected_count:
        # Get queue attributes
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
        #This block receives messages if available
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
                        continue  # skip duplicates

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
                logger.error(f"Error receiving messages: {e}")
        else:
            logger.info("No messages available, waiting 10 seconds...")
            time.sleep(10)

    logger.info("\n All messages received successfully.")
    return collected_data
#This task sends the final solution message back to the prof
@task
def send_solution(uvaid, collected_data, platform):
    sqs = boto3.client("sqs", region_name="us-east-1")
    try:
        
        phrase = " ".join(collected_data[str(i)] for i in sorted(map(int, collected_data.keys())))
        response = sqs.send_message(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit",
            MessageBody="Submitting solution",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        print(f"Response: {response}")
        print("Solution sent successfully.")

    except Exception as e:
        print(f"Error sending message: {e}")#
#This task confirms the final results
@task
def summarize_results(collected_data: dict):
    """Print the final results."""
    logger = get_run_logger()
    logger.info("\nFinal results:")
    for order_no in sorted(collected_data, key=lambda x: int(x)):
        logger.info(f"{order_no}: {collected_data[order_no]}")

#The main flow that ties all tasks together
@flow(name="SQS Message Collector Flow")
def sqs_monitor_flow():
    """Main Prefect flow to trigger API, read SQS, and collect messages."""
    sqs_url = trigger_api()
    receive_initial_messages(sqs_url)
    collected_data = monitor_sqs_queue(sqs_url)
    send_solution("xdw9vp", collected_data, "prefect")
    summarize_results(collected_data)

#trigger the flow
if __name__ == "__main__":
    sqs_monitor_flow()
