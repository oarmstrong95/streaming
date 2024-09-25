from google.cloud import pubsub_v1
from dotenv import load_dotenv
import time
import os
from google.api_core import exceptions

# Initialize environment variables
load_dotenv()

# Get environment variables
PROJECT_ID = os.getenv('PROJECT_ID')
TOPIC_NAME = os.getenv('TOPIC_NAME')
SUBSCRIPTION_NAME = os.getenv('SUBSCRIPTION_NAME')

print(f"PROJECT_ID: {PROJECT_ID}")
print(f"TOPIC_NAME: {TOPIC_NAME}")
print(f"SUBSCRIPTION_NAME: {SUBSCRIPTION_NAME}")

# Create clients
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

# Full paths
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

# Create topic if it doesn't exist
try:
    topic = publisher.create_topic(request={"name": topic_path})
    print(f"Topic created: {topic.name}")
except exceptions.AlreadyExists:
    print(f"Topic {topic_path} already exists.")
except Exception as e:
    print(f"Error creating topic: {e}")
    exit(1)

# Create subscription if it doesn't exist
try:
    subscription = subscriber.create_subscription(
        request={"name": subscription_path, "topic": topic_path}
    )
    print(f"Subscription created: {subscription.name}")
except exceptions.AlreadyExists:
    print(f"Subscription {subscription_path} already exists.")
except Exception as e:
    print(f"Error creating subscription: {e}")
    exit(1)

# Function to publish a message
def publish_message(message):
    try:
        future = publisher.publish(topic_path, message.encode('utf-8'))
        message_id = future.result()
        print(f"Published message ID: {message_id}")
    except Exception as e:
        print(f"Error publishing message: {e}")

# Publish some messages
for i in range(5):
    publish_message(f"Message {i}")
    time.sleep(1)

# Callback function to process received messages
def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    message.ack()

# Subscribe to the topic
try:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}")

    # Wait for 60 seconds for messages
    streaming_pull_future.result(timeout=60)
except exceptions.NotFound:
    print(f"Subscription {subscription_path} not found. Please check if it exists.")
except TimeoutError:
    streaming_pull_future.cancel()
    print("Finished listening for messages.")
except Exception as e:
    print(f"Error in subscription: {e}")
finally:
    subscriber.close()

# Delete a subscription
subscriber.delete_subscription(request={"subscription": subscription_path})

# Delete a topic
publisher.delete_topic(request={"topic": topic_path})