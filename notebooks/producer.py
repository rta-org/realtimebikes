#!/usr/bin/env python3
"""
Script to fetch Lyft GBFS data from multiple endpoints and send it to corresponding Kafka topics.
"""

import json
import logging
import time
from datetime import datetime
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration - GBFS endpoints and corresponding Kafka topics
GBFS_FEEDS = [
    {
        "url": "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json",
        "topic": "information_station",
        "description": "Static station information",
        "poll_interval": 3600  # Poll every hour (3600 seconds)
    },
    {
        "url": "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json",
        "topic": "station_status",
        "description": "Real-time station status",
        "poll_interval": 60  # Poll every minute (60 seconds)
    },
    {
        "url": "https://gbfs.lyft.com/gbfs/2.3/bkn/en/vehicle_types.json",
        "topic": "vehicle_types",
        "description": "Vehicle types information",
        "poll_interval": 86400  # Poll once a day (86400 seconds)
    }
]

KAFKA_BOOTSTRAP_SERVERS = ['broker:9092']

def fetch_gbfs_data(url):
    """
    Fetch data from a GBFS API endpoint.
    
    Args:
        url (str): The GBFS endpoint URL
        
    Returns:
        dict: The JSON response from the API
    """
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {url}: {e}")
        return None

def create_kafka_producer():
    """
    Create and return a Kafka producer instance.
    
    Returns:
        KafkaProducer: Configured Kafka producer
    """
    try:
        # Create producer that serializes data as JSON
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'  # Wait for all replicas to acknowledge
        )
    except KafkaError as e:
        logger.error(f"Error creating Kafka producer: {e}")
        return None

def on_send_success(record_metadata):
    """Callback for successful Kafka message delivery."""
    logger.info(f"Message delivered to {record_metadata.topic} "
                f"[partition: {record_metadata.partition}, offset: {record_metadata.offset}]")

def on_send_error(exc):
    """Callback for failed Kafka message delivery."""
    logger.error(f"Message delivery failed: {exc}")

def process_and_send_data(producer, feed):
    """
    Fetch data from a specific GBFS feed and send it to the corresponding Kafka topic.
    
    Args:
        producer (KafkaProducer): The Kafka producer instance
        feed (dict): Dictionary containing feed URL and topic information
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Get current timestamp
    timestamp = datetime.now().isoformat()
    
    # Fetch data
    data = fetch_gbfs_data(feed["url"])
    
    if not data:
        logger.warning(f"No data fetched for {feed['description']}, skipping this feed")
        return False
    
    # Add metadata
    enriched_data = {
        "source": "lyft_gbfs",
        "feed_type": feed["description"],
        "collected_at": timestamp,
        "data": data
    }
    
    # Send to Kafka
    try:
        future = producer.send(
            feed["topic"],
            value=enriched_data
        )
        future.add_callback(on_send_success).add_errback(on_send_error)
        logger.info(f"Data sent to Kafka topic {feed['topic']}")
        return True
    except Exception as e:
        logger.error(f"Error sending data to Kafka topic {feed['topic']}: {e}")
        return False

def main():
    """Main execution function."""
    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting.")
        return
    
    logger.info("Starting to poll GBFS feeds with individual polling intervals")
    
    # Initialize last poll time for each feed
    last_poll_times = {i: 0 for i in range(len(GBFS_FEEDS))}
    
    try:
        while True:
            current_time = time.time()
            successful_feeds = 0
            processed_feeds = 0
            
            # Process each feed if it's time to poll
            for i, feed in enumerate(GBFS_FEEDS):
                # Check if it's time to poll this feed
                if current_time - last_poll_times[i] >= feed["poll_interval"]:
                    logger.info(f"Polling feed: {feed['description']}")
                    processed_feeds += 1
                    
                    if process_and_send_data(producer, feed):
                        successful_feeds += 1
                        # Update last poll time
                        last_poll_times[i] = current_time
            
            # Make sure all messages are sent before continuing
            if processed_feeds > 0:
                producer.flush()
                logger.info(f"Processed {successful_feeds}/{processed_feeds} feeds successfully")
            
            # Wait for a short interval before checking again
            # This determines how responsive the program is to interrupts
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Process interrupted. Closing Kafka producer.")
    finally:
        # Clean up resources
        if producer:
            producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    main()