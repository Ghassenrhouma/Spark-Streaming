from kafka import KafkaProducer
import json
import time
import random
import pandas as pd

def continuous_producer(csv_file, delay_seconds=2):
    """
    Sends data to Kafka continuously with delays between messages
    
    Args:
        csv_file: Path to your CSV file
        delay_seconds: Delay between each message (creates batches)
    """
    
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Read CSV
    df = pd.read_csv(csv_file)
    
    print(f"Starting continuous producer...")
    print(f"Sending {len(df)} transactions with {delay_seconds}s delay between each")
    print("Press Ctrl+C to stop\n")
    
    try:
        for index, row in df.iterrows():
            transaction = {
                "transaction_id": str(row['transaction_id']),
                "user_id": str(row['user_id']),
                "amount": str(row['amount']),
                "timestamp": str(row['timestamp'])
            }
            
            # Send to Kafka
            producer.send('my-first-topic', transaction)
            
            print(f"✓ Sent transaction {row['transaction_id']} | Amount: ${row['amount']}")
            
            # Wait before sending next message
            time.sleep(delay_seconds)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping producer...")
    finally:
        producer.flush()
        producer.close()
        print("✓ Producer stopped")


if __name__ == "__main__":
    # Adjust these parameters
    csv_file = "transactions.csv"  # Your CSV file
    delay = 2  # Seconds between messages (smaller = more batches)
    
    continuous_producer(csv_file, delay)