from confluent_kafka import Consumer

import json

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker", # The application group id 
    "auto.offset.reset": "earliest" # When the instance doesnt know where it left of when its reading the events it will reset to the earliest event.
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"]) # Subscribe to the topic

print("Consumer is running and subscribed to orders topic")

try:
    while True:
        msg = consumer.poll(1.0) # Pinging the broker for new messages
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue
        
        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(f"Received order: {order['quantity']} x {order['item']} from {order['user']}")
except KeyboardInterrupt:
    print("\n Stopping consumer")

finally:
    consumer.close()
        