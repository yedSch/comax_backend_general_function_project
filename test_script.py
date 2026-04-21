import os
import json
import logging
from azure.storage.queue import QueueClient
from function_app import process_document   # the queue-trigger function you want to call


# ✅ MUST MATCH the connection used by your Function
AZURE_STORAGE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=comaxgenenralbackend;"
    "AccountKey=<YOUR_ACCOUNT_KEY>;"
    "EndpointSuffix=core.windows.net"
)

PROCESSING_QUEUE_NAME = "processing-queue"


# ✅ Dummy wrapper so .get_body() behaves correctly
class DummyMsg:
    def __init__(self, body):
        self._body = body

    def get_body(self):
        return self._body.encode("utf-8")


def manual_dequeue_and_process():
    print("Connecting to queue...")
    queue = QueueClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING,
        PROCESSING_QUEUE_NAME
    )

    print("Receiving messages (up to 10)...")
    messages = queue.receive_messages(max_messages=10, visibility_timeout=60)

    processed = 0

    for m in messages:
        try:
            print(f"\n--- DEQUEUED ---")
            print(f"ID: {m.id}")
            print(f"BODY: {m.content}")

            # ✅ Build dummy object that mimics QueueMessage
            dummy_msg = DummyMsg(m.content)

            # ✅ Trigger Azure function manually
            process_document(dummy_msg)

            # ✅ Delete AFTER successful processing
            queue.delete_message(m.id, m.pop_receipt)
            processed += 1

        except Exception as e:
            logging.error(f"Error processing message {m.id}: {e}", exc_info=True)

    print(f"\n✅ DONE — processed {processed} messages.")


if __name__ == "__main__":
    manual_dequeue_and_process()
