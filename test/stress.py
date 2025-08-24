from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import requests
import statistics
import string
import time

SERVER_URL = "http://127.0.0.1:8080"
NUM_THREADS = 15
MESSAGES_PER_THREAD = 100
TOPIC_COUNT = 5

def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def publish_message(topic, message):
    url = f"{SERVER_URL}/publish/{topic}"
    start = time.time()
    response = requests.post(url, data=message)
    elapsed_ms = (time.time() - start) * 1000
    return elapsed_ms, response.status_code

def consume_message(topic):
    url = f"{SERVER_URL}/consume/{topic}"
    start = time.time()
    response = requests.post(url)
    elapsed_ms = (time.time() - start) * 1000
    return elapsed_ms, response.status_code

def worker(thread_id):
    topic = f"topic_{random.randint(1, TOPIC_COUNT)}"
    latencies = []
    for _ in range(MESSAGES_PER_THREAD):
        message = f"Thread {thread_id} - {random_string(20)}"
        elapsed, status = publish_message(topic, message)
        latencies.append(elapsed)
        elapsed_c, status_c = consume_message(topic)
        latencies.append(elapsed_c)
    return latencies

def main():
    all_latencies = []
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(worker, i) for i in range(NUM_THREADS)]
        for future in as_completed(futures):
            all_latencies.extend(future.result())

    if all_latencies:
        print(f"Gesamtanfragen: {len(all_latencies)}")
        print(f"Durchschnittliche Latenz: {sum(all_latencies)/len(all_latencies):.2f} ms")
        print(f"Median Latenz: {statistics.median(all_latencies):.2f} ms")
        print(f"Max Latenz: {max(all_latencies):.2f} ms")
        print(f"Min Latenz: {min(all_latencies):.2f} ms")

if __name__ == "__main__":
    main()