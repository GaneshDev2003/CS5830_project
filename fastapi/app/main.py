from fastapi import FastAPI, Request
from tensorflow.keras.models import Sequential
import tensorflow as tf
import numpy as np
import os
from pydantic import BaseModel
import prometheus_client
from prometheus_client import Summary, start_http_server, Counter, Gauge
from prometheus_fastapi_instrumentator import Instrumentator
import time
import psutil

REQUEST_DURATION = Summary("api_timing", "Request duration in seconds")
counter = Counter(
    "api_usage_counter", "Total number of API requests", ["endpoint", "client"]
)
gauge_runtime = Gauge(
    "api_runtime_secs", "runtime of the method in seconds", ["endpoint", "client"]
)
gauge_length = Gauge(
    "input_text_length", "length of input text", ["endpoint", "client"]
)
gauge_ptpc = Gauge(
    "ptpc", "Processing time per character (PTPC)", ["endpoint", "client"]
)
# Additional metrics
gauge_memory_utilization = Gauge(
    "api_memory_utilization", "API memory utilization", ["endpoint", "client"]
)
gauge_cpu_utilization = Gauge(
    "api_cpu_utilization", "API CPU utilization rate", ["endpoint", "client"]
)
gauge_network_io_bytes = Gauge(
    "api_network_io_bytes", "API network I/O bytes sent", ["endpoint", "client"]
)
gauge_network_io_rate = Gauge(
    "api_network_io_rate", "API network I/O rate sent", ["endpoint", "client"]
)

app = FastAPI()
Instrumentator().instrument(app).expose(app)
model_path = str(os.getenv("MODEL_PATH"))  # Model path from terminal
# Run "export MODEL_PATH=/path/to/model" in terminal


class ReviewRequest(BaseModel):
    text: str


def load_model_weights(model_path: str):
    model = tf.keras.models.load_model(model_path)
    return model


def predict_rating(model: Sequential, data_point: np.array):
    data_point = np.expand_dims(data_point, axis=0)
    pred = model.predict(data_point)
    return str(np.argmax(pred) + 1)  # Returns predicted sentiment rating


def read_review(review: str):
    return np.random.rand(100)


@app.post("/read_review")
async def predict_senti(review: ReviewRequest, request: Request):
    counter.labels(endpoint="/np", client=request.client.host).inc()  # Add counter
    # Capture initial network I/O
    initial_network_io = psutil.net_io_counters()
    start = time.time()
    review_text = review.text  # Receives user review
    input_text_length = len(review_text)
    processed_text = read_review(review_text)  # Pre-process it
    model = load_model_weights(model_path)
    rating = predict_rating(model, processed_text)  # Predict the score
    time_taken = time.time() - start  # Time taken
    # Capture final network I/O
    final_network_io = psutil.net_io_counters()

    process_time_per_char = 0
    if input_text_length > 0:
        process_time_per_char = (
            time_taken * 1000000.0 / input_text_length
        )  # Process time per character (in micro-seconds)

    memory_info = psutil.virtual_memory()
    memory_util = memory_info.percent
    cpu_util = psutil.cpu_percent(interval=1)
    io_bytes_sent = final_network_io.bytes_sent - initial_network_io.bytes_sent
    io_rate_sent = io_bytes_sent / time_taken

    gauge_runtime.labels(endpoint="/np", client=request.client.host).set(
        time_taken
    )  # Gauge time
    gauge_length.labels(endpoint="/np", client=request.client.host).set(
        input_text_length
    )  # Gauge input text length
    gauge_ptpc.labels(endpoint="/np", client=request.client.host).set(
        process_time_per_char
    )  # Gauge process time per character
    gauge_memory_utilization.labels(endpoint="/np", client=request.client.host).set(
        memory_util
    )  # Gauge memory utilization
    gauge_cpu_utilization.labels(endpoint="/np", client=request.client.host).set(
        cpu_util
    )  # Gauge CPU utilization
    gauge_network_io_bytes.labels(endpoint="/np", client=request.client.host).set(
        io_bytes_sent
    )  # Gauge API network I/O bytes sent
    gauge_network_io_rate.labels(endpoint="/np", client=request.client.host).set(
        io_rate_sent
    )  # Gauge API network I/O rate sent
    return {"Sentiment score ": rating}
