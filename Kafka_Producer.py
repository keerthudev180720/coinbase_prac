import json
import os
import websocket
import threading
import time
from kafka import KafkaProducer

# ------------------------
# Config
# ------------------------
# Inside Docker: KAFKA_BOOTSTRAP=kafka:9092
# Running locally against Docker: KAFKA_BOOTSTRAP=localhost:29092
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = "crypto_ticks"
WS_URL = "wss://ws-feed.exchange.coinbase.com"
TIME_LIMIT = 3600
PRODUCT_IDS = ["BTC-USD", "ETH-USD"]

# ------------------------
# Kafka setup
# ------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=10,
    enable_idempotence=True,
    max_in_flight_requests_per_connection=1,
)

def on_send_success(metadata):
    pass  # suppress noisy logging; remove to debug partition assignments

def on_send_error(exc):
    print(f"❌ Kafka delivery failed: {exc}")

ws = None

# ------------------------
# WebSocket handlers
# ------------------------
def on_message(ws, message):
    data = json.loads(message)
    msg_type = data.get("type")

    if msg_type == "subscriptions":
        print("📡 Subscription confirmed:", data)
        return

    if msg_type == "error":
        print("❌ Coinbase error:", data.get("message"), data.get("reason"))
        return

    if msg_type != "ticker":
        return

    # Coinbase ticker fields reference:
    # https://docs.cloud.coinbase.com/exchange/docs/websocket-channels#ticker-channel
    #
    # 24h OHLCV fields present on every ticker message:
    #   open_24h, high_24h, low_24h, volume_24h
    #
    # NOTE: "last_24" does NOT exist on the ticker channel — use these instead.
    # These are rolling 24h values reset at midnight UTC by Coinbase.

    def safe_float(key, default=0.0):
        val = data.get(key)
        try:
            return float(val) if val is not None else default
        except (ValueError, TypeError):
            return default

    event = {
        "product_id": data.get("product_id"),
        "price":      safe_float("price"),
        "bid":        safe_float("best_bid"),
        "ask":        safe_float("best_ask"),
        "volume_24h": safe_float("volume_24h"),

        # Rolling 24h OHLC — useful for Kibana range/trend panels
        "open_24h":   safe_float("open_24h"),
        "high_24h":   safe_float("high_24h"),
        "low_24h":    safe_float("low_24h"),

        # Coinbase sends ISO 8601 UTC: "2026-03-20T12:00:00.123456Z"
        # Spark TimestampType parses this format correctly as-is.
        "event_time": data.get("time"),
    }

    # Skip malformed messages missing critical fields
    if not event["product_id"] or not event["price"] or not event["event_time"]:
        print(f"⚠️  Skipping incomplete ticker: {event}")
        return

    producer.send(
        TOPIC,
        key=event["product_id"],
        value=event
    ).add_callback(on_send_success).add_errback(on_send_error)

    print(f"📤 {event['product_id']} @ {event['price']}  "
          f"bid={event['bid']} ask={event['ask']}")


def on_open(ws):
    print("✅ WebSocket connected")
    subscribe_msg = {
        "type": "subscribe",
        "channels": [
            {"name": "ticker", "product_ids": PRODUCT_IDS}
        ]
    }
    ws.send(json.dumps(subscribe_msg))
    print(f"📡 Subscribed to: {PRODUCT_IDS}")


def on_error(ws, error):
    print("❌ WebSocket error:", error)


def on_close(ws, close_status_code, close_msg):
    print(f"🔌 WebSocket closed (code={close_status_code})")
    producer.flush()
    producer.close()


def start_ws():
    global ws
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)


# ------------------------
# Main
# ------------------------
if __name__ == "__main__":
    t = threading.Thread(target=start_ws, daemon=True)
    t.start()

    start_time = time.time()
    try:
        while True:
            time.sleep(1)
            if time.time() - start_time > TIME_LIMIT:
                print(f"⏰ Time limit ({TIME_LIMIT}s) reached. Stopping...")
                ws.close()
                break
    except KeyboardInterrupt:
        print("🛑 Stopped via Ctrl+C")
        ws.close()

    t.join(timeout=5)
    print("✅ Producer stopped cleanly")