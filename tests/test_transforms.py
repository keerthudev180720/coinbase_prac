import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType
)
from datetime import datetime

# =====================================================
# FIXTURES
# =====================================================
@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    return SparkSession.builder \
        .appName("CryptoStreamTests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


@pytest.fixture(scope="session")
def sample_df(spark):
    """Sample raw ticker data matching Kafka schema."""
    data = [
        ("BTC-USD", 84320.5, 84318.0, 84323.0, 1000.0,
         datetime(2026, 3, 27, 12, 0, 0)),
        ("ETH-USD", 3210.4,  3209.0,  3211.0,  500.0,
         datetime(2026, 3, 27, 12, 0, 1)),
        # Bad row — negative price
        ("BTC-USD", -1.0,    0.0,     0.0,     0.0,
         datetime(2026, 3, 27, 12, 0, 2)),
        # Bad row — null product_id
        (None,      3210.0,  3209.0,  3211.0,  100.0,
         datetime(2026, 3, 27, 12, 0, 3)),
        # Bad row — bid > ask (invalid spread)
        ("ETH-USD", 3210.0,  3212.0,  3209.0,  100.0,
         datetime(2026, 3, 27, 12, 0, 4)),
        # lowercase product_id — should be uppercased
        ("btc-usd", 84320.0, 84318.0, 84322.0, 200.0,
         datetime(2026, 3, 27, 12, 0, 5)),
    ]
    schema = StructType([
        StructField("product_id",  StringType()),
        StructField("price",       DoubleType()),
        StructField("bid",         DoubleType()),
        StructField("ask",         DoubleType()),
        StructField("volume_24h",  DoubleType()),
        StructField("event_time",  TimestampType()),
    ])
    return spark.createDataFrame(data, schema)


# =====================================================
# CLEANING TESTS
# =====================================================
def apply_cleaning(df):
    from pyspark.sql.functions import upper, trim
    return df \
        .filter(col("product_id").isNotNull()) \
        .filter(col("price") > 0) \
        .filter(col("bid") > 0) \
        .filter(col("ask") > 0) \
        .filter(col("bid") <= col("ask")) \
        .withColumn("product_id", upper(trim(col("product_id"))))


def test_cleaning_removes_null_product_id(sample_df):
    cleaned = apply_cleaning(sample_df)
    null_count = cleaned.filter(col("product_id").isNull()).count()
    assert null_count == 0, "Null product_ids should be removed"


def test_cleaning_removes_negative_price(sample_df):
    cleaned = apply_cleaning(sample_df)
    bad_price = cleaned.filter(col("price") <= 0).count()
    assert bad_price == 0, "Negative prices should be removed"


def test_cleaning_removes_invalid_spread(sample_df):
    cleaned = apply_cleaning(sample_df)
    invalid_spread = cleaned.filter(col("bid") > col("ask")).count()
    assert invalid_spread == 0, "Rows where bid > ask should be removed"


def test_cleaning_uppercases_product_id(sample_df):
    cleaned = apply_cleaning(sample_df)
    lowercase = cleaned.filter(col("product_id") != col("product_id")).count()
    btc_rows = cleaned.filter(col("product_id") == "BTC-USD").count()
    assert btc_rows >= 1, "product_id should be uppercased"


def test_cleaning_row_count(sample_df):
    cleaned = apply_cleaning(sample_df)
    # 6 rows input, 3 bad rows removed = 3 valid rows
    assert cleaned.count() == 3, f"Expected 3 clean rows, got {cleaned.count()}"


# =====================================================
# ENRICHMENT TESTS
# =====================================================
def apply_enrichment(df):
    from pyspark.sql.functions import upper, trim, when, unix_timestamp, current_timestamp
    cleaned = apply_cleaning(df)
    return cleaned \
        .withColumn("spread", col("ask") - col("bid")) \
        .withColumn("spread_pct", (col("spread") / col("price")) * 100) \
        .withColumn("mid_price", (col("bid") + col("ask")) / 2) \
        .withColumn("is_suspicious_spread", col("spread_pct") > 10)


def test_enrichment_spread_calculated(sample_df):
    enriched = apply_enrichment(sample_df)
    row = enriched.filter(col("product_id") == "BTC-USD").first()
    expected_spread = round(row["ask"] - row["bid"], 2)
    assert round(row["spread"], 2) == expected_spread, "Spread should be ask - bid"


def test_enrichment_mid_price_calculated(sample_df):
    enriched = apply_enrichment(sample_df)
    row = enriched.filter(col("product_id") == "BTC-USD").first()
    expected_mid = (row["bid"] + row["ask"]) / 2
    assert row["mid_price"] == expected_mid, "Mid price should be (bid + ask) / 2"


def test_enrichment_suspicious_spread_flag(sample_df):
    enriched = apply_enrichment(sample_df)
    # All clean rows have tight spreads — none should be suspicious
    suspicious = enriched.filter(col("is_suspicious_spread") == True).count()
    assert suspicious == 0, "No suspicious spreads expected in clean data"


# =====================================================
# DEDUPLICATION TESTS
# =====================================================
def test_deduplication_removes_duplicates(spark):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    data = [
        ("BTC-USD", 84320.5, datetime(2026, 3, 27, 12, 0, 0)),
        ("BTC-USD", 84320.5, datetime(2026, 3, 27, 12, 0, 0)),  # duplicate
        ("ETH-USD", 3210.4,  datetime(2026, 3, 27, 12, 0, 1)),
    ]
    schema = StructType([
        StructField("product_id", StringType()),
        StructField("price",      DoubleType()),
        StructField("event_time", TimestampType()),
    ])
    df = spark.createDataFrame(data, schema)
    deduped = df.dropDuplicates(["product_id", "event_time", "price"])
    assert deduped.count() == 2, "Duplicate rows should be removed"