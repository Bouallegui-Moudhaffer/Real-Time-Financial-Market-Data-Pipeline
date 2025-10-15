"""
Spark Structured Streaming job:
- Kafka -> parse JSON trade schema
- Event-time watermark + dedupe on 'key'
- Windows: 1s & 1m -> OHLCV, VWAP
- Indicators (1m): vwap_1m, vol_1m (Parkinson), pct_change_1m
- Upsert to Cassandra tables with foreachBatch
- (NEW) Optional signals -> Kafka (pct_change_1m threshold)
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T


def get_env(name: str, default: str) -> str:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v


def build_spark() -> SparkSession:
    cassandra_host = get_env("CASSANDRA_HOST", "cassandra")
    cassandra_port = get_env("CASSANDRA_PORT", "9042")
    app_name = get_env("SPARK_APP_NAME", "rtmkt-ohlcv")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", int(get_env("SPARK_SHUFFLE_PARTS", "8")))
        .config("spark.cassandra.connection.host", cassandra_host)
        .config("spark.cassandra.connection.port", cassandra_port)
        # Workaround for Kafka Admin-based offset fetch NPE in some Spark 3.5.x combos
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "true")
        .getOrCreate()
    )
    return spark


def read_kafka(spark: SparkSession) -> DataFrame:
    brokers = get_env("KAFKA_BROKERS", "kafka:9092")
    topic = get_env("KAFKA_TOPIC", "market.trades.raw")
    starting = os.getenv("KAFKA_STARTING_OFFSETS", os.getenv("KAFKA_STARTING_OFF", "latest"))
    max_offsets = int(get_env("SPARK_MAX_OFFSETS_PER_TRIGGER", "5000"))

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", starting)
        .option("maxOffsetsPerTrigger", max_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_dedupe(df_kafka: DataFrame) -> DataFrame:
    trade_schema = T.StructType(
        [
            T.StructField("type", T.StringType(), False),
            T.StructField("source", T.StringType(), False),
            T.StructField("ingest_ts", T.StringType(), False),
            T.StructField("event_ts", T.LongType(), False),  # ms epoch
            T.StructField("symbol", T.StringType(), False),
            T.StructField("price", T.DoubleType(), False),
            T.StructField("volume", T.DoubleType(), False),
            T.StructField("conditions", T.ArrayType(T.StringType()), True),
            T.StructField("key", T.StringType(), False),
        ]
    )

    df = df_kafka.selectExpr("CAST(value AS STRING) AS json_str")
    df = df.select(F.from_json("json_str", trade_schema).alias("j")).select("j.*")

    # event_time (UTC)
    df = df.withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("event_ts") / F.lit(1000.0))))
    df = df.filter(
        F.col("symbol").isNotNull()
        & F.col("price").isNotNull()
        & F.col("volume").isNotNull()
        & F.col("event_time").isNotNull()
        & (F.col("type") == F.lit("trade"))
    )

    # Watermark + dedupe
    watermark_sec = int(get_env("SPARK_WATERMARK_SECONDS", "30"))
    df = df.withWatermark("event_time", f"{watermark_sec} seconds")
    df = df.dropDuplicates(["key"])

    return df


def agg_ohlcv(df_trades: DataFrame, window_dur: str) -> DataFrame:
    w = F.window(F.col("event_time"), window_dur)
    price = F.col("price")
    vol = F.col("volume")

    s_min = F.min(F.struct(F.col("event_time").alias("t"), price.alias("p"))).alias("s_min")
    s_max = F.max(F.struct(F.col("event_time").alias("t"), price.alias("p"))).alias("s_max")

    sum_vol = F.sum(vol).alias("sum_vol")
    sum_pv = F.sum(price * vol).alias("sum_pv")
    trade_cnt = F.count(F.lit(1)).alias("trade_count")
    high = F.max(price).alias("high")
    low = F.min(price).alias("low")

    agg = (
        df_trades.groupBy(F.col("symbol"), w)
        .agg(s_min, s_max, high, low, sum_vol, sum_pv, trade_cnt, F.max("event_time").alias("last_event_ts"))
        .select(
            F.col("symbol"),
            F.col("window").start.alias("window_start"),
            F.col("window").end.alias("window_end"),
            F.col("s_min").getField("p").alias("open"),
            F.col("high").alias("high"),
            F.col("low").alias("low"),
            F.col("s_max").getField("p").alias("close"),
            F.col("sum_vol").cast("double").alias("volume"),
            F.col("trade_count").cast("long").alias("trade_count"),
            (F.when(F.col("sum_vol") > 0, F.col("sum_pv") / F.col("sum_vol")).otherwise(F.lit(None))).alias("vwap"),
            F.col("last_event_ts").alias("last_event_ts"),
        )
    )

    agg = agg.withColumn("bucket_date", F.to_date(F.col("window_start")))
    agg = agg.withColumn("src", F.lit("spark-ss-3.5.6"))
    return agg


def write_ohlcv(df: DataFrame, table: str, keyspace: str, checkpoint: str, trigger: str, query_name: str):
    def _writer(batch_df: DataFrame, batch_id: int):
        (
            batch_df.write.format("org.apache.spark.sql.cassandra")
            .options(keyspace=keyspace, table=table)
            .mode("append")
            .save()
        )

    return (
        df.writeStream.outputMode("update")
        .foreachBatch(_writer)
        .option("checkpointLocation", checkpoint)
        .queryName(query_name)
        .trigger(processingTime=trigger)
        .start()
    )


def write_indicators_from_ohlcv_1m(df_1m: DataFrame, keyspace: str, checkpoint: str, trigger: str, query_name: str):
    ln_hl = F.log(F.col("high") / F.col("low"))
    vol_1m = F.sqrt((ln_hl * ln_hl) / (F.lit(4.0) * F.log(F.lit(2.0)))).alias("vol_1m")
    indicators = df_1m.select(
        "symbol",
        "bucket_date",
        "window_start",
        "window_end",
        F.col("vwap").alias("vwap_1m"),
        vol_1m,
    )

    def _writer(batch_df: DataFrame, batch_id: int):
        (
            batch_df.write.format("org.apache.spark.sql.cassandra")
            .options(keyspace=keyspace, table="indicators_1m")
            .mode("append")
            .save()
        )

    return (
        indicators.writeStream.outputMode("update")
        .foreachBatch(_writer)
        .option("checkpointLocation", checkpoint)
        .queryName(query_name)
        .trigger(processingTime=trigger)
        .start()
    )


def write_signals_kafka(df_1m: DataFrame, checkpoint_root: str, trigger: str):
    """
    Minimal, robust signal: emit when abs(pct_change_1m) >= threshold.
    Writes JSON to Kafka topic.
    """
    enable = get_env("ENABLE_SIGNALS", "1").lower() in ("1", "true", "yes", "on")
    if not enable:
        return None

    brokers = get_env("KAFKA_BROKERS", "kafka:9092")
    topic = get_env("SIGNALS_TOPIC", "market.signals")
    ck = os.path.join(checkpoint_root, "signals")
    threshold = float(get_env("SIGNAL_PCT_CHANGE", "0.01"))  # 1%

    signals = (
        df_1m
        .filter(F.abs(F.col("pct_change_1m")) >= F.lit(threshold))
        .select(
            F.col("symbol").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    F.lit("pct_change").alias("type"),
                    F.col("symbol"),
                    F.col("window_start").alias("ts"),
                    F.round(F.col("pct_change_1m"), 6).alias("pct_change"),
                    F.col("close"),
                    F.col("vwap"),
                    F.col("volume"),
                    F.col("src"),
                )
            ).alias("value")
        )
    )

    return (
        signals.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("topic", topic)
        .option("checkpointLocation", ck)
        .queryName("signals_kafka_writer")
        .outputMode("append")  # Kafka sink supports append
        .trigger(processingTime=trigger)
        .start()
    )


def main():
    spark = build_spark()

    # Read + parse + dedupe
    df_kafka = read_kafka(spark)
    df = parse_and_dedupe(df_kafka)

    # Aggregations
    df_1s = agg_ohlcv(df, "1 second")
    df_1m = agg_ohlcv(df, "1 minute").withColumn(
        "pct_change_1m", ((F.col("close") - F.col("open")) / F.col("open"))
    )

    # Settings
    keyspace = get_env("CASSANDRA_KEYSPACE", "market_ks")
    checkpoint_root = get_env("CHECKPOINT_ROOT", "/tmp/streaming-checkpoints")
    ck_1s = get_env("CHECKPOINT_1S", os.path.join(checkpoint_root, "ohlcv_1s"))
    ck_1m = get_env("CHECKPOINT_1M", os.path.join(checkpoint_root, "ohlcv_1m"))
    ck_ind = get_env("CHECKPOINT_IND", os.path.join(checkpoint_root, "indicators_1m"))
    trigger = get_env("TRIGGER_INTERVAL", "5 seconds")

    # Write streams (one writer per DF; each with its own checkpoint + queryName)
    q1 = write_ohlcv(
        df_1s.select(
            "symbol","bucket_date","window_start","window_end",
            "open","high","low","close","volume","trade_count",
            "vwap","last_event_ts","src",
        ),
        table="ohlcv_1s", keyspace=keyspace, checkpoint=ck_1s, trigger=trigger, query_name="ohlcv_1s_writer"
    )

    q2 = write_ohlcv(
        df_1m.select(
            "symbol","bucket_date","window_start","window_end",
            "open","high","low","close","volume","trade_count",
            "vwap","pct_change_1m","last_event_ts","src",
        ),
        table="ohlcv_1m", keyspace=keyspace, checkpoint=ck_1m, trigger=trigger, query_name="ohlcv_1m_writer"
    )

    q3 = write_indicators_from_ohlcv_1m(
        df_1m, keyspace=keyspace, checkpoint=ck_ind, trigger=trigger, query_name="indicators_1m_writer"
    )

    # NEW: Kafka signals (optional; defaults ON)
    q4 = write_signals_kafka(df_1m, checkpoint_root=checkpoint_root, trigger=trigger)

    spark.sparkContext.setLogLevel(get_env("SPARK_LOG_LEVEL", "WARN"))

    # Await all active queries
    if q4 is not None:
        q1.awaitTermination(); q2.awaitTermination(); q3.awaitTermination(); q4.awaitTermination()
    else:
        q1.awaitTermination(); q2.awaitTermination(); q3.awaitTermination()


if __name__ == "__main__":
    main()
