import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, to_date,
    window, count, udf, lit, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, MapType
)
from pyspark.ml.feature import VectorAssembler
import joblib

BASE_PATH = os.environ.get("PROJECT_BASE_PATH", "/opt/project")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = "transactions_pca"
ALERTS_TOPIC = "alerts"
RAW_PATH = os.path.join(BASE_PATH, "data", "raw", "transactions_pca")
CHECKPOINT_RAW = os.path.join(BASE_PATH, "checkpoints", "raw")
CHECKPOINT_ENRICHED = os.path.join(BASE_PATH, "checkpoints", "enriched")
CHECKPOINT_FRAUD_METRICS = os.path.join(BASE_PATH, "checkpoints", "fraud_metrics")
CHECKPOINT_ALERTS = os.path.join(BASE_PATH, "checkpoints", "alerts")
MODEL_DIR = os.path.join(BASE_PATH, "model")
EVENT_ENRICHED_PATH = os.path.join(BASE_PATH, "data", "transactions_enriched")
METRICS_PATH = os.path.join(BASE_PATH, "data", "metrics", "fraud_window_metrics")
ALERT_THRESHOLD = 0.7
FEATURE_COLUMNS = [f"V{i}" for i in range(1, 29)] + ["amount"]

for path in [
    RAW_PATH,
    CHECKPOINT_RAW,
    CHECKPOINT_ENRICHED,
    CHECKPOINT_FRAUD_METRICS,
    CHECKPOINT_ALERTS,
    EVENT_ENRICHED_PATH,
    METRICS_PATH,
]:
    os.makedirs(path, exist_ok=True)

spark = SparkSession.builder.appName("FraudStreamingPipeline").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

model_files = [
    f for f in os.listdir(MODEL_DIR)
    if f.startswith("fraud_model_") and f.endswith(".pkl")
]
if not model_files:
    raise FileNotFoundError(f"No model files found in {MODEL_DIR}")

latest_model = sorted(model_files)[-1]
MODEL_VERSION = latest_model.replace("fraud_model_", "").replace(".pkl", "")
print(f"Loading model version: {MODEL_VERSION}")

model = joblib.load(os.path.join(MODEL_DIR, latest_model))
broadcast_model = spark.sparkContext.broadcast(model)

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("event_time", StringType()),
    StructField("amount", DoubleType()),
    StructField("features", MapType(StringType(), DoubleType())),
    StructField("label", IntegerType()),
    StructField("ingestion_time", StringType()),
])

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("event_time"))
)

parsed_df = parsed_df.withColumn("event_date", to_date(col("event_time")))

feature_df = parsed_df
for f in FEATURE_COLUMNS[:-1]:
    feature_df = feature_df.withColumn(f, col("features").getItem(f))
feature_df = feature_df.withColumn("amount", col("amount"))

assembler = VectorAssembler(
    inputCols=FEATURE_COLUMNS,
    outputCol="feature_vector"
)

assembled_df = assembler.transform(feature_df)

def score_transaction(feature_vector):
    mdl = broadcast_model.value
    return float(mdl.predict_proba([feature_vector])[0][1])

score_udf = udf(score_transaction, DoubleType())

scored_df = (
    assembled_df
    .withColumn("fraud_score", score_udf(col("feature_vector")))
    .withColumn("predicted_label", (col("fraud_score") >= ALERT_THRESHOLD).cast("int"))
    .withColumn("model_version", lit(MODEL_VERSION))
)

scored_df.writeStream \
    .format("parquet") \
    .partitionBy("event_date") \
    .option("path", EVENT_ENRICHED_PATH) \
    .option("checkpointLocation", CHECKPOINT_ENRICHED) \
    .outputMode("append") \
    .start()

fraud_agg_df = (
    scored_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(window(col("event_time"), "10 minutes"))
    .agg(
        count("*").alias("txn_count"),
        spark_sum("predicted_label").alias("fraud_count"),
    )
    .withColumn("fraud_rate", col("fraud_count") / col("txn_count"))
)

fraud_agg_query = (
    fraud_agg_df
    .writeStream
    .format("parquet")
    .option("path", METRICS_PATH)
    .option("checkpointLocation", CHECKPOINT_FRAUD_METRICS) \
    .outputMode("append")
    .start()
)

alerts_df = (
    scored_df
    .filter(col("predicted_label") == 1)
    .select(
        "transaction_id",
        "event_time",
        "amount",
        "fraud_score",
        "model_version",
    )
)

alerts_query = (
    alerts_df
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", ALERTS_TOPIC)
    .option("checkpointLocation", CHECKPOINT_ALERTS)
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()
