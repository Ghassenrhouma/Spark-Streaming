from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_timestamp,
    sum as spark_sum, avg, count, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
import sys


def setup_windows_environment():
    """Setup Windows-specific Hadoop environment"""
    if sys.platform != "win32":
        return
    
    print("Configuring for Windows environment...")
    
    java_home = os.environ.get('JAVA_HOME', '')
    if java_home.endswith('\\bin'):
        java_home = java_home[:-4]
        os.environ['JAVA_HOME'] = java_home
    
    hadoop_home = os.path.join(os.getcwd(), 'hadoop')
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['hadoop.home.dir'] = hadoop_home
    
    hadoop_bin = os.path.join(hadoop_home, 'bin')
    os.makedirs(hadoop_bin, exist_ok=True)
    
    print(f"✓ HADOOP_HOME set to: {hadoop_home}")
    
    files_to_download = {
        'winutils.exe': 'https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe',
        'hadoop.dll': 'https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/hadoop.dll'
    }
    
    for filename, url in files_to_download.items():
        filepath = os.path.join(hadoop_bin, filename)
        if not os.path.exists(filepath):
            print(f"⚠ {filename} not found. Downloading...")
            try:
                import urllib.request
                urllib.request.urlretrieve(url, filepath)
                print(f"✓ Successfully downloaded {filename}")
            except Exception as e:
                print(f"✗ Could not download {filename}: {e}")
        else:
            print(f"✓ {filename} found")
    
    if hadoop_bin not in os.environ.get('PATH', ''):
        os.environ['PATH'] = hadoop_bin + ';' + os.environ.get('PATH', '')
        print(f"✓ Added {hadoop_bin} to PATH")


def main():
    """
    Main entry point of the Spark Streaming application.
    """
    
    setup_windows_environment()

    print("\nCreating Spark Session...")
    
    spark = SparkSession.builder \
        .appName("KafkaSparkStreamingPipeline") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✓ Spark Session created successfully")
    print(f"  - Spark Version: {spark.version}")
    

    print("\nDefining Kafka connection parameters...")
    
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "my-first-topic"
    
    print(f"✓ Kafka parameters defined")
    

    print("\nDefining transaction schema...")
    
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])
    
    print("✓ Schema defined")
    

    print("\nReading streaming data from Kafka...")
    
    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("✓ Successfully connected to Kafka stream")
        
    except Exception as e:
        print(f"✗ Error reading from Kafka: {str(e)}")
        spark.stop()
        return
    

    print("\nProcessing streaming data...")
    
    # Convert binary to string
    string_df = kafka_df.selectExpr(
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as json_value",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp"
    )
    
    # Parse JSON
    parsed_df = string_df.withColumn(
        "transaction_data",
        from_json(col("json_value"), transaction_schema)
    ).select(
        "kafka_timestamp",
        "transaction_data.*"
    )
    
    # Convert types
    processed_df = parsed_df \
        .withColumn("amount", col("amount").cast(DoubleType())) \
        .withColumn("transaction_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    print("✓ Stream processing configured")
    

    # FILTERING: High Value Transactions (> $200)
    print("\n" + "=" * 70)
    print("FILTERING: High-Value Transactions (> $200)")
    print("=" * 70)
    
    high_value_transactions = processed_df \
        .filter(col("amount") > 200) \
        .select("transaction_id", "user_id", "amount", "timestamp")
    
    query1 = high_value_transactions \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 20) \
        .queryName("HighValueTransactions") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    print("✓ Filtering query started")
    

    # AGGREGATION: Total and Average per User
    print("\n" + "=" * 70)
    print("AGGREGATION: Statistics per User")
    print("=" * 70)
    
    user_stats = processed_df \
        .groupBy("user_id") \
        .agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        ) \
        .select(
            "user_id",
            "transaction_count",
            spark_round("total_amount", 2).alias("total_amount"),
            spark_round("avg_amount", 2).alias("avg_amount")
        )
    
    query2 = user_stats \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .queryName("UserStatistics") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    print("✓ Aggregation query started")
    

    print("\n" + "=" * 70)
    print("✓ Streaming pipeline started successfully!")
    print("=" * 70)
    print("\nActive Queries:")
    print("  1. High Value Transactions Filter (amount > $200)")
    print("  2. User Statistics Aggregation (count, total, average)")
    print("\n⏳ Processing data... (Press Ctrl+C to stop)\n")
    
    
    # Wait for all queries to terminate
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n\n Stopping all streaming queries...")
        query1.stop()
        query2.stop()
        spark.stop()
        print("✓ Application stopped successfully")


if __name__ == "__main__":
    main()