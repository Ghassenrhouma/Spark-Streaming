from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_timestamp,
    sum as spark_sum, avg, count, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
import sys
import threading
import time
from collections import deque
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
from datetime import datetime


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


class RealtimeVisualizer:
    """Real-time visualization using file-based matplotlib"""
    
    def __init__(self, output_dir='./visualizations', max_points=50):
        self.output_dir = output_dir
        self.max_points = max_points
        self.lock = threading.Lock()
        
        # Data storage
        self.high_value_amounts = deque(maxlen=max_points)
        self.high_value_times = deque(maxlen=max_points)
        self.user_stats = {}
        self.batch_count = 0
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"✓ Visualizer initialized (output: {output_dir})")
    
    def add_high_value_transaction(self, amount, timestamp):
        """Add a high value transaction"""
        with self.lock:
            self.high_value_amounts.append(float(amount))
            self.high_value_times.append(timestamp)
    
    def update_user_stats(self, user_stats_dict):
        """Update user statistics"""
        with self.lock:
            self.user_stats = user_stats_dict.copy()
            self.batch_count += 1
    
    def generate_plots(self):
        """Generate and save visualization plots"""
        with self.lock:
            try:
                # Create figure with subplots
                fig = plt.figure(figsize=(16, 10))
                gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Plot 1: High Value Transactions Timeline
                ax1 = fig.add_subplot(gs[0, :])
                if len(self.high_value_amounts) > 0:
                    x = list(range(len(self.high_value_amounts)))
                    y = list(self.high_value_amounts)
                    ax1.plot(x, y, 'o-', color='#2ecc71', linewidth=2, markersize=6)
                    ax1.fill_between(x, y, alpha=0.3, color='#2ecc71')
                    ax1.set_title('High Value Transactions Timeline (>$200)', 
                                 fontsize=14, fontweight='bold', pad=15)
                    ax1.set_xlabel('Transaction Number', fontsize=11)
                    ax1.set_ylabel('Amount ($)', fontsize=11)
                    ax1.grid(True, alpha=0.3)
                    
                    # Add statistics text
                    avg_amount = sum(y) / len(y)
                    max_amount = max(y)
                    min_amount = min(y)
                    ax1.text(0.02, 0.98, 
                            f'Count: {len(y)}\nAvg: ${avg_amount:.2f}\nMax: ${max_amount:.2f}\nMin: ${min_amount:.2f}',
                            transform=ax1.transAxes, fontsize=10,
                            verticalalignment='top',
                            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
                else:
                    ax1.text(0.5, 0.5, ' Waiting for high-value transactions (>$200)...',
                            ha='center', va='center', fontsize=14, color='gray',
                            bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.3))
                    ax1.set_title('High Value Transactions Timeline (>$200)', 
                                 fontsize=14, fontweight='bold')
                ax1.set_xlim(-1, self.max_points)
                
                # Plot 2: Transaction Count per User
                ax2 = fig.add_subplot(gs[1, 0])
                if self.user_stats:
                    users = list(self.user_stats.keys())
                    counts = [self.user_stats[u]['count'] for u in users]
                    
                    colors = ['#3498db' if i % 2 == 0 else '#2980b9' for i in range(len(users))]
                    bars = ax2.bar(users, counts, color=colors, alpha=0.8, edgecolor='black')
                    ax2.set_title('Transaction Count per User', fontsize=12, fontweight='bold')
                    ax2.set_xlabel('User ID', fontsize=10)
                    ax2.set_ylabel('Number of Transactions', fontsize=10)
                    ax2.tick_params(axis='x', rotation=45)
                    ax2.grid(True, alpha=0.3, axis='y')
                    
                    # Add value labels
                    for bar in bars:
                        height = bar.get_height()
                        ax2.text(bar.get_x() + bar.get_width()/2., height,
                                f'{int(height)}',
                                ha='center', va='bottom', fontsize=9, fontweight='bold')
                else:
                    ax2.text(0.5, 0.5, '⏳ Waiting for user data...',
                            ha='center', va='center', fontsize=12, color='gray',
                            bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.3))
                    ax2.set_title('Transaction Count per User', fontsize=12, fontweight='bold')
                
                # Plot 3: Total Amount per User (Horizontal Bar)
                ax3 = fig.add_subplot(gs[1, 1])
                if self.user_stats:
                    users = list(self.user_stats.keys())
                    totals = [self.user_stats[u]['total'] for u in users]
                    
                    # Sort by total amount
                    sorted_data = sorted(zip(users, totals), key=lambda x: x[1], reverse=True)
                    users_sorted = [x[0] for x in sorted_data]
                    totals_sorted = [x[1] for x in sorted_data]
                    
                    colors = ['#e74c3c' if i % 2 == 0 else '#c0392b' for i in range(len(users_sorted))]
                    bars = ax3.barh(users_sorted, totals_sorted, color=colors, alpha=0.8, edgecolor='black')
                    ax3.set_title('Total Amount per User', fontsize=12, fontweight='bold')
                    ax3.set_xlabel('Total Amount ($)', fontsize=10)
                    ax3.set_ylabel('User ID', fontsize=10)
                    ax3.grid(True, alpha=0.3, axis='x')
                    
                    # Add value labels
                    for i, bar in enumerate(bars):
                        width = bar.get_width()
                        ax3.text(width, bar.get_y() + bar.get_height()/2.,
                                f' ${width:.2f}',
                                ha='left', va='center', fontsize=9, fontweight='bold')
                else:
                    ax3.text(0.5, 0.5, ' Waiting for user data...',
                            ha='center', va='center', fontsize=12, color='gray',
                            bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.3))
                    ax3.set_title('Total Amount per User', fontsize=12, fontweight='bold')
                
                # Plot 4: Average Transaction Amount per User
                ax4 = fig.add_subplot(gs[2, 0])
                if self.user_stats:
                    users = list(self.user_stats.keys())
                    averages = [self.user_stats[u]['avg'] for u in users]
                    
                    colors = ['#9b59b6' if i % 2 == 0 else '#8e44ad' for i in range(len(users))]
                    bars = ax4.bar(users, averages, color=colors, alpha=0.8, edgecolor='black')
                    ax4.set_title('Average Transaction Amount per User', fontsize=12, fontweight='bold')
                    ax4.set_xlabel('User ID', fontsize=10)
                    ax4.set_ylabel('Average Amount ($)', fontsize=10)
                    ax4.tick_params(axis='x', rotation=45)
                    ax4.grid(True, alpha=0.3, axis='y')
                    
                    # Add value labels
                    for bar in bars:
                        height = bar.get_height()
                        ax4.text(bar.get_x() + bar.get_width()/2., height,
                                f'${height:.2f}',
                                ha='center', va='bottom', fontsize=9, fontweight='bold')
                else:
                    ax4.text(0.5, 0.5, ' Waiting for user data...',
                            ha='center', va='center', fontsize=12, color='gray',
                            bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.3))
                    ax4.set_title('Average Transaction Amount per User', fontsize=12, fontweight='bold')
                
                # Plot 5: Summary Statistics
                ax5 = fig.add_subplot(gs[2, 1])
                ax5.axis('off')
                
                # Create summary text
                summary_text = "STREAMING STATISTICS\n" + "="*40 + "\n\n"
                
                if self.user_stats:
                    total_transactions = sum(s['count'] for s in self.user_stats.values())
                    total_amount = sum(s['total'] for s in self.user_stats.values())
                    avg_per_transaction = total_amount / total_transactions if total_transactions > 0 else 0
                    
                    summary_text += f" Active Users: {len(self.user_stats)}\n"
                    summary_text += f" Total Transactions: {total_transactions}\n"
                    summary_text += f" Total Amount: ${total_amount:.2f}\n"
                    summary_text += f" Avg per Transaction: ${avg_per_transaction:.2f}\n\n"
                    
                    summary_text += f" High-Value Txns: {len(self.high_value_amounts)}\n"
                    
                    if len(self.high_value_amounts) > 0:
                        high_avg = sum(self.high_value_amounts) / len(self.high_value_amounts)
                        high_total = sum(self.high_value_amounts)
                        summary_text += f" Avg High-Value: ${high_avg:.2f}\n"
                        summary_text += f" Total High-Value: ${high_total:.2f}\n"
                    
                    summary_text += f"\n Batches Processed: {self.batch_count}\n"
                    summary_text += f" Last Update: {datetime.now().strftime('%H:%M:%S')}"
                else:
                    summary_text += " Waiting for data...\n\n"
                    summary_text += "Pipeline is active and monitoring\n"
                    summary_text += "Kafka topic for new transactions.\n\n"
                    summary_text += f" Batches Processed: {self.batch_count}\n"
                    summary_text += f" Current Time: {datetime.now().strftime('%H:%M:%S')}"
                
                ax5.text(0.1, 0.9, summary_text, 
                        transform=ax5.transAxes,
                        fontsize=11,
                        verticalalignment='top',
                        fontfamily='monospace',
                        bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
                
                # Add main title
                fig.suptitle(' Kafka-Spark Streaming Dashboard - Real-Time Analytics', 
                           fontsize=16, fontweight='bold', y=0.98)
                
                # Save with timestamp
                filename = os.path.join(self.output_dir, f'dashboard_{timestamp}.png')
                fig.savefig(filename, dpi=150, bbox_inches='tight', facecolor='white')
                
                # Save as "latest"
                latest_filename = os.path.join(self.output_dir, 'dashboard_latest.png')
                fig.savefig(latest_filename, dpi=150, bbox_inches='tight', facecolor='white')
                
                # Close figure
                plt.close(fig)
                
                # Verify file was created
                if os.path.exists(latest_filename):
                    file_size = os.path.getsize(latest_filename)
                    print(f" Dashboard updated: {latest_filename} ({file_size:,} bytes)")
                else:
                    print(f" Warning: Dashboard file not created!")
                
                return filename
                
            except Exception as e:
                print(f" Error generating plots: {e}")
                import traceback
                traceback.print_exc()
                plt.close('all')
                return None


class DataCollector:
    """Thread-safe data collector for streaming metrics"""
    def __init__(self, visualizer=None):
        self.high_value_transactions = []
        self.user_statistics = {}
        self.lock = threading.Lock()
        self.visualizer = visualizer
    
    def add_high_value_transaction(self, transaction_id, user_id, amount, timestamp):
        with self.lock:
            self.high_value_transactions.append({
                'transaction_id': transaction_id,
                'user_id': user_id,
                'amount': amount,
                'timestamp': timestamp
            })
            
            # Update visualizer
            if self.visualizer:
                self.visualizer.add_high_value_transaction(amount, timestamp)
            
            # Keep only last 100 transactions
            if len(self.high_value_transactions) > 100:
                self.high_value_transactions = self.high_value_transactions[-100:]
    
    def update_user_stats(self, user_id, count, total, avg):
        with self.lock:
            self.user_statistics[user_id] = {
                'count': count,
                'total': total,
                'avg': avg
            }
    
    def update_all_user_stats(self, stats_dict):
        """Update all user stats at once"""
        with self.lock:
            self.user_statistics = stats_dict.copy()
            
            # Update visualizer
            if self.visualizer:
                self.visualizer.update_user_stats(stats_dict)
    
    def get_summary(self):
        with self.lock:
            return {
                'total_high_value': len(self.high_value_transactions),
                'total_users': len(self.user_statistics),
                'high_value_transactions': self.high_value_transactions[-10:],
                'user_stats': dict(self.user_statistics)
            }


# Global instances
visualizer = None
data_collector = None


def process_high_value_batch(batch_df, batch_id):
    """Process each batch of high value transactions"""
    global data_collector
    
    try:
        row_count = batch_df.count()
        
        if row_count > 0:
            print(f"\n{'='*70}")
            print(f"[Batch {batch_id}] HIGH VALUE TRANSACTIONS (> $200)")
            print(f"{'='*70}")
            
            # Collect data (limit display to first 5)
            transactions = batch_df.limit(5).collect()
            
            for row in transactions:
                data_collector.add_high_value_transaction(
                    row['transaction_id'],
                    row['user_id'],
                    float(row['amount']),
                    str(row['timestamp'])
                )
                
                print(f"   {row['transaction_id'][:12]}... | "
                      f"User: {row['user_id']} | ${row['amount']:.2f}")
            
            if row_count > 5:
                # Still collect all remaining transactions for visualization
                remaining = batch_df.limit(row_count).collect()[5:]
                for row in remaining:
                    data_collector.add_high_value_transaction(
                        row['transaction_id'],
                        row['user_id'],
                        float(row['amount']),
                        str(row['timestamp'])
                    )
                print(f"  ... and {row_count - 5} more transactions")
            
            print(f"\n✓ Processed {row_count} high-value transactions")
            print(f"{'='*70}\n")
            
    except Exception as e:
        print(f" Error processing high value batch: {e}")
        import traceback
        traceback.print_exc()


def process_user_stats_batch(batch_df, batch_id):
    """Process each batch of user statistics"""
    global data_collector, visualizer
    
    try:
        row_count = batch_df.count()
        
        if row_count > 0:
            print(f"\n{'='*70}")
            print(f"[Batch {batch_id}] USER STATISTICS SUMMARY")
            print(f"{'='*70}")
            
            # Collect ALL stats for visualization
            all_stats = batch_df.collect()
            stats_dict = {}
            
            for row in all_stats:
                stats_dict[row['user_id']] = {
                    'count': int(row['transaction_count']),
                    'total': float(row['total_amount']),
                    'avg': float(row['avg_amount'])
                }
            
            # Update collector with all stats
            data_collector.update_all_user_stats(stats_dict)
            
            # Display top 5
            top_stats = batch_df.orderBy(col("total_amount").desc()).limit(5).collect()
            
            for row in top_stats:
                print(f"   {row['user_id']} | "
                      f"Count: {row['transaction_count']} | "
                      f"Total: ${row['total_amount']:.2f} | "
                      f"Avg: ${row['avg_amount']:.2f}")
            
            if row_count > 5:
                print(f"  ... and {row_count - 5} more users")
            
            # Generate visualization
            print(f"\n Generating visualization...")
            if visualizer:
                visualizer.generate_plots()
            
            summary = data_collector.get_summary()
            print(f"\n   OVERALL SUMMARY:")
            print(f"     Total high-value transactions: {summary['total_high_value']}")
            print(f"     Total active users: {summary['total_users']}")
            print(f"{'='*70}\n")
            
    except Exception as e:
        print(f" Error processing user stats batch: {e}")
        import traceback
        traceback.print_exc()


def save_visualization_data():
    """Save collected data to CSV files"""
    import csv
    from datetime import datetime
    
    try:
        summary = data_collector.get_summary()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if summary['high_value_transactions']:
            filename = f'high_value_transactions_{timestamp}.csv'
            with open(filename, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['transaction_id', 'user_id', 'amount', 'timestamp'])
                writer.writeheader()
                writer.writerows(summary['high_value_transactions'])
            print(f"✓ Saved {filename}")
        
        if summary['user_stats']:
            filename = f'user_statistics_{timestamp}.csv'
            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['user_id', 'transaction_count', 'total_amount', 'avg_amount'])
                for user_id, stats in summary['user_stats'].items():
                    writer.writerow([user_id, stats['count'], stats['total'], stats['avg']])
            print(f"✓ Saved {filename}")
        
    except Exception as e:
        print(f" Error saving data: {e}")


def main():
    """Main entry point"""
    global visualizer, data_collector
    
    setup_windows_environment()

    print("\nCreating Spark Session...")
    
    spark = SparkSession.builder \
        .appName("KafkaSparkStreamingPipeline") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"✓ Spark Session created")
    print(f"  - Version: {spark.version}")
    

    # Initialize visualizer and data collector
    print("\n Initializing visualization system...")
    visualizer = RealtimeVisualizer(output_dir='./visualizations', max_points=50)
    data_collector = DataCollector(visualizer=visualizer)
    
    # Generate initial empty dashboard
    print(" Generating initial dashboard...")
    visualizer.generate_plots()
    print("✓ Visualization system ready\n")

    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "my-first-topic"
    
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])
    
    print("Reading from Kafka...")
    
    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("✓ Connected to Kafka\n")
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        spark.stop()
        return
    
    # Process data
    string_df = kafka_df.selectExpr(
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as json_value",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp"
    )
    
    parsed_df = string_df.withColumn(
        "transaction_data",
        from_json(col("json_value"), transaction_schema)
    ).select(
        "kafka_timestamp",
        "transaction_data.*"
    )
    
    processed_df = parsed_df \
        .withColumn("amount", col("amount").cast(DoubleType())) \
        .withColumn("transaction_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    # Query 1: High Value Transactions
    high_value_transactions = processed_df \
        .filter(col("amount") > 200) \
        .select("transaction_id", "user_id", "amount", "timestamp")
    
    query1 = high_value_transactions \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(process_high_value_batch) \
        .queryName("HighValueTransactions") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Query 2: User Statistics
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
        .foreachBatch(process_user_stats_batch) \
        .queryName("UserStatistics") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("=" * 70)
    print("✓ STREAMING PIPELINE ACTIVE")
    print("=" * 70)
    print("\n Visualizations:")
    print("  • Location: ./visualizations/")
    print("  • Latest: dashboard_latest.png")
    print("  • Updates: Every 10 seconds (when data arrives)")
    print("\n View the dashboard:")
    print("   Windows: start .\\visualizations\\dashboard_latest.png")
    print("   Or open with any image viewer that auto-refreshes")
    print("\n⏳ Processing... (Ctrl+C to stop)\n")
    
    try:
        last_heartbeat = time.time()
        
        while query1.isActive and query2.isActive:
            time.sleep(5)
            
            # Heartbeat every 15 seconds
            if time.time() - last_heartbeat > 15:
                print(f" Pipeline active [{datetime.now().strftime('%H:%M:%S')}] - "
                      f"Dashboard: ./visualizations/dashboard_latest.png")
                last_heartbeat = time.time()
                
    except KeyboardInterrupt:
        print("\n\n Stopping...")
        
        print("\n Saving final data...")
        save_visualization_data()
        
        print(" Generating final visualization...")
        if visualizer:
            visualizer.generate_plots()
        
        query1.stop()
        query2.stop()
        spark.stop()
        print("\n✓ Stopped successfully")
        print(f" Check ./visualizations/ for all generated dashboards\n")


if __name__ == "__main__":
    main()