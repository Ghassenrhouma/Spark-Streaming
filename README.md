# Lab — Introduction to Spark Streaming

This lab is your **first hands-on introduction to Spark Structured Streaming**.

You will build a simple **Kafka → Spark Streaming pipeline** and apply basic transformations on streaming data.
The goal is to understand how Spark processes streams.


---

## Learning Objectives

By the end of this lab, you should be able to:

- Connect Spark Streaming to Kafka
- Read data continuously from a Kafka topic
- Understand what a streaming DataFrame is
- Apply simple transformations on streaming data
- Write streaming results to an output sink

---

## Concepts Covered

This lab focuses only on the following concepts:

- Kafka → Spark Streaming pipeline
- Spark Structured Streaming
- Streaming DataFrames
- Micro-batch execution model
- Basic transformations (`select`, `filter`)
- Writing streaming outputs


---

# Lab Overview

You will progressively build a streaming application that:

1. Reads events from a Kafka topic
2. Processes them using Spark Streaming
3. Writes the results continuously to an output sink

This lab builds directly on your previous Kafka labs.

---

## Requirements

Make sure you have:

- Docker
- Python 3.9+
- Kafka running via Docker
- Apache Spark available (local or Docker)

---

##  Lab Tasks

### 🔹 Task 1 — Start Kafka and Spark

- Start Kafka using Docker
- Start Spark
- Verify that both systems are running correctly

Goal: understand that **Kafka and Spark are two separate systems**.

---

### 🔹 Task 2 — Create a Spark Streaming Application

- Create a Spark session
- Configure Spark for streaming
- Prepare your application structure

Observe how Spark Streaming applications differ from batch applications.

---

### 🔹 Task 3 — Read Data from Kafka

- Connect Spark Streaming to a Kafka topic
- Read messages as a streaming DataFrame
- Inspect the schema of the streaming DataFrame

Key observation:
Kafka messages appear as rows with binary `key` and `value` columns.

---

### 🔹 Task 4 — Parse and Inspect the Stream

- Convert Kafka message values from bytes to strings
- Inspect incoming events
- Print the stream to the console

First insight:
Spark does not know or care where the data comes from once it is in a DataFrame.

---

### 🔹 Task 5 — Filter the Stream

- Apply one or more filter conditions
- Keep only events that match a criterion

Examples:
- Messages containing a specific keyword
- JSON events with a numeric field above a threshold

Key idea:
Transformations in Spark Streaming are the same as in batch Spark.

---

### 🔹 Task 6 — Write Streaming Results

- Write the processed stream to an output sink:
  - Console
  - Memory
  - File sink
- Observe continuous output over time

Notice how Spark processes data in **micro-batches**.

---

## Reflection Questions

Answer the following questions after completing the lab:

- How is Spark Streaming different from a Kafka consumer?
- What role does Kafka play in this pipeline?
- What role does Spark play?
- Why separate data storage (Kafka) from computation (Spark)?

---

## Expected Outcome

After this lab, you should:

- Have a working Kafka → Spark Streaming pipeline
- Understand Spark Streaming at a high level
- Be comfortable reading and transforming streaming data
