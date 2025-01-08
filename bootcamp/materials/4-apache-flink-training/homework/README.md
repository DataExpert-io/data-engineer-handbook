# README

## Overview
This project implements a Flink job that processes web event logs streamed from a Kafka topic. The job sessionizes events based on a session gap of 5 minutes (300 seconds) and writes the results to a PostgreSQL database.

### Features
- **Sessionization**: Groups events by IP and host, using a session window with a gap of 5 minutes.
- **Kafka Integration**: Reads events from a Kafka topic.
- **PostgreSQL Sink**: Writes the sessionized data into PostgreSQL for further analysis.
- **Error Logging**: Uses a logging framework to capture errors with context.

---

## Requirements
- **Docker Compose** for running the Flink job and dependencies.
- **Apache Kafka** for the event stream.
- **PostgreSQL** for persisting the sessionized data.

### Environment Variables
Ensure the following environment variables are set before running the job:
- `KAFKA_WEB_TRAFFIC_KEY`: Kafka user key.
- `KAFKA_WEB_TRAFFIC_SECRET`: Kafka user secret.
- `KAFKA_URL`: Kafka broker URL.
- `KAFKA_TOPIC`: Kafka topic name.
- `KAFKA_GROUP`: Kafka consumer group ID.
- `POSTGRES_URL`: JDBC URL for PostgreSQL.
- `POSTGRES_USER`: PostgreSQL username (default: `postgres`).
- `POSTGRES_PASSWORD`: PostgreSQL password (default: `postgres`).

---

## Running the Flink Job
1. Ensure Docker Compose is running:
   ```bash
   docker-compose up
   ```
2. Submit the Flink job:
   ```bash
   docker compose exec jobmanager ./bin/flink run -py /opt/src/job/homework.py --pyFiles /opt/src -d
   ```

---

## PostgreSQL Tables

### `sessionized_events`
The Flink job creates and writes to the `sessionized_events` table with the following schema:
- `ip`: User IP address.
- `host`: Host accessed by the user.
- `session_start`: Start timestamp of the session.
- `session_end`: End timestamp of the session.
- `num_logs`: Number of logs in the session.

---

## SQL Queries for Analysis

The SQL queries for analyzing the sessionized data are located in the file `homework.sql`. Use this file to execute the queries in your PostgreSQL client.

---

## Logging and Debugging
Errors during execution are logged to the console with detailed context. Use the logs to debug issues related to environment configuration or data flow.

---

## Next Steps
1. Ensure the required environment variables are properly set up.
2. Use the `homework.sql` file to analyze the sessionized data in PostgreSQL.
3. Extend the Flink job to handle additional use cases such as anomaly detection or real-time monitoring.

---

## Troubleshooting
If the job fails:
- Verify that Kafka and PostgreSQL are running and accessible.
- Check the environment variable configurations.
- Inspect logs for detailed error messages.

