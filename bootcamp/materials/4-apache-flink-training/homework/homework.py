# Updated Flink Script with Proper Sessionization and Documentation

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session
import os

def get_env_var(var_name: str, default: str = "") -> str:
    value = os.environ.get(var_name, default)
    if not value:
        raise ValueError(f"Environment variable '{var_name}' is not set.")
    return value

def create_events_source_kafka(t_env: StreamTableEnvironment) -> str:
    kafka_key = get_env_var("KAFKA_WEB_TRAFFIC_KEY")
    kafka_secret = get_env_var("KAFKA_WEB_TRAFFIC_SECRET")
    kafka_url = get_env_var("KAFKA_URL")
    kafka_topic = get_env_var("KAFKA_TOPIC")
    kafka_group = get_env_var("KAFKA_GROUP")
    table_name = "process_events_kafka"

    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            WATERMARK FOR event_time AS event_time - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{kafka_url}',
            'topic' = '{kafka_topic}',
            'properties.group.id' = '{kafka_group}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' =
                'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_jdbc_table(t_env: StreamTableEnvironment, table_name: str, schema: str) -> None:
    postgres_url = get_env_var("POSTGRES_URL")
    postgres_user = get_env_var("POSTGRES_USER", "postgres")
    postgres_password = get_env_var("POSTGRES_PASSWORD", "postgres")

    sink_ddl = f"""
        CREATE TABLE {table_name} (
            {schema}
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{postgres_url}',
            'table-name' = '{table_name}',
            'username' = '{postgres_user}',
            'password' = '{postgres_password}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)

def log_aggregation_with_session_windows() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source table
        kafka_table = create_events_source_kafka(t_env)

        # Create sessionized table
        session_table = "sessionized_events"
        create_jdbc_table(t_env, session_table, """
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_logs BIGINT
        """)

        # Perform sessionization
        t_env.from_path(kafka_table) \
            .window(Session.with_gap(lit(300).seconds).on(col("event_time")).alias("session_window")) \
            .group_by(col("session_window"), col("ip"), col("host")) \
            .select(
                col("ip"),
                col("host"),
                col("session_window").start.alias("session_start"),
                col("session_window").end.alias("session_end"),
                col("ip").count.alias("num_logs")
            ) \
            .execute_insert(session_table).wait()

    except Exception as e:
        print(f"Error during Flink job execution: {e}")

if __name__ == "__main__":
    log_aggregation_with_session_windows()
