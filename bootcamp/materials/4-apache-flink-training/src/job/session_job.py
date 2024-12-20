import os
import traceback
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


# Create the Postgres Sink to write the data to
def create_host_ip_events_sink_postgres(t_env):
    table_name = 'host_ip_sessions'
    sink_dll = f"""
        CREATE TABLE {table_name} (
            host VARCHAR
            , ip VARCHAR
            , start_time TIMESTAMP(3)
            , end_time TIMESTAMP(3)
            , num_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_dll)
    return table_name

# Create the Kafka Source table
def create_host_ip_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_dll = f"""
        CREATE TABLE {table_name} (
            host VARCHAR
            , ip VARCHAR
            , event_time VARCHAR
            , window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}')
            , WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_dll)
    return table_name


# Set up the logging environment
def log_sessions():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try: 
        # Create the kafka source table 
        source_table = create_host_ip_events_source_kafka(t_env)
    
        # Create the postgres sink table
        postgres_sink = create_host_ip_events_sink_postgres(t_env)

        # Use Session Windows (not tumble) for aggregation
        t_env.from_path(source_table)\
            .window(
                Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("session_window")
            ).group_by(
                col("session_window")
                , col("host")
                , col("ip")
            ).select(
                col("host")
                , col("ip")
                , col("session_window").start.alias("start_time")
                , col("session_window").end.alias("end_time")
                , lit(1).count.alias("num_events")
            ).execute_insert(postgres_sink)\
            .wait()
        
    except Exception as e: 
        print("wriing records from Kafka to JDBC was a massive fail:", str(e))
        traceback.print_exc()

if __name__ == '__main__':
    log_sessions()
        
            

'''
The postgres queries to create the table with a serial number for each session:

CREATE TABLE host_ip_sessions (
            session_id SERIAL PRIMARY KEY
            , host VARCHAR
            , ip VARCHAR
            , start_time TIMESTAMP(3)
            , end_time TIMESTAMP(3)
            , num_events INT
);


Queries to answer the following: 
1. What is the average number of web events of a session from a user on Tech Creator?
    The CTE events_all_hosts finds the average number of web events per session across all users associated with a techcreator.io host.

2. Compare results between different hosts
    The remainder of the query looks at all hosts' average events per session individually and compares them to the overall 
    average from the CTE. There is a case structure that tells whether that host is greater than the overall average.




WITH events_all_hosts AS (
    SELECT 
        AVG(num_events) AS overall_avg
    FROM host_ip_sessions
    WHERE host LIKE '%.techcreator.io'
) 
SELECT
    h.host
    , COUNT(h.*) AS num_sessions
    , ROUND(AVG(h.num_events), 2) AS avg_events_per_session
    , ROUND(e.overall_avg, 2) AS overall_avg
    , CASE 
        WHEN AVG(h.num_events) > e.overall_avg THEN True
        ELSE False 
        END AS above_average
FROM host_ip_sessions AS h, events_all_hosts AS e
WHERE host IS NOT NULL
GROUP BY host, e.overall_avg




The output of the query:

zach.techcreator.io	1	4.00	2.97	True
zachwilson.techcreator.io	57	3.96	2.97	True
lulu.techcreator.io	88	3.14	2.97	True
www.zachwilson.techcreator.io	7	3.14	2.97	True
bootcamp.techcreator.io	18950	2.98	2.97	True
dutchengineer.techcreator.io	35	2.89	2.97	False
www.dataexpert.io	20193	2.73	2.97	False
www.fullstackexpert.io	42	2.50	2.97	False
duchengineer.techcreator.io	2	2.50	2.97	False
www.techcreator.io	406	2.27	2.97	False
duchenginer.techcreator.io	1	1.00	2.97	False
ductchengineer.techcreator.io	1	1.00	2.97	False
data.techcreator.io	1	1.00	2.97	False
www.linkedinexpert.io	7	1.00	2.97	False
ulu.techcreator.io	1	1.00	2.97	False
05.techcreator.io	1	1.00	2.97	False
admin.zachwilson.tech	1	1.00	2.97	False
fjaxyu.techcreator.io	5	1.00	2.97	False
eczachly.techcreator.io	1	1.00	2.97	False

'''
