from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import ScalarFunction, udf
import os
import json
import requests
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment

def create_processed_events_sink_kafka(t_env):
    table_name = "process_events_kafka"
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    sasl_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_GROUP').split('.')[0] + '.' + table_name}',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.jaas.config' = '{sasl_config}',
            'format' = 'json'
        );
        """
    print(sink_ddl)
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    # must include driver code when implementing flink
    # This DOES NOT create a table in postgres, it just lets flink know what the table looks like
    t_env.execute_sql(sink_ddl)
    return table_name

class GetLocation(ScalarFunction):    # Scalar takes 1 row or 1 column and returns 1 row or 1 column
  def eval(self, ip_address):
    url = "https://api.ip2location.io"  # Gets the country and city from the api 
    response = requests.get(url, params={
        'ip': ip_address,
        'key': os.environ.get("IP_CODING_KEY")
    })

    if response.status_code != 200:
        # Return empty dict if request failed
        return json.dumps({})

    data = json.loads(response.text)

    # Extract the country and state from the response
    # This might change depending on the actual response structure
    country = data.get('country_code', '')
    state = data.get('region_name', '')
    city = data.get('city_name', '')
    return json.dumps({'country': country, 'state': state, 'city': city})

get_location = udf(GetLocation(), result_type=DataTypes.STRING())



def create_events_source_kafka(t_env):
    # This actually creates the kafka table
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"   # We need to establish a format for a time stamp
    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    
    # flink isn't inherently connected to kafka, so we need to establish the kafka connector
    # the files from the KAFKA_GROUP, SSL and other crap in in the keys folder
    # format can be set to json, csv, etc. 
    print(source_ddl)
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    print('Starting Job!')
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment() # This gives a streaming environment, can also enableBatchMode()
    print('got streaming environment')
    env.enable_checkpointing(10 * 1000) # Measured in milliseconds, mult by 1000 makes every 10 seconds
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings) # Working with spark equiv of df
    t_env.create_temporary_function("get_location", get_location)  # registering a UDF for the GetLocation function in spark
    try:
        # Create Kafka table as the source table
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)
        print('loading into postgres')
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                        ip,
                        event_timestamp,
                        referrer,
                        host,
                        url,
                        get_location(ip) as geodata
                    FROM {source_table}
                    """
        ).wait()
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
