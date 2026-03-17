from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_tip_sink_postgres(t_env):
    table_name = 'green_trips_tip_hourly'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_tip DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    table_name = 'events'
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            lpep_pickup_datetime VARCHAR,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def run_tip_amount_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    t_env.get_config().set("state.checkpoint-storage", "filesystem")
    t_env.get_config().set("state.checkpoints.dir", "file:///tmp/flink-checkpoints")

    try:
        source_table = create_events_source_kafka(t_env)
        sink_table = create_tip_sink_postgres(t_env)

        t_env.execute_sql(
            f"""
            INSERT INTO {sink_table}
            SELECT
                window_start,
                window_end,
                SUM(tip_amount) AS total_tip
            FROM TABLE(
                TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
            )
            GROUP BY window_start, window_end
            """
        ).wait()

    except Exception as e:
        print('Tip amount job failed:', str(e))


if __name__ == '__main__':
    run_tip_amount_job()
