'''
ETL Proccessing class
'''
import time
import sys
import os
import platform
import json
import psutil
from sqlalchemy import create_engine, exc
import pandas as pd
from configs import Configs
from series import DataSeries
from load import SQLQuery
from pyspark.sql import SparkSession
from time import time
from pandas import DataFrame

class ETL():
    '''
    ETL Class
    '''
    def __init__(self, conf):
        load_config = Configs()
        db_config = load_config.db_lst(conf)
        rules_config = load_config.rules_transform_lst(conf)

        self.db_config = db_config
        self.rules_config = rules_config

    def df_in_chunks(df, row_count):
        """
        in: df
        out: [df1, df2, ..., df100]
        """
        count = df.count()

        if count > row_count:
            num_chunks = count // row_count
            chunk_percent = 1 / num_chunks  # 1% would become 0.01
            return df.randomSplit([chunk_percent] * num_chunks, seed=1234)
        yield [df]

    def extacrt_using_pyspark(self):
        try:
            spark = SparkSession.builder.config(\
                "spark.jars",\
                "/Users/mukul/Documents/mysql-connector-java-8.0.222/mysql-connector-java-8.0.22.jar") \
                .master("local").appName("PySpark_MySQL_test").getOrCreate()

            df = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:mysql://localhost:3306/analyticdemo") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", "video_measure") \
                .option("user", "root") \
                .option("password", "mukul123") \
                .load()
            for df_part in df_in_chunks(df, 50000):
                rows = list(map(lambda row: row.asDict(), df_part.collect()))
                do_operation(rows)
                # DON'T DO THIS - See explanation below
                save_progress(df.agg({'id': 'max'}).collect()[0]['max(id)'])

            print(count)


        except Exception as e:
            print(e)



    def extract(self):
        '''
        func extract data
        '''
        read_conn_string = self.db_config['read_conn_string']
        read_table = self.db_config['read_table']
        try:
            engine = create_engine(read_conn_string)
        except exc.SQLAlchemyError:
            print('Encountered general SQLAlchemyError')

        query = f'SELECT * FROM {read_table}'
        return pd.read_sql_query(query, con=engine, chunksize=10)

    def transform(self, extr: pd.DataFrame):
        '''
        func transform & load data
        '''
        transform_row = 0
        transform_elapse = 0
        load_row = 0
        load_elapsed = 0
        # print(extr)
        conn = SQLQuery(self.db_config['write_conn_string'])
        for _, row in extr.iterrows():
            start = time.time()

            tsm = str(row.tsupdate)
            tsm = tsm[:len(tsm) - 3]

            if len(row.sessid) > 36:
                row.sessid = row.sessid[:35]

            dict_row = {'sessid': row.sessid, 'tsupdate': tsm, 'merchant': row.merchant}
            dict_series_conf = {'dict_row': dict_row, 'rules_config': self.rules_config}
            set_series = DataSeries(dict_series_conf)

            if row.geoip is not None:
                vm_geoip, q_geoip, t_geoip = set_series.geoips(row.geoip)
                transform_row += t_geoip
                t_db = conn.load_data('video_measure', vm_geoip, q_geoip)
                load_row += t_geoip
                load_elapsed += t_db
            elif row.userid is not None:
                vm_userid, q_userid = set_series.userids(row.userid)
                transform_row += 1
                t_db = conn.load_data('video_measure', vm_userid, q_userid)
                load_row += 1
                load_elapsed += t_db
            elif row.duuid is not None:
                df_duuid, q_duuid = set_series.duuid1(row.duuid)
                vm_duuid = set_series.duuid2(row.duuid)
                transform_row += 1
                t_db1 = conn.load_data('video_measure', df_duuid, q_duuid)
                t_db2 = conn.load_data('vm_duuid', vm_duuid, '')
                load_row += 2
                load_elapsed += t_db1 + t_db2
            elif row.uadev is not None:
                vm_uadev, q_uadev, t_uadev = set_series.uadevs(row.uadev)
                transform_row += t_uadev
                t_db = conn.load_data('video_measure', vm_uadev, q_uadev)
                load_row += t_uadev
                load_elapsed += t_db
            elif row.id is not None:
                vm_id, q_id, t_id = set_series.ids(row.id)
                transform_row += t_id
                t_db = conn.load_data('video_measure', vm_id, q_id)
                load_row += t_id
                load_elapsed += t_db
            elif row['name'] is not None:
                vm_name, q_name, t_name = set_series.names(row['name'])
                transform_row += t_name
                t_db = conn.load_data('video_measure', vm_name, q_name)
                load_row += t_name
                load_elapsed += t_db
            elif row.categories is not None:
                vm_category, q_category, t_category = set_series.categories(row.categories)
                transform_row += t_category
                t_db = conn.load_data('video_measure', vm_category, q_category)
                load_row += t_category
                load_elapsed += t_db
            elif row.tags is not None:
                vm_tags, q_tags, t_tags = set_series.tags(row.tags)
                transform_row += t_tags
                t_db = conn.load_data('video_measure', vm_tags, q_tags)
                load_row += t_tags
                load_elapsed += t_db
            elif row.attributes is not None:
                vm_attribute, q_attribute, t_attr = set_series.attributes(row.attributes)
                transform_row += t_attr
                t_db = conn.load_data('video_measure', vm_attribute, q_attribute)
                load_row += t_attr
                load_elapsed += t_db
            elif row.aload is not None:
                df_load, q_load = set_series.load1(row.aload)
                vm_load = set_series.load2(row.aload)
                transform_row += 1
                t_db1 = conn.load_data('video_measure', df_load, q_load)
                t_db2 = conn.load_data('vm_load', vm_load, '')
                load_row += 2
                load_elapsed += t_db1 + t_db2
            elif row.duration is not None:
                vm_duration, q_duration = set_series.durations(row.duration)
                transform_row += 1
                t_db = conn.load_data('video_measure', vm_duration, q_duration)
                load_row += 1
                load_elapsed += t_db
            elif row.bitrate is not None:
                vm_bitrate, vm_table = set_series.bitrates(row.bitrate)
                transform_row += 1
                t_db = conn.load_data(vm_table, vm_bitrate, '')
                load_row += 1
                load_elapsed += t_db
            elif row.play is not None:
                vm_play = set_series.plays(row.play)
                transform_row += 1
                t_db = conn.load_data('vm_play', vm_play, '')
                load_row += 1
                load_elapsed += t_db
            elif row.pause is not None:
                vm_pause = set_series.pauses(row.pause)
                transform_row += 1
                t_db = conn.load_data('vm_pause', vm_pause, '')
                load_row += 1
                load_elapsed += t_db
            elif row.seek is not None:
                vm_seek = set_series.seeks(row.seek)
                transform_row += 1
                t_db = conn.load_data('vm_seek', vm_seek, '')
                load_row += 1
                load_elapsed += t_db
            elif row.playing is not None:
                vm_playing = set_series.playings(row.playing)
                transform_row += 1
                t_db = conn.load_data('vm_playing', vm_playing, '')
                load_row += 1
                load_elapsed += t_db
            elif row.buffer is not None:
                vm_buffer = set_series.buffers(row.buffer)
                transform_row += 1
                t_db = conn.load_data('vm_buffer', vm_buffer, '')
                load_row += 1
                load_elapsed += t_db
            elif row.error is not None:
                vm_error, q_error = set_series.errors(row.error)
                transform_row += 1
                t_db = conn.load_data('vm_error', vm_error, q_error)
                load_row += 1
                load_elapsed += t_db
            elif row.complete is not None:
                vm_complete = set_series.completes(row.complete)
                transform_row += 1
                t_db = conn.load_data('vm_complete', vm_complete, '')
                load_row += 1
                load_elapsed += t_db
            if row.unload is not None:
                df_unload, q_unload = set_series.unload1(row.unload)
                vm_unload = set_series.unload2(row.unload)
                transform_row += 1
                t_db1 = conn.load_data('video_measure', df_unload, q_unload)
                t_db2 = conn.load_data('vm_unload', vm_unload, '')
                load_row += 2
                load_elapsed += t_db
            end = time.time()
            transform_elapse = end - start
        dict_transform_load = {
            'transform_row': transform_row,
            'transform_elapsed': transform_elapse,
            'load_row': load_row,
            'load_elapsed': load_elapsed
        }
        return dict_transform_load


    def transform_(self, extr: pd.DataFrame):
        '''
        func transform & load data
        '''
        transform_row = 0
        transform_elapse = 0
        load_row = 0
        load_elapsed = 0
        # print(extr)
        conn = SQLQuery(self.db_config['write_conn_string'])
        for _, row in extr.iterrows():
            start = time.time()

            tsm = str(row.tsupdate)
            tsm = tsm[:len(tsm) - 3]

            if len(row.sessid) > 36:
                row.sessid = row.sessid[:35]

            dict_row = {'sessid': row.sessid, 'tsupdate': tsm, 'merchant': row.merchant}
            dict_series_conf = {'dict_row': dict_row, 'rules_config': self.rules_config}
            set_series = DataSeries(dict_series_conf)

            if row.geoip is not None:
                vm_geoip, q_geoip, t_geoip = set_series.geoips(row.geoip)
                transform_row += t_geoip
                t_db = conn.load_data('video_measure', vm_geoip, q_geoip)
                load_row += t_geoip
                load_elapsed += t_db
            elif row.userid is not None:
                vm_userid, q_userid = set_series.userids(row.userid)
                transform_row += 1
                t_db = conn.load_data('video_measure', vm_userid, q_userid)
                load_row += 1
                load_elapsed += t_db
            elif row.duuid is not None:
                df_duuid, q_duuid = set_series.duuid1(row.duuid)
                vm_duuid = set_series.duuid2(row.duuid)
                transform_row += 1
                t_db1 = conn.load_data('video_measure', df_duuid, q_duuid)
                t_db2 = conn.load_data('vm_duuid', vm_duuid, '')
                load_row += 2
                load_elapsed += t_db1 + t_db2
            elif row.uadev is not None:
                vm_uadev, q_uadev, t_uadev = set_series.uadevs(row.uadev)
                transform_row += t_uadev
                t_db = conn.load_data('video_measure', vm_uadev, q_uadev)
                load_row += t_uadev
                load_elapsed += t_db
            elif row.id is not None:
                vm_id, q_id, t_id = set_series.ids(row.id)
                transform_row += t_id
                t_db = conn.load_data('video_measure', vm_id, q_id)
                load_row += t_id
                load_elapsed += t_db
            elif row['name'] is not None:
                vm_name, q_name, t_name = set_series.names(row['name'])
                transform_row += t_name
                t_db = conn.load_data('video_measure', vm_name, q_name)
                load_row += t_name
                load_elapsed += t_db
            elif row.categories is not None:
                vm_category, q_category, t_category = set_series.categories(row.categories)
                transform_row += t_category
                t_db = conn.load_data('video_measure', vm_category, q_category)
                load_row += t_category
                load_elapsed += t_db
            elif row.tags is not None:
                vm_tags, q_tags, t_tags = set_series.tags(row.tags)
                transform_row += t_tags
                t_db = conn.load_data('video_measure', vm_tags, q_tags)
                load_row += t_tags
                load_elapsed += t_db
            elif row.attributes is not None:
                vm_attribute, q_attribute, t_attr = set_series.attributes(row.attributes)
                transform_row += t_attr
                t_db = conn.load_data('video_measure', vm_attribute, q_attribute)
                load_row += t_attr
                load_elapsed += t_db
            elif row.aload is not None:
                df_load, q_load = set_series.load1(row.aload)
                vm_load = set_series.load2(row.aload)
                transform_row += 1
                t_db1 = conn.load_data('video_measure', df_load, q_load)
                t_db2 = conn.load_data('vm_load', vm_load, '')
                load_row += 2
                load_elapsed += t_db1 + t_db2
            elif row.duration is not None:
                vm_duration, q_duration = set_series.durations(row.duration)
                transform_row += 1
                t_db = conn.load_data('video_measure', vm_duration, q_duration)
                load_row += 1
                load_elapsed += t_db
            elif row.bitrate is not None:
                vm_bitrate, vm_table = set_series.bitrates(row.bitrate)
                transform_row += 1
                t_db = conn.load_data(vm_table, vm_bitrate, '')
                load_row += 1
                load_elapsed += t_db
            elif row.play is not None:
                vm_play = set_series.plays(row.play)
                transform_row += 1
                t_db = conn.load_data('vm_play', vm_play, '')
                load_row += 1
                load_elapsed += t_db
            elif row.pause is not None:
                vm_pause = set_series.pauses(row.pause)
                transform_row += 1
                t_db = conn.load_data('vm_pause', vm_pause, '')
                load_row += 1
                load_elapsed += t_db
            elif row.seek is not None:
                vm_seek = set_series.seeks(row.seek)
                transform_row += 1
                t_db = conn.load_data('vm_seek', vm_seek, '')
                load_row += 1
                load_elapsed += t_db
            elif row.playing is not None:
                vm_playing = set_series.playings(row.playing)
                transform_row += 1
                t_db = conn.load_data('vm_playing', vm_playing, '')
                load_row += 1
                load_elapsed += t_db
            elif row.buffer is not None:
                vm_buffer = set_series.buffers(row.buffer)
                transform_row += 1
                t_db = conn.load_data('vm_buffer', vm_buffer, '')
                load_row += 1
                load_elapsed += t_db
            elif row.error is not None:
                vm_error, q_error = set_series.errors(row.error)
                transform_row += 1
                t_db = conn.load_data('vm_error', vm_error, q_error)
                load_row += 1
                load_elapsed += t_db
            elif row.complete is not None:
                vm_complete = set_series.completes(row.complete)
                transform_row += 1
                t_db = conn.load_data('vm_complete', vm_complete, '')
                load_row += 1
                load_elapsed += t_db
            if row.unload is not None:
                df_unload, q_unload = set_series.unload1(row.unload)
                vm_unload = set_series.unload2(row.unload)
                transform_row += 1
                t_db1 = conn.load_data('video_measure', df_unload, q_unload)
                t_db2 = conn.load_data('vm_unload', vm_unload, '')
                load_row += 2
                load_elapsed += t_db
            end = time.time()
            transform_elapse = end - start
        dict_transform_load = {
            'transform_row': transform_row,
            'transform_elapsed': transform_elapse,
            'load_row': load_row,
            'load_elapsed': load_elapsed
        }
        return dict_transform_load


    def execute(self):
        '''
        func execute ETL
        '''
        benchmarks = []
        extract_row = []
        for df_row in self.extacrt_using_pyspark():
            start = time.time()
            extract_row.append(len(df_row))
            dtl = self.transform(df_row)
            end = time.time()
            elapsed = end-start

            mem = psutil.virtual_memory().used
            exp_str = [ (0, 'B'), (10, 'KB'),(20, 'MB'),(30, 'GB'),(40, 'TB'), (50, 'PB'),]
            i = 0
            while i+1 < len(exp_str) and mem >= (2 ** exp_str[i+1][0]):
                i += 1
                rounded_val = round(float(mem) / 2 ** exp_str[i][0], 2)
            mem_used = '%s %s' % (int(rounded_val), exp_str[i][1])

            transform_elapsed = dtl['transform_elapsed']
            load_elapsed = dtl['load_elapsed']
            benchmark_series = [[
                1000, len(df_row), f'{elapsed:8.4f}',
                dtl['transform_row'], f'{transform_elapsed:8.4f}',
                dtl['load_row'], f'{load_elapsed:8.4f}',
                mem_used, f'{psutil.cpu_percent()} %'
            ]]
            benchmark = pd.DataFrame(benchmark_series,
                columns = ['chunksize', 'extract', 'extract_elapsed_time_in_sec',
                    'transform', 'transform_elapsed_time_in_sec',
                    'load', 'load_elapsed_time_in_sec',
                    'memory_usage', 'cpu_usage'])
            print(benchmark)
            benchmarks.append(benchmark)
        df_benchmark = pd.concat(benchmarks, ignore_index=True, sort=False)
        if not os.path.isfile('benchmark.csv'):
            df_benchmark.to_csv('./benchmark.csv', encoding='utf-8', index=True)
        else:
            df_benchmark.to_csv('./benchmark.csv', mode='a', header=False)

def system_info():
    '''
    print system info function
    '''
    my_system = platform.uname()
    print(f"Operating System: {my_system.system}")
    print(f"Node Name: {my_system.node}")
    print(f"Release: {my_system.release}")
    print(f"Version: {my_system.version}")
    print(f"Machine: {my_system.machine}")
    print(f"Processor: {my_system.processor}")
    print(f"RAM: {str(round(psutil.virtual_memory().total / (1024.0 **3)))} GB\n")

if __name__ == '__main__':
    sys.argv[1] = 'init.conf'
    if len(sys.argv) < 2: #python etl.py
        print('I/O error: config file are not include. Aborting')
        sys.exit(2)

    try:
        with open('init.conf', 'r', encoding='utf8') as json_file:
            configs = json.load(json_file)

        system_info()
        etl = ETL(configs)
        etl.execute()

        mem = psutil.virtual_memory().used
        exp_str = [ (0, 'B'), (10, 'KB'),(20, 'MB'),(30, 'GB'),(40, 'TB'), (50, 'PB'),]
        i = 0
        while i+1 < len(exp_str) and mem >= (2 ** exp_str[i+1][0]):
            i += 1
            rounded_val = round(float(mem) / 2 ** exp_str[i][0], 2)
        mem_used = '%s %s' % (int(rounded_val), exp_str[i][1])
        print(f"etl_memory_usage: {mem_used}, etl_cpu_usage: {psutil.cpu_percent()} %, total_execute_time: {elapsed:8.4f} s")
    except FileNotFoundError:
        print(f'File {sys.argv[1]} not found. Aborting')
        sys.exit(1)
    except OSError:
        print(f'OS error occurred trying to open {sys.argv[1]}. Aborting')
        sys.exit(1)
    except Exception as e:
        print(e)
