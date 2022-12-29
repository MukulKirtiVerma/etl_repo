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
import pyspark.pandas as ps
# ps.set_option('compute.ops_on_diff_frames', True)
from pandas import DataFrame
import warnings

with open('init.conf', 'r', encoding='utf8') as json_file:
    configs = json.load(json_file)

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

    def extacrt_using_pyspark(self, config):
        try:
            spark = SparkSession.builder.config("spark.jars", config['sql_connector_jar_file']) \
                .master(config['spark_url']).appName("PySpark_MySQL_test").getOrCreate()

            df = spark.read \
                .format("jdbc") \
                .option("url", config["db_url"]) \
                .option("driver", config["driver"]) \
                .option("dbtable", config["db_write_tbl_name"]) \
                .option("user", config["db_read_user"]) \
                .option("password", config["db_read_pass"]) \
                .load()
            return df

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
    @staticmethod
    def write_to_db(spark_df, table_name):

        spark_df.write \
            .format("jdbc") \
            .mode('append') \
            .option("url", configs["db_url"]) \
            .option("driver", configs["driver"]) \
            .option("dbtable", table_name) \
            .option("user", configs["db_read_user"]) \
            .option("password", configs["db_read_pass"]) \
            .save()



    def transform_(self, extr: pd.DataFrame):
        '''
        func transform & load data
        '''
        ps.set_option('compute.ops_on_diff_frames', True)

        #================================================================
        df=ps.DataFrame(extr)
        df.tsupdate = df.tsupdate.apply(str)
        df.tsupdate = df.tsupdate.apply(lambda x: x[:- 3])
        print('#1================================================================')


        #===============================

        df.sessid = df.sessid.apply(lambda x: x[:35] )
        #===============================


        # dict_row = {'sessid': df.sessid, 'tsupdate': tsm, 'merchant': df.merchant}
        # dict_series_conf = {'dict_row': dict_row, 'rules_config': self.rules_config}
        # set_series = DataSeries(dict_series_conf)
        #temp = df.geoip.apply(str)
        df.ctrcode = df.geoip.apply(lambda x: x.split(',')[0] if x is not None and ',' in x else 'ID')
        df.ctrname = df.geoip.apply(lambda x: x.split(',')[1] if x is not None and ',' in x else 'Indonesia')
        df.regname = df.geoip.apply(lambda x: x.split(',')[2] if x is not None and ',' in x and x.split(',')[2].isascii() else 'Unknown')
        df.ctyname = df.geoip.apply(lambda x: x.split(',')[3] if x is not None and ',' in x and x.split(',')[3].isascii() else 'Unknown')
        df.geoasn = df.geoip.apply(lambda x: x.split(',')[4] if x is not None and ',' in x else 'Unknown')
        df.uip = df.geoip.apply(lambda x: 'Unknown' if x is None or str(x).split(',')[5] == '' else x.split(',')[5])


        print('#2================================================================')

       #================================================================
        df.tscreated = df.tsupdated

        print('#3================================================================')

       #===================duuid=============================================

        # sessid = self.extract_row['sessid']
        # tsm = self.extract_row['tsupdate']
        #save to table ======================================================
        # vm_duuid_series = [[sessid, duuid, tsm]]
        # vm_duuid = pd.DataFrame(vm_duuid_series, columns=['sessid', 'duuid', 'nts'])
        # t_db2 = conn.load_data('vm_duuid', vm_duuid, '')

        # ===================duuid=============================================


        #===============================uadev=================================================
        df.uadev = df.uadev.apply(str)
        df.devops = df.uadev.apply(lambda x: x.split(',')[0] if ',' in x else '')
        df.devver = df.uadev.apply(lambda x: x.split(',')[1] if ',' in x else '')
        df.devmdl = df.uadev.apply(lambda x: x.split(',')[2] if ',' in x else '')
        df.devbrd = df.uadev.apply(lambda x: x.split(',')[3] if ',' in x else '')
        df.devmfr = df.uadev.apply(lambda x: x.split(',')[4] if ',' in x else '')

        print('#4================================================================')

        #===============================uadev=================================================

        #-=================================ID==============================

        temp = df.id.apply(str)
        temp= temp.apply(lambda x: x.split('|'))
        df_temp = ps.DataFrame(temp.to_list())
        df[['id'+ str(i) for i in df_temp.columns]] = df_temp


        # df3 = df.id.apply(ps.Series)
        # df3.columns = ['id'+str(i) for i in range(1,len(df3.columns))]
        # df[df3.columns] = df3
        # del df3

        print('#5================================================================')

        #===============================id =================================


        #===============================name =================================

        df_name = df.name.apply(str)
        df_name = df_name.apply(lambda x: x.split('||'))
        df_name = ps.DataFrame(df_name.to_list())
        df[['name'+str(i) for i in df_name.columns]] = df_name
        print('#6================================================================')

        # x = df.name.apply(lambda x: x.split('||'))
        # df3 = x.name.apply(pd.Series)
        # df3.columns = ['name' + str(i) for i in range(1, len(df3.columns))]
        # df[df3.columns] = df3

        #===========================name================================================

        #===============================category =================================

        df_categories = df.categories.apply(str)
        df_categories = df_categories.apply(lambda x: x.split(','))
        df_categories = ps.DataFrame(df_categories.to_list())
        df[['category'+ str(i) for i in df_categories.columns]]= df_categories
        print('#7================================================================')



        # x = df.categories.apply(lambda x: x.split(','))
        # df3 = x.categories.apply(pd.Series)
        # df3.columns = ['category' + str(i) for i in range(1, len(df3.columns))]
        # df[df3.columns] = df3

        # ===============================category =================================

        # ===============================tags =================================
        df_tags = df.tags.apply(str)
        df_tags = df_tags.apply(lambda x: x.split(','))
        df_tags = ps.DataFrame(df_tags.to_list())
        df[['tag'+str(i) for i in df_tags.columns]] = df_tags

        print('#8================================================================')

        # x = df.tags.apply(lambda x: x.split(','))
        # df3 = x.tags.apply(pd.Series)
        # df3.columns = ['tag' + str(i) for i in range(1, len(df3.columns))]
        # df[df3.columns] = df3

        # ===============================tags =================================

        # ===============================attribute =================================
        df_attributes = df.attributes.apply(str)
        df_attributes = df_attributes.apply(lambda x: x.split(','))
        df_attributes = ps.DataFrame(df_attributes.to_list())
        df[['attribute'+str(i) for i in df_attributes.columns]] = df_attributes

        print('#9================================================================')


        # ===============================attribute =================================


        # ===============================aload =================================
        df.aload = df.aload.apply(lambda x: x[4:] if x is not None and str(x)[0] == '$' else x)
        # t_db1 = conn.load_data('video_measure', df_load, q_load)
        # t_db2 = conn.load_data('vm_load', vm_load, '')
        print('#10================================================================')

        # ===============================aload =================================

        # ===============================duration =================================
        df.duration = df.duration.apply(lambda x: int(x.replace(',','.')) if x is not None else x)
        print('#11================================================================')

        # ===============================duration =================================

        # ===============================bitrate =================================


        # cols = ['sessid', 'nts', 'nvalue', 'pfm']
        df_bitrate = df[df.bitrate.notnull()][['sessid', 'bitrate']]
        df_bitrate['bitrate_temp'] = df_bitrate.bitrate.apply(lambda x: x[4:].split(',') + [x[1:4]])
        df_temp = ps.DataFrame(df_bitrate.bitrate_temp.to_list(), columns=['arr0','arr1','nts', 'pfm'])
        df_bitrate[df_temp.columns] = df_temp
        df_bitrate = df_bitrate.to_spark()
        df_abr = df_bitrate.filter(~((df_bitrate.arr0 == '0') | (df_bitrate.arr0 == '-1')))
        df_abr = df_abr.drop('arr1', 'bitrate', 'bitrate_temp')
        df_abr = df_abr.withColumnRenamed("arr0", "nvalue")
        print('#12================================================================')

        try:
            ETL.write_to_db(df_abr, "vm_abr")
        except Exception as e:
            print(f"error while saving data in 'vm_abr', {e}")

        print('#13================================================================')


        df_vbr = df_bitrate.filter(~((df_bitrate.arr1 == '0') | (df_bitrate.arr1 == '-1')))
        df_vbr = df_vbr.drop('arr0', 'bitrate', 'bitrate_temp')
        df_vbr = df_vbr.withColumnRenamed("arr1", "nvalue")

        print('#14================================================================')
        try:
            ETL.write_to_db(df_vbr, "vm_vbr")
        except Exception as e:
            print(f"error while saving data in 'vm_vbr', {e}")

        print('#15================================================================')

        # ===============================bitrate =================================

        # ===============================unload=================================
        df['nload'] = df.unload.apply(lambda x: x[4:] if x is not None else x)
        df_nload = df[df.unload.notnull()][['sessid', 'unload']]
        df_nload.unload = df_nload.unload.apply(lambda x: [x[4:], x[1:4]])
        df_temp = ps.DataFrame(df_nload['unload'].to_list(), columns=['nts', 'pfm'])
        df_nload[df_temp.columns] = df_temp
        df_nload = df_nload.drop(columns=['unload'])

        try:
            ETL.write_to_db(df_nload.to_spark(), "vm_unload")
        except Exception as e:
            print(f"error while saving data in 'vm_unload', {e}")
        print('#16================================================================')


        try:
            ETL.write_to_db(df.to_spark(), "video_measure2")
        except Exception as e:
            print(f"error while saving data in 'video_measure2', {e}")

        print('#17================================================================')

        # ===============================unload =================================

        # ===============================plays =================================
        df_plays = df[df.play.notnull()][['sessid', 'play']]
        df_plays.play = df_plays.play.apply(lambda x: [x[4:], x[1:4]])
        df_temp = ps.DataFrame(df_plays['play'].to_list(), columns=['nts', 'pfm'])
        df_plays[df_temp.columns] = df_temp
        df_plays = df_plays.drop(columns=['play'])
        print('#18================================================================')
        try:
            ETL.write_to_db(df_plays.to_spark(), "vm_play")
        except Exception as e:
            print(f"error while saving data in 'vm_play', {e}")

        print('#19================================================================')


        # ===============================plays =================================

        # ===============================pause =================================
        df_pause = df[df.pause.notnull()][['sessid', 'pause']]
        df_pause.pause = df_pause.pause.apply(lambda x: [x[4:], x[1:4]])
        df_temp = ps.DataFrame(df_pause['pause'].to_list(), columns=['nts', 'pfm'])
        df_pause[df_temp.columns] = df_temp
        df_pause = df_pause.drop(columns=['pause'])
        print('#20================================================================')
        try:
            ETL.write_to_db(df_pause.to_spark(), "vm_pause")
        except Exception as e:
            print(f"error while saving data in 'vm_pause', {e}")

        print('#21================================================================')

        # ===============================pause =================================

        # ===============================seek =================================
        df_seek = df[df.seek.notnull()][['sessid', 'seek']]
        df_seek.seek = df_seek.seek.apply(lambda x: [x[4:x.index(',')],  x[x.index(',')+1:], x[1:4]])
        df_temp = ps.DataFrame(df_seek['seek'].to_list(), columns=['nts', 'nvalue', 'pfm'])
        df_seek[df_temp.columns] = df_temp
        df_seek = df_seek.drop(columns=['seek'])
        print('#22================================================================')
        try:
            ETL.write_to_db(df_seek.to_spark(), "vm_seek")
        except Exception as e:
            print(f"error while saving data in 'vm_seek', {e}")

        print('#23================================================================')

        # ===============================seek =================================

        # ===============================playing =================================
        df_playing = df[df.playing.notnull()][['sessid', 'playing']]
        df_playing.playing = df_playing.playing.apply(lambda x: [x[4:x.index(',')], x[x.index(',') + 1:], x[1:4]])
        df_temp = ps.DataFrame(df_playing['playing'].to_list(), columns=['nts', 'svalue', 'pfm'])
        df_playing[df_temp.columns] = df_temp
        df_playing = df_playing.drop(columns=['playing'])
        print('#24================================================================')
        try:
            ETL.write_to_db(df_playing.to_spark(), "vm_playing")
        except Exception as e:
            print(f"error while saving data in 'vm_playing', {e}")

        print('#25================================================================')

        # ===============================playing =================================

        # ===============================buffer =================================
        df_buffer = df[df.buffer.notnull()][['sessid', 'buffer']]
        df_buffer.buffer = df_buffer.buffer.apply(lambda x: [x[1:4], x[4:]])
        df_temp = ps.DataFrame(df_buffer['buffer'].to_list(), columns= ['nts', 'pfm'])
        df_buffer[df_temp.columns] = df_temp
        df_buffer = df_buffer.drop(columns=['buffer'])
        print('#26================================================================')
        try:
            ETL.write_to_db(df_buffer.to_spark(), "vm_buffer")
        except Exception as e:
            print(f"error while saving data in 'vm_buffer', {e}")

        print('#27================================================================')

        # ===============================buffer =================================

        # ===============================error =================================
        def errors_transform(error):

            serror = error.replace(",", "^")
            aerror = serror.split("^")

            if len(aerror) > 1:
                if len(aerror[1]) > 1024:
                    aerror[1] = aerror[1][:1023]
            error_series = [[aerror[0], aerror[1]]]
            return error_series

        df_error = df[df.error.notnull()][['sessid', 'error']]
        df_error.error = df_error.error.apply(lambda x: errors_transform(x))
        df_temp = ps.DataFrame(df_error['error'].to_list(), columns= ['nts', 'msg', 'pfm'])
        df_error[df_temp.columns] = df_temp
        df_error = df_error.drop(columns=['error'])

        try:
            ETL.write_to_db(df_error.to_spark(), "vm_error")
        except Exception as e:
            print(f"error while saving data in 'vm_error', {e}")

        print('#28================================================================')


        # ===============================error =================================

        # ===============================complete =================================
        df_complete = df[df.complete.notnull()][['sessid', 'complete']]
        df_complete.complete = df_complete.complete.apply(lambda x: [x[4:], x[1:4]])
        df_temp = ps.DataFrame(df_complete['complete'].to_list(), columns= ['nts', 'pfm'])
        df_complete[df_temp.columns] = df_temp
        df_complete = df_complete.drop(columns=['complete'])
        print('#29================================================================')
        try:
            ETL.write_to_db(df_complete.to_spark(), "vm_complete")
        except Exception as e:
            print(f"error while saving data in 'vm_complete', {e}")
        print('#30================================================================')

        # ===============================complete =================================


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

    def execute_(self, config):
        df = self.extacrt_using_pyspark(config)
        self.transform_(df)
        return

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

# if __name__ == '__main__':
#     sys.argv[1] = 'init.conf'
#     if len(sys.argv) < 2: #python etl.py
#         print('I/O error: config file are not include. Aborting')
#         sys.exit(2)
#
#     try:
#         with open('init.conf', 'r', encoding='utf8') as json_file:
#             configs = json.load(json_file)
#
#         system_info()
#         etl = ETL(configs)
#         etl.execute()
#
#         mem = psutil.virtual_memory().used
#         exp_str = [ (0, 'B'), (10, 'KB'),(20, 'MB'),(30, 'GB'),(40, 'TB'), (50, 'PB'),]
#         i = 0
#         while i+1 < len(exp_str) and mem >= (2 ** exp_str[i+1][0]):
#             i += 1
#             rounded_val = round(float(mem) / 2 ** exp_str[i][0], 2)
#         mem_used = '%s %s' % (int(rounded_val), exp_str[i][1])
#         print(f"etl_memory_usage: {mem_used}, etl_cpu_usage: {psutil.cpu_percent()} %, total_execute_time: {elapsed:8.4f} s")
#     except FileNotFoundError:
#         print(f'File {sys.argv[1]} not found. Aborting')
#         sys.exit(1)
#     except OSError:
#         print(f'OS error occurred trying to open {sys.argv[1]}. Aborting')
#         sys.exit(1)
#     except Exception as e:
#         print("==================================ERROR: ", e)

if __name__ == '__main__':
    sys.argv[1] = 'init.conf'
    if len(sys.argv) < 2: #python etl.py
        print('I/O error: config file are not include. Aborting')
        sys.exit(2)

    try:
        # with open('init.conf', 'r', encoding='utf8') as json_file:
        #     configs = json.load(json_file)
        t=time.time()
        system_info()
        etl = ETL(configs)
        etl.execute_(configs)
        print("====================================exce time:", time.time() - t)

    except Exception as e:
        print("==================================ERROR: ", e)

