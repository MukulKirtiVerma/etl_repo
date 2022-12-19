'''
SQL Connector class
'''
import time
from sqlalchemy import create_engine, exc
import pandas as pd
import pymysql



class SQLQuery:
    '''
    Initialized class SQLQuery
    '''
    def __init__(self, write_conn_string: str):
        self.write_conn_string = write_conn_string
        # Connection creation


    def get_table_fields(self, table_name):
        serverIP = "127.0.0.1"
        serverUser = "root"
        serverUserPwd = "mukul123"
        characterSet = "utf8mb4"
        cursorType = pymysql.cursors.DictCursor
        mySQLConnection = pymysql.connect(host=serverIP,
                                          user=serverUser,
                                          password=serverUserPwd,
                                          charset=characterSet,
                                          cursorclass=cursorType)
        cols = []
        try:
            # Obtain a cursor object
            cursorObject = mySQLConnection.cursor()

            # Execute DESCRIBE statement
            cursorObject.execute(f"DESCRIBE analyticdemo.{table_name}")

            # Fetch and print the meta-data of the table
            indexList = cursorObject.fetchall()
            cols = [i['Field'] for i in indexList]
            return cols
        except Exception as e:
            print("Exception occured:{}".format(e))

        finally:
            mySQLConnection.close()
            return cols

    def alter_cols(self, table_name, columns):

        con = pymysql.connect(
            host="127.0.0.1",
            user="root",
            password="mukul123",
            db="analyticdemo",
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )

        try:
            print(table_name, columns)
            for col in columns:
                cursorObject = con.cursor()
                alterStatement = f"ALTER TABLE {table_name} ADD {col} varchar(255);"
                # Execute the SQL ALTER statement
                cursorObject.execute(alterStatement)
                # Verify the new schema by issuing a DESCRIBE statament
                descibeStatement = f"DESC {table_name}"
                # Execute the SQL SELECT query
                cursorObject.execute(descibeStatement)
                # Fetch the updated row
                columns = cursorObject.fetchall()
                # Print the updated row...
            for column in columns:
                print(column)
        except Exception as e:
            print("Exeception occured:{}".format(e))
        finally:
            con.close()

    def load_data(self, target_table: str, ddf: pd.DataFrame, query: str):
        '''
        func insert data
        '''
        start = time.time()
        try:
            engine = create_engine(self.write_conn_string)
            df_cols = set(ddf.columns)
            existing_cols = set(self.get_table_fields(target_table))
            new_cols = df_cols.difference(existing_cols)
            if len(new_cols)>0:
                self.alter_cols(target_table, new_cols)
            ddf.to_sql(target_table, engine, if_exists='append', index=False, index_label=None)
        except exc.IntegrityError as err:
            if "Duplicate entry" not in str(err):
                print(f'Encountered SQLAlchemyError: insert[{target_table}], {str(err)}\n')
            else:
                if target_table == 'video_measure':
                    self.update_data(target_table, query)
        except exc.OperationalError as err:
            print(f'Encountered SQLAlchemyError: insert[{target_table}], {str(err)}\n')
        except exc.DataError as err:
            print(f'Encountered SQLAlchemyError: insert[{target_table}], {str(err)}\n')
        end = time.time()
        elapsed = end-start
        return elapsed

    def update_data(self, target_table: str, query: str):
        '''
        func update data
        '''
        try:
            engine = create_engine(self.write_conn_string)
            conn = engine.connect()
            conn.execute(query)
        except exc.SQLAlchemyError as err:
            print(f'Encountered SQLAlchemyError: update[{target_table}], , {str(err)}\n')
