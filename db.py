import logging
from sqlalchemy import create_engine, exc
import creds
from sqlalchemy.exc import ProgrammingError, OperationalError, IntegrityError, DataError
import re
import time
import pandas as pd
from datetime import datetime as dt
try:
    from config import logging_level
except ModuleNotFoundError:
    logging_level = logging.DEBUG

# Set up logging
logging.basicConfig(level=logging_level)


class DBtools(object):
    def __init__(self, con_timeout=None,
                 db_user=creds.db_user,
                 db_passwd=creds.db_passwd,
                 db_host=creds.db_host,
                 db_name=creds.db_name,
                 ssl_args=creds.db_ssl_args,
                 connection_attempts=10):
        """
        Manage connection, reading, and writing to/from MySQL Database.
        :param con_timeout:
        :param db_user:
        :param db_passwd:
        :param db_host:
        :param db_name:
        :param ssl_args:
        """
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.db_host = db_host
        self.db_name = db_name
        self.ssl_args = ssl_args
        self.connection_attempts = connection_attempts
        self.connection_attempts_remaining = connection_attempts

        # Connect to DB
        self.connect_to_db()

        # Set timeout
        if con_timeout is not None:
            self.sql_to_db(sql='SET GLOBAL MAX_EXECUTION_TIME={}'.format(con_timeout))

    def __del__(self):
        """On exit, close connection"""
        self.close_con()

    def connect_to_db(self):
        """Establish connection to DB"""
        try:
            con_str = 'mysql://' + self.db_user + ':' + self.db_passwd + '@' + self.db_host + '/' + self.db_name
            engine = create_engine(con_str, connect_args=self.ssl_args)
            self.connection = engine.connect()

            msg = 'Connected to database'
            logging.debug(msg)

            # Reset to default
            self.connection_attempts_remaining = self.connection_attempts

        except OperationalError:
            if self.connection_attempts_remaining <= 0:
                raise ConnectionRefusedError('Can\'t connect to database, attempts exhausted')

            msg = 'Re-trying connection to database {} more time(s)...'.format(self.connection_attempts_remaining)
            logging.error(msg)
            self.connection_attempts_remaining -= 1
            time.sleep(3)
            self.connection = self.connect_to_db()

        except Exception as e:
            msg = 'Another Database error: {}'.format(e)
            logging.error(msg)
            raise Exception(e)

    def close_con(self):
        """Close database connection"""
        self.connection.close()
        logging.debug("Done, closed connection to DB")

    def df_to_sql_duplicate_update(self, df, db_name='items_db', db_table=None, chunk_size=5000):
        """Pandas DataFrame to MySQL, on duplicate key update. Write to DB in chunks."""
        # Fill nan with NULL, for MySQL
        df.fillna('NULL', inplace=True)

        # Escape quotes
        df.replace({"'": "''"}, regex=True, inplace=True)

        cols = ','.join(list(df))
        if isinstance(df, pd.DataFrame):
            num_cols = len(df.columns)
        elif isinstance(df, pd.Series):
            num_cols = 1
        else:
            err = 'Expected df to be of type pd.DataFrame or pd.Series. Got {}'.format(type(df))
            logging.error(err)
            raise IOError(err)

        if num_cols == 0:
            msg = 'No columns in DF'
            logging.debug(msg)

            return None

        # Reset index because it will conflicts with chunk size logic if the index is not: ix = 0 -->n, ix+1...
        df.reset_index(inplace=True, drop=True)

        x = 0
        while x <= len(df):
            end_ix = min(x + chunk_size, len(df)) - 1
            df_temp = df.ix[x:end_ix]

            x += chunk_size

            sql = """
            INSERT INTO {}.{}
                ({})
                VALUES
            """.format(db_name, db_table, cols)

            for index, row in df_temp.iterrows():
                temp_values = r'('
                for c in range(0, num_cols):

                    # If Float, i.e. prices, no quotes around value
                    if isinstance(row[c], float) or isinstance(row[c], bool) or row[c] == 'NULL':

                        temp_values += r"{},".format(str(row[c]))

                    elif len(str(row[c])) == 0:

                        temp_values += r"NULL,"

                    # If not Float, need quotes around value
                    else:

                        temp_values += r"'{}',".format(str(row[c]))

                # Remove last comma
                temp_values = temp_values[:-1]
                temp_values += '),'

                # Append to SQL string
                sql += '\n' + temp_values

            # Remove last comma
            sql = sql[:-1]

            sql += '\n ' + 'ON DUPLICATE KEY UPDATE \n'

            for col in list(df):
                sql += '\n{} = VALUES({}),'.format(col, col)

            # Remove last comma
            sql = sql[:-1]

            # Remove non ASCII, percent sign escape
            sql = re.sub(r'[^\x00-\x7F]', '', sql)
            sql = sql.replace('%', r'%%')

            if len(df_temp) > 0:
                self.sql_to_db(sql=sql)
            else:
                msg = 'Zero values to be written, not executing SQL'
                logging.debug(msg)

            # Close con
            # con.close()

    def df_to_sql_append(self, df, db_name='items_db', db_table=None, chunk_size=5000, if_exists='append'):
        df.to_sql(db_table, con=self.connection, if_exists=if_exists, index=False, chunksize=chunk_size)

    def sql_to_db(self, sql):
        """Execute SQL"""
        try:
            t_start = dt.now()
            result = self.connection.execute(sql)
            msg = 'SUCCESS writing to SQL. Time to execute: {}'.format((dt.now() - t_start).total_seconds())
            logging.debug(msg)
            return result

        except DataError as e:
            msg = 'ERROR formatting SQL: {}'.format(e)
            logging.error(msg)

        except ProgrammingError as e:
            msg = 'ERROR writing to SQL: {}'.format(e)
            logging.error(msg)
        except exc.OperationalError as e:
            msg = "sqlalchemy.exc.OperationalError {}".format(e)
            logging.error(msg)

        except IntegrityError as e:
            msg = 'Problem with Foreign - Primary key: {}'.format(e)
            logging.error(msg)

        except Exception as e:
            msg = 'Unexpected error in sql_to_db'
            logging.error(msg)
            logging.error(e)
            logging.error(sql)
            logging.info('Error time: {}'.format(dt.now()))
            raise Exception(e)

    def make_sql(self, cols, vals, db_name, db_table):
        """
        :param cols: list of columns, ['col1', 'col2']...
        :param vals: list of lists, [['a', 'b', 'c', 44.32], ['d', 'e', 'f', 'NULL'], ['g', 'h', 'i', '234']]...
        :param db_name: str, 'test table'
        :param db_table: str, 'test db'
        :return: sql str
        """
        if not isinstance(vals, list):
            msg = 'vals must be a list, received {}'.format(type(vals))
            raise Exception(msg)
        if not isinstance(cols, list):
            msg = 'cols must be a list, received {}'.format(type(cols))
            raise Exception(msg)
        for row in vals:
            if not isinstance(row, list):
                msg = 'row in vals must be a list, received {} \nRow: {}'.format(type(row), row)
                raise Exception(msg)
        for v in vals:
            if len(v) != len(cols):
                msg = 'Number of columns needs to match number of values per row. \n' \
                     'Received {} values, expected {} \n Row: {}'.format(len(v), len(cols), v)
                logging.error(msg)
                raise Exception(msg)

        cols_str = ','.join(cols)
        sql = "INSERT INTO `{}`.`{}`({}) \nVALUES \n".format(db_name, db_table, cols_str)

        for row in vals:
            temp_sql = '\n('
            for col_num in range(0, len(cols)):
                if isinstance(row[col_num], float) or str(row[col_num]).upper() in ['NULL', 'FALSE', 'TRUE']:
                    temp_sql += '{},'.format(row[col_num])
                else:
                    temp_sql += "'{}',".format(row[col_num])

            temp_sql = temp_sql[:-1] + '),'
            sql += temp_sql

        # Remove last comma
        sql = sql[:-1]

        sql += '\n ' + 'ON DUPLICATE KEY UPDATE \n'

        for col in cols:
            sql += '\n`{}` = VALUES(`{}`),'.format(col, col)

        # Remove last comma
        sql = sql[:-1]

        # Remove non ASCII
        sql = re.sub(r'[^\x00-\x7F]|[\\]]', '', sql)

        return sql

    def pd_read_sql(self, sql):
        """Wrapper for pd.read_sql(). Tests connection, and creates a new one if broken
        :returns pd.DataFrame of results"""

        try:
            logging.info('Trying pd.read_sql')
            t_start = dt.now()
            resp = pd.read_sql(sql=sql, con=self.connection)
            logging.info('Success on pd.read_sql, time: {}'.format((dt.now() - t_start).total_seconds()))
            return resp

        except Exception as e:
            logging.error('Error in pd_read_sql, trying again: {}'.format(e))
            # Re-create a connection to DB
            try:
                self.__del__()
            except Exception as f:
                logging.warning('Error calling pd_read_sql.__del__ : {}'.format(f))
                pass

            self.connect_to_db()

            # Re-try SQL
            logging.info('Re-trying pd_read_sql')
            self.pd_read_sql(sql=sql)
