import luigi
import time

import psycopg2
import pg_db_env as pg_env

class PgSelect(luigi.Task):
    
    sql = ''
    columns = []
    conn = None
    
    def requires(self):
        return None

    def output(self):
        return None
    
    def run(self):

        if self.conn is None:
            conn = psycopg2.connect(
                host=pg_env.DB_HOST,
                dbname=pg_env.DB_NAME,
                user=pg_env.DB_USER,
                password=pg_env.DB_PW,
            )
        
        try:
            
            cur = conn.cursor()
            cur.execute(self.sql)
            rows = cur.fetchall()

            if len(rows) > 0:
                print(len(rows))
                self.write(rows)
            else:
                print("No result")
        
        finally:
            if cur is not None: cur.close()
            if conn is not None: conn.close()
            
    def write(self, rows):
        return None
    
    def fill_columns(self, row, columns):
        for i in range(len(row) - len(columns)):
            new_col_name = 'col%d' % (i+1)
            print('Add column name: ', new_col_name)
            columns.append(new_col_name)
            
        return columns
    
    def build_dict_list(self, rows, columns):
        dict_list = []
        for i, row in enumerate(rows):
            row_dict = dict(zip(columns, row))
            dict_list.append(row_dict)
            
        return dict_list
        

import pymongo
import luigi.contrib.mongodb as luigi_mongo
import mongo_db_env as mg_env

class Pg2Mongo(PgSelect):
    
    mg_collection=''
    
    def output(self):
        client = pymongo.MongoClient(
            mg_env.DB_HOST, 
            mg_env.DB_PORT,
#             username=mg_env.DB_USER,
#             password=mg_env.DB_PW,
        )
        
        return luigi_mongo.MongoCollectionTarget(
            mongo_client=client, 
            index=mg_env.DB_NAME,
            collection=self.mg_collection,
        )
    
    def write(self, rows):
        coll = self.output().get_collection()
        
        columns = self.fill_columns(rows[0], self.columns)
        records = self.build_dict_list(rows, columns)
        coll.insert_many(records)
        
        
if __name__ == '__main__':
    luigi.run()
    