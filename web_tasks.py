import random
import re
import time
import urllib.request as request
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from datetime import datetime

import luigi

import pymongo
import luigi.contrib.mongodb as luigi_mongo
import mongo_db_env as mg_env

class Write2Mongo(luigi.Task):
    
    mg_collection = ''
    
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
    
    def write(self, dict_list):
        mongo_target = self.output()
        
        try:
            coll = mongo_target.get_collection()

            coll.insert_many(dict_list)
        finally:
            mongo_target._mongo_client.close()
        

class Web2Mongo(Write2Mongo):
    
    urls = []
    time_out_sec = 5
    max_sleep_sec = 3
    
    redirect_pattern = re.compile('top\.location\.replace\(\\\"(.+)\\\"\)')
    
    def requires(self):
        return None

    def run(self):
        
        dict_list = []
        for url in self.urls:
            # print(url)
            
            try:
                url, html = self.read_html(url)
                dict_list.append({'url':url, 'html':html})
            except Exception as ex:
                print('Exception:', url, ex)
                time.sleep(30)
                
            time.sleep(random.randint(1, self.max_sleep_sec))
            
        if len(dict_list) > 0:
            self.write(dict_list)
        else:
            print("No result")
        
    def read_html(self, url):
        resp = request.urlopen(url, timeout=self.time_out_sec)
        html = resp.read().decode('utf-8', 'ignore')
        
        redirect_url = self.get_redirect_url(url, html)
        if redirect_url is None:
            return url, html
        else:
            return self.read_html(redirect_url)
        
    def get_redirect_url(self, url, html):
        m = self.redirect_pattern.findall(html)
        
        if len(m) == 0:
            return None
        else:
            domain = extract_domain_url(url)
            return domain + m[0]
        

def extract_domain_url(url):
    parsed_uri = urlparse(url)
    domain = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)

    return domain
        
        
if __name__ == '__main__':
    luigi.run()