import psycopg2 as pg2
import pymongo as mongo
from datetime import datetime
from time import sleep
import Pandas as pd
import bson
from dotenv import dotenv_values

config = dotenv_values(".env")
mongodb_client = MongoClient(config["ATLAS_URI"])

def startup_db_client():
    database = mongodb_client[config["DB_NAME"]]
    print("Connected to the MongoDB database!")
    return database

def shutdown_db_client():
    mongodb_client.close()

def get_mongo_relation(relname, fields=None):
    relation = db[relname]
    if fields:
        df = pd.DataFrame(list(relation.find(projection=fields)))
    else:
        df = pd.DataFrame(list(relation.find(limit=1000)))
    return df

def import_mongo_schema():
	fname = config["MONGO_BACKUP"]+"_SCHEMA.bson"
	with open(fname, 'rb') as f:
		data = bson.decode_all(f.read())
		print(data[:100])

 if __name__ == '__main__':
 	import_mongo_schema()