from functools import lru_cache
from dotenv import load_dotenv
import sys
import os
from os.path import join, dirname, abspath
from dotenv import load_dotenv
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  
MODEL_DIR = os.path.join(BASE_DIR, "..", "models")    

class Settings():
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
    KAFKA_GROUP = os.getenv("KAFKA_GROUP")
    BATCH_SIZE = os.getenv("BATCH_SIZE")
    BATCH_SIZE = int(BATCH_SIZE)
    
    MILVUS_URL = os.getenv("MILVUS_URL")
    MILVUS_DB_NAME = os.getenv("MILVUS_DB_NAME")
    MILVUS_COLLECTION_NAME = os.getenv("MILVUS_COLLECTION_NAME") 

    LOCAL_MODEL_PATH_M3= os.path.join(MODEL_DIR , os.getenv("LOCAL_MODEL_PATH_M3"))  

@lru_cache()
def get_settings():
    return Settings()

