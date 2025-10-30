from settings import get_settings
settings = get_settings()
from kafka import KafkaConsumer
import logging
import json
import time
from multiprocessing import Process
import pandas as pd 
from pymilvus import (
    FieldSchema,      
    CollectionSchema,  
    DataType,          
    MilvusClient       
)
from sentence_transformers import SentenceTransformer
import torch
import time
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('Embeding vector storage')



KAFKA_BOOTSTRAP = settings.KAFKA_BOOTSTRAP 
KAFKA_TOPIC = settings.KAFKA_TOPIC
KAFKA_GROUP = settings.KAFKA_GROUP
BATCH_SIZE = settings.BATCH_SIZE

MILVUS_URL = settings.MILVUS_URL
MILVUS_DB_NAME = settings.MILVUS_DB_NAME
MILVUS_COLLECTION_NAME = settings.MILVUS_COLLECTION_NAME 

LOCAL_MODEL_PATH_M3 = settings.LOCAL_MODEL_PATH_M3 

# Load embedding model once

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logger.info(f" Loading embedding model once on device: {device} ...")

try:
    embedding_model = SentenceTransformer(LOCAL_MODEL_PATH_M3, device=device)
    logger.info("✅ Model M3 loaded successfully!")
except Exception as e:
    logger.error(f"❌ Failed to load embedding model: {e}", exc_info=True)
    embedding_model = None


def create_consumer() :
    return KafkaConsumer(
        KAFKA_TOPIC ,  
        bootstrap_servers=[ KAFKA_BOOTSTRAP ],
        auto_offset_reset='earliest',
        group_id= KAFKA_GROUP ,
        enable_auto_commit=False,
        fetch_max_bytes=1000 * 1024 * 1024 ,
        max_poll_records = BATCH_SIZE,   
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )


def connect_milvus(retries=5, delay=3):
    for i in range(retries):
        try:
            client = MilvusClient(uri=MILVUS_URL)
            client.using_database(MILVUS_DB_NAME)
            logger.info("✅ Kết nối Milvus thành công")
            return client
        except Exception as e:
            logger.warning(f"Lỗi kết nối Milvus, thử lại {i+1}/{retries} ... {e}")
            time.sleep(delay)
    return None

def insert_milvus( data ) :
    global embedding_model
    if embedding_model is None:
        logger.error("⚠️ Embedding model not loaded, skipping insert.")
        return False
    
    df = pd.DataFrame(columns=["uuid", "data_vector" ])
    for i in data:
        try:
            uuid_data = i['uuid']  
            vector_data = i['text']  
            df.loc[len(df)] = [ uuid_data ,  vector_data ]
        except:
            continue

    if len( df )  == 0:
        logger.info("Data is incorrect format with uuid & text ...!")
        return False 

    try:

        texts = df["data_vector"].tolist()
        logger.info(f"Encoding {len(texts)} records...")
        embeddings = embedding_model.encode(texts, normalize_embeddings=True)

        df["embedding"] = embeddings.tolist()
        EMBED_DIMENSION = 1024 
            
        # Kết nối Milvus
        client = connect_milvus()
        if client is None:
            logger.error("Không thể kết nối Milvus")
            return False
            

        fields = [ FieldSchema(name="uuid", dtype=DataType.VARCHAR, max_length=100 ,  is_primary=True  ),
                    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=EMBED_DIMENSION)  ]
        schema = CollectionSchema(fields, description="Collection schema for ES embeding")

        if not client.has_collection(MILVUS_COLLECTION_NAME):
            client.create_collection(
                collection_name=MILVUS_COLLECTION_NAME,
                schema=schema,
                consistency_level="Eventually"  
            )
            print(f"Collection '{MILVUS_COLLECTION_NAME}' created!")
            
            index_params = client.prepare_index_params()
            index_params.add_index(
                field_name="embedding",
                index_name="idx1",
                index_type="GPU_IVF_FLAT",     
                metric_type="IP"    
            )
            client.create_index(collection_name=MILVUS_COLLECTION_NAME, index_params=index_params)
            client.load_collection(MILVUS_COLLECTION_NAME)
            
        else:
            print(f"Collection '{MILVUS_COLLECTION_NAME}' already exists.")

        # Insert data

        extracted_data =  df[ ['uuid', 'embedding'] ].to_dict(orient='records')
        insert_result = client.insert(MILVUS_COLLECTION_NAME, extracted_data)
        
        
        # Kiểm tra kết quả insert
        inserted_count = insert_result.get("insert_count", 0)
        if inserted_count != len(extracted_data):
            logger.error(f"⚠️ Insert không đủ records: {inserted_count}/{len(extracted_data)}")
            return False

        logger.info(f"✅ Inserted count: {inserted_count}")
        logger.info("Milvus data preparation complete!")
        return True

        #client.flush(MILVUS_COLLECTION_NAME)
    
    except Exception as e:
        logger.error(f"❌ Insert Milvus thất bại: {e}", exc_info=True)
        return False  



def consume_batches():
    consumer = create_consumer()
    batch = []
    last_insert_time = time.time()
    TIMEOUT_SECONDS_CONTINUE = 300  
    POLL_TIMEOUT_MS = 5000 

    logger.info(f"🚀 Consumer started. Subscribed to topic={KAFKA_TOPIC}")

    try:
        while True:
            records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

            if not records:

                current_time = time.time()
                if batch and (current_time - last_insert_time) >= TIMEOUT_SECONDS_CONTINUE:
                    logger.info(f"⏰ Timeout reached — inserting {len(batch)} messages to Milvus")
                    success = insert_milvus(batch)
                    if success:
                        consumer.commit()
                        logger.info(f"✅ Offsets committed {len(batch)} ")
                    else:
                        logger.error("⚠️ Insert thất bại — offsets không commit ! ")
                    batch.clear()
                    last_insert_time = current_time
                continue  


            for tp, msgs in records.items():
                for message in msgs:
                    batch.append(message.value)

    
            if len(batch) >= BATCH_SIZE:
                logger.info(f"📦 Inserting {len(batch)} messages")
                success = insert_milvus(batch)
                if success:
                    consumer.commit()
                    logger.info(f"✅ Offsets committed {len(batch)} ")
                else:
                    logger.error("⚠️ Insert thất bại — offsets không commit !! ") 
                batch.clear()
                last_insert_time = time.time()
                time.sleep(1)

    except KeyboardInterrupt:
        logger.warning("🛑 KeyboardInterrupt received, flushing remaining batch...")

    except Exception as e:
        logger.error(f"❌ Error consuming Kafka: {e}", exc_info=True)

    finally:

        if batch:

            success = insert_milvus(batch)
            if success:
                consumer.commit()
                logger.info(f"✅ Offsets committed {len(batch)} ")
            else:
                logger.error("⚠️ Insert thất bại — offsets không commit !!! ") 
        
if __name__ == "__main__":
    while True:
        consume_batches()