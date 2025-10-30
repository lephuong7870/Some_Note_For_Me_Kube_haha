from fastapi import FastAPI, HTTPException, Response, Query , Body  , Path
from typing import Annotated
from fastapi import Depends, FastAPI, HTTPException, status
import os
from datetime import datetime
import logging
from pydantic import BaseModel
from typing import Optional, Annotated
from pydantic import BaseModel
from fastapi import Depends

app = FastAPI(title="Convert vector embeding API")
from sentence_transformers import SentenceTransformer
import torch
import time
from fastapi.responses import JSONResponse


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('Create vector embeding')


from settings import get_settings
settings = get_settings()
LOCAL_MODEL_PATH_M3 = settings.LOCAL_MODEL_PATH_M3 

class QueryRequest(BaseModel):
    query: str
   
@app.post("/vectorEmbeding" )
async def vectorProcessing( string : QueryRequest ):
    """
    Request with the following information:

    - **query**

    """
    query  = string.query 
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    logger.info("Loading model embeding ...")
    embedding_model = SentenceTransformer( LOCAL_MODEL_PATH_M3 , device=device )
    embeddings = embedding_model.encode( [ query ] , normalize_embeddings=True)
    logger.info("Complete ...")
    embedding_list = embeddings.tolist()[0]
    return embedding_list 


