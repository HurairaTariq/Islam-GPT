import pandas as pd
import PyPDF2
import os
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch, exceptions as es_exceptions
import openai
from openai import Embedding



api_key = '*******************************'
openai.api_key = api_key

trans2 = 'No Verse Available'
EMBEDDING_DIMS = 768
ENCODER_BOOST = 10
books = [] 

sentence_transformer = SentenceTransformer("deepset/deberta-v3-large-squad2")
# es_client = Elasticsearch("http://localhost:9200", verify_certs=False, request_timeout=60)
es_client = Elasticsearch("http://elasticsearch:9200", verify_certs=False, request_timeout=600)


def query_question(question: str, index_name: str, top_n: int):
    embedding = sentence_transformer.encode(question)
    es_result = es_client.search(
        index=index_name,
        size=top_n,
        from_=0,
        source=["chvr", "company", "trans1", "context",],
        query={
            "script_score": {   
                #  "query": {
                #     "bool": {
                #         "must":[
                #             {"match": {"company": company}}
                #         ],
                #         "should": [
                #             {"match": {"context": question}}
                #         ]
                #     }
                # },
                "query": {
                    "match": {
                        "context": question,
                        # "company": company
                    }
                },
                "script": {
                    "source": """
                            (cosineSimilarity(params.query_vector, "embedding") + 1)* params.encoder_boost + _score
                        """,
                    "params": {
                        "query_vector": embedding,
                        "encoder_boost": ENCODER_BOOST,
                    },
                },
            }
        }
    )
    hits = es_result["hits"]["hits"]
    # print(hits)
    clean_result = []
    for hit in hits:
        clean_result.append({
            "chvr": hit["_source"]["chvr"], # add "chvr" field
            "company": hit["_source"]["company"], # add "company" field
            "trans1": hit["_source"]["trans1"], # add "trans1" field
            "context": hit["_source"]["context"],
            "score": hit["_score"],
        })
    print('done')
    return clean_result


def query_question1(question: str, index_name: str, top_n: int):
    embedding = sentence_transformer.encode(question)
    es_result = es_client.search(
        index=index_name,
        size=top_n,
        from_=0,
        source=["auther", "book", "page", "chapter","content"],
        query={
            "script_score": {   
                #  "query": {
                #     "bool": {
                #         "must":[
                #             {"match": {"company": company}}
                #         ],
                #         "should": [
                #             {"match": {"context": question}}
                #         ]
                #     }
                # },
                "query": {
                    "match": {
                        "content": question,
                        # "company": company
                    }
                },
                "script": {
                    "source": """
                            (cosineSimilarity(params.query_vector, "embedding") + 1)* params.encoder_boost + _score
                        """,
                    "params": {
                        "query_vector": embedding,
                        "encoder_boost": ENCODER_BOOST,
                    },
                },
            }
        }
    )
    hits = es_result["hits"]["hits"]
    # print(hits)
    clean_result = []
    for hit in hits:
        clean_result.append({
            "auther": hit["_source"]["auther"], # add "chvr" field
            "book": hit["_source"]["book"], # add "company" field
            "page": hit["_source"]["page"], # add "trans1" field
            "chapter": hit["_source"]["chapter"],
            "content": hit["_source"]["content"],
            "score": hit["_score"],
        })
    print('done')
    return clean_result



def get_recent_data(index_name: str):
    try:
        es_result = es_client.search(
            index=index_name,
            size=1,
            sort={"time": {"order": "desc"}},
            query={
                "match_all": {}
            }
        )
        hits = es_result["hits"]["hits"]
        clean_result = []
        for hit in hits:
            clean_result.append({
                "phoneno": hit["_source"]["phoneno"],
                "time": hit["_source"]["time"],
                "context": hit["_source"]["context"],
            })
        return clean_result
    except es_exceptions.NotFoundError:
        # Return default values if the index is not found
        return [{"phoneno": '0', "time": '0', "context": '0'}]

def string_convert(x):
    string_answer = ''
    for i in range(len(x)):
        valuess = x[0]['content']
        # print(valuess)
        string_answer += valuess + '.'

    return string_answer
def string_convert1(x):
    string_answer = ''
    for i in range(len(x)):
        valuess = x[0]['context']
        # print(valuess)
        string_answer += valuess + '.'

    return string_answer



#for handling sessions
def create_session(index_name) -> None:
    es_client.options(ignore_status=404).indices.delete(index=index_name)
    es_client.options(ignore_status=400).indices.create(
        index=index_name,
        mappings={
            "properties": {
                "embedding": {
                    "type": "dense_vector",
                    "dims": EMBEDDING_DIMS,
                },
                "phoneno": {
                    "type": "text",
                },
                "time": {
                    "type": "date",   
                    # "format": "yyyy-MM-dd HH:mm:ss",   # Specify the date format
                },
                "context": {
                    "type": "text",
                },
            }
        }
    )

def refresh_session(index_name) -> None:
    es_client.indices.refresh(index=index_name)

# Add values to the database 
def index_session(index_name, time, cont):
        if cont == 0:
            liist = get_recent_data(index_name)
            if liist:
                cont = liist[0]['context']
        data = {
            "phoneno": index_name,
            "time": time,
            "context": cont,
        }
        print(data)
        es_client.options(max_retries=0).index(
            index=index_name,
            document=data
        )

def chatgpt_query(context, query):
        print(context)
        if not context.strip():
            return context
    
        prompt = f"You are a intelligent chatbot which gives accurate answer from data according to the query without changing the data"
        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"You hava the question and the get the answer from the data in less than 300 words but do not add anything by yourself\ndata:{context}\nquesiton:{query}\nAnswer:"}
        ]
        response = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=messages, temperature=0)
        return response.choices[0].message.content

def api_fun1(n, time1, cont):
    if not es_client.indices.exists(index=n):
        create_session(n)
    print("creating index")
    print("refreshing index")
    refresh_session(n)
    index_session(n, time1, cont)
    print("inserted")

def api_funb(query):
    global trans2
    global comen2
    print("hellooooo")
    if not es_client.indices.exists(index="elasticbook"):
        return "No Data", 0, 0
    x1 = query_question1(query, 'elasticbook', 1)
    # print(x1)
    # if x1:
    #     trans2 = x1[0]['chapter']
    #     comen2 = x1[0]['auther']
        # print(trans2)
    # print(x1[0]['context'])
    if x1:
        x = string_convert(x1)
        x = x.strip()
        x = chatgpt_query(x, query)

        ver = '\nAuther: ' + x1[0]['auther'] + "\nBook Name: " + x1[0]['book'] +  '\nPage Number: ' + x1[0]['page']
        # print(x+ver)
        if len(x) < 50:
            return x, 0, 0
        else:
            return x+ver, 0, 0
    else:
        return "Ask a Valid Question", 'No Verse Available', 'No Commentary Available'

def api_fun(query):
    global trans2
    global comen2
    if not es_client.indices.exists(index="elasticdb"):
        return "No Data"
    x1 = query_question(query, 'elasticdb', 1)
    if x1:
        trans2 = x1[0]['trans1']
        comen2 = x1[0]['context']
        # print(trans2)
    # print(x1[0]['context'])
    if x1:
        x = string_convert1(x1)
        x = x.strip()
        x = chatgpt_query(x, query)
        ver = '\n' + x1[0]['company'] +  ' ' + x1[0]['chvr']
        # print(x+ver)
        if len(x) < 50:
            return x, trans2, comen2
        else:
            return x+ver, trans2, comen2
    else:
        return "Ask a Valid Question", 'No Verse Available', 'No Commentary Available'