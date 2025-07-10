import json
from fastapi import FastAPI, Depends, HTTPException
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import redis

# --- Налаштування ---
CASSANDRA_HOST = 'cassandra'
CASSANDRA_KEYSPACE = 'amazon_reviews'
REDIS_HOST = 'redis'
CACHE_TTL = 300  # 5 хвилин

# --- Ініціалізація ---
app = FastAPI(title="Amazon Reviews API")
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

def get_db_session():
    """Створює та повертає сесію Cassandra."""
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.row_factory = dict_factory
    return session

def get_from_cache(key: str):
    """Отримує дані з кешу Redis."""
    data = redis_client.get(key)
    return json.loads(data) if data else None

def set_to_cache(key: str, value: list):
    """Зберігає дані в кеш Redis."""
    redis_client.set(key, json.dumps(value, default=str), ex=CACHE_TTL)

# --- Ендпоінти ---
@app.get("/reviews/product/{product_id}", tags=["Reviews"])
def get_reviews_by_product(product_id: str, session = Depends(get_db_session)):
    cache_key = f"product:{product_id}"
    if cached_data := get_from_cache(cache_key):
        return {"source": "cache", "data": cached_data}

    query = "SELECT * FROM reviews_by_product WHERE product_id = %s"
    rows = list(session.execute(query, [product_id]))
    set_to_cache(cache_key, rows)
    return {"source": "database", "data": rows}

@app.get("/reviews/product/{product_id}/rating/{star_rating}", tags=["Reviews"])
def get_reviews_by_product_and_rating(product_id: str, star_rating: int, session = Depends(get_db_session)):
    cache_key = f"product:{product_id}:rating:{star_rating}"
    if cached_data := get_from_cache(cache_key):
        return {"source": "cache", "data": cached_data}
    
    query = "SELECT * FROM reviews_by_product_and_rating WHERE product_id = %s AND star_rating = %s"
    rows = list(session.execute(query, [product_id, star_rating]))
    set_to_cache(cache_key, rows)
    return {"source": "database", "data": rows}

@app.get("/reviews/customer/{customer_id}", tags=["Reviews"])
def get_reviews_by_customer(customer_id: int, session = Depends(get_db_session)):
    cache_key = f"customer:{customer_id}"
    if cached_data := get_from_cache(cache_key):
        return {"source": "cache", "data": cached_data}

    query = "SELECT * FROM reviews_by_customer WHERE customer_id = %s"
    rows = list(session.execute(query, [customer_id]))
    set_to_cache(cache_key, rows)
    return {"source": "database", "data": rows}

@app.get("/analytics/most-reviewed", tags=["Analytics"])
def get_most_reviewed_products(period: str, n: int = 10, session = Depends(get_db_session)):
    cache_key = f"most-reviewed:{period}:n{n}"
    if cached_data := get_from_cache(cache_key):
        return {"source": "cache", "data": cached_data}

    query = "SELECT product_id, product_title, total_reviews FROM most_reviewed_products_by_month WHERE period = %s LIMIT %s"
    rows = list(session.execute(query, [period, n]))
    set_to_cache(cache_key, rows)
    return {"source": "database", "data": rows}

@app.get("/analytics/most-productive", tags=["Analytics"])
def get_most_productive_customers(period: str, n: int = 10, session = Depends(get_db_session)):
    cache_key = f"most-productive:{period}:n{n}"
    if cached_data := get_from_cache(cache_key):
        return {"source": "cache", "data": cached_data}

    query = "SELECT customer_id, total_reviews FROM most_productive_customers_by_month WHERE period = %s LIMIT %s"
    rows = list(session.execute(query, [period, n]))
    set_to_cache(cache_key, rows)
    return {"source": "database", "data": rows}

@app.get("/analytics/haters", tags=["Analytics"])
def get_top_haters(period: str, n: int = 10, session = Depends(get_db_session)):
    cache_key = f"haters:{period}:n{n}"
    if cached_data := get_from_cache(cache_key):
        return {"source": "cache", "data": cached_data}

    query = "SELECT customer_id, hater_reviews FROM haters_by_month WHERE period = %s LIMIT %s"
    rows = list(session.execute(query, [period, n]))
    set_to_cache(cache_key, rows)
    return {"source": "database", "data": rows}

@app.get("/analytics/backers", tags=["Analytics"])
def get_top_backers(period: str, n: int = 10, session = Depends(get_db_session)):
    cache_key = f"backers:{period}:n{n}"
    if cached_data := get_from_cache(cache_key):
        return {"source": "cache", "data": cached_data}
    
    query = "SELECT customer_id, backer_reviews FROM backers_by_month WHERE period = %s LIMIT %s"
    rows = list(session.execute(query, [period, n]))
    set_to_cache(cache_key, rows)
    return {"source": "database", "data": rows}
