import os
import time
import requests
import yaml
import json
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests_aws4auth import AWS4Auth
from botocore.session import Session

load_dotenv()

API_KEY = os.getenv("REDIS_CLOUD_API_KEY")
API_SECRET = os.getenv("REDIS_CLOUD_API_SECRET")
SUBSCRIPTION_ID = os.getenv("REDIS_CLOUD_SUBSCRIPTION_ID")
API_URL = "https://api.redislabs.com/v1"

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

THROUGHPUT_THRESHOLD = config.get('throughput_threshold', 0.8)
MEMORY_THRESHOLD = config.get('memory_threshold', 0.8)
CPU_THRESHOLD = config.get('cpu_threshold', 0.6)
LATENCY_THRESHOLD_MS = config.get('latency_threshold_ms', 3)
PAYLOAD_SIZE_THRESHOLD_KB = config.get('payload_size_threshold_kb', 3)
PROM_SERVER_URL = config.get('prometheus_server_url', 'http://localhost:9090')
PROM_QUERY_PERIOD = config.get('prometheus_query_period', '1h')
CLOUD_API_QUERY_INTERVAL_SECONDS = config.get('cloud_api_query_interval_seconds', 3600)
CLOUD_API_QUERY_INTERVAL_SECONDS_AUTOSCALE = config.get('cloud_api_query_interval_seconds_autoscale', 60)

# Autoscaling configuration
MEMORY_SCALING_PERCENTAGE = config.get('memory_scaling_percentage', 20)
THROUGHPUT_SCALING_PERCENTAGE = config.get('throughput_scaling_percentage', 20)

# --- Caching for Redis API ---
_redis_cache = {
    'subscriptions': None,
    'databases': {},
    'last_fetch': None
}

# --- Session for HTTP requests ---
_session = None
_prom_auth = {}

def get_prom_auth(prom_url):
    global _prom_auth
    if prom_url.startswith('https://aps-workspaces.'):
        aws_region = prom_url.split('.')[1]
        if _prom_auth.get(aws_region) is None:
            credentials = Session().get_credentials()
            _prom_auth[aws_region] = AWS4Auth(region=aws_region, service='aps', refreshable_credentials=credentials)
        return _prom_auth[aws_region]
    return None

def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        # Configure session for better performance
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=3,
            pool_block=False
        )
        _session.mount('http://', adapter)
        _session.mount('https://', adapter)
    return _session

AUTOSCALE_QUERY_PERIOD = config.get('autoscale_query_period', '5m')

def is_any_autoscale_enabled():
    # Import here to avoid circular import
    try:
        import autoscaling
        enabled = autoscaling.get_all_autoscale_enabled()
        return bool(enabled)
    except Exception as e:
        return False

def get_subscriptions_cached():
    now = datetime.utcnow()
    # Use shorter TTL if any DB has autoscaling enabled
    if is_any_autoscale_enabled():
        cache_ttl = timedelta(seconds=CLOUD_API_QUERY_INTERVAL_SECONDS_AUTOSCALE)
    else:
        cache_ttl = timedelta(seconds=CLOUD_API_QUERY_INTERVAL_SECONDS)
    if _redis_cache['subscriptions'] is not None and _redis_cache['last_fetch'] and now - _redis_cache['last_fetch'] < cache_ttl:
        return _redis_cache['subscriptions']
    subs = get_subscriptions()
    _redis_cache['subscriptions'] = subs
    _redis_cache['databases'] = {}  # clear DB cache on new subs fetch
    _redis_cache['last_fetch'] = now
    return subs

def get_databases_for_subscription_cached(subscription_id):
    now = datetime.utcnow()
    # Use shorter TTL if any DB has autoscaling enabled
    if is_any_autoscale_enabled():
        cache_ttl = timedelta(seconds=CLOUD_API_QUERY_INTERVAL_SECONDS_AUTOSCALE)
    else:
        cache_ttl = timedelta(seconds=CLOUD_API_QUERY_INTERVAL_SECONDS)
    if (subscription_id in _redis_cache['databases'] and _redis_cache['last_fetch'] and now - _redis_cache['last_fetch'] < cache_ttl):
        return _redis_cache['databases'][subscription_id]
    dbs = get_databases_for_subscription(subscription_id)
    _redis_cache['databases'][subscription_id] = dbs
    return dbs

# --- Pricing cache and fetch ---
_pricing_cache = {
    'pricing': {},  # {subscription_id: pricing_list}
    'last_fetch': {},  # {subscription_id: datetime}
}

PRICING_CACHE_TTL_SECONDS = 3600  # 1 hour

def get_pricing_for_subscription(subscription_id):
    now = datetime.utcnow()
    last_fetch = _pricing_cache['last_fetch'].get(subscription_id)
    if (
        subscription_id in _pricing_cache['pricing']
        and last_fetch
        and (now - last_fetch).total_seconds() < PRICING_CACHE_TTL_SECONDS
    ):
        return _pricing_cache['pricing'][subscription_id]
    url = f"{API_URL}/subscriptions/{subscription_id}/pricing"
    headers = {
        "accept": "application/json",
        "x-api-key": API_KEY,
        "x-api-secret-key": API_SECRET
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        pricing = data.get("pricing", [])
        _pricing_cache['pricing'][subscription_id] = pricing
        _pricing_cache['last_fetch'][subscription_id] = now
        return pricing
    except Exception as e:
        return []

# --- Shard Type Pricing and Unit Types Cache ---
_shardtype_cache = {
    'types': None,
    'pricings': None
}

def get_shard_types():
    if _shardtype_cache['types'] is not None:
        return _shardtype_cache['types']
    url = 'https://app.redislabs.com/api/v1/shardTypes'
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    _shardtype_cache['types'] = data.get('shardTypes', [])
    return _shardtype_cache['types']

def get_shard_type_pricings():
    if _shardtype_cache['pricings'] is not None:
        return _shardtype_cache['pricings']
    url = 'https://app.redislabs.com/api/v1/shardTypePricings'
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    _shardtype_cache['pricings'] = data.get('shardTypePricings', [])
    return _shardtype_cache['pricings']

# --- Existing API functions ---
def get_subscriptions():
    url = f"{API_URL}/subscriptions"
    headers = {
        "accept": "application/json",
        "x-api-key": API_KEY,
        "x-api-secret-key": API_SECRET
    }
    session = get_session()
    response = session.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("subscriptions", [])

def get_databases_for_subscription(subscription_id):
    url = f"{API_URL}/subscriptions/{subscription_id}/databases?offset=0&limit=100"
    headers = {
        "accept": "application/json",
        "x-api-key": API_KEY,
        "x-api-secret-key": API_SECRET
    }
    session = get_session()
    response = session.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("subscription", [])[0].get("databases", [])

def query_prometheus(prom_url, promql, bdb=None, cluster=None):
    try:
        session = get_session()
        resp = session.get(f"{prom_url}/api/v1/query", params={"query": promql}, timeout=15, auth=get_prom_auth(prom_url))
        resp.raise_for_status()
        data = resp.json()
        if data["status"] == "success" and data["data"]["result"]:
            for result in data["data"]["result"]:
                metric = result.get("metric", {})
                if (bdb is None or metric.get("bdb") == bdb) and (cluster is None or metric.get("cluster") == cluster):
                    return float(result["value"][1])
            return None  # No matching result found
        else:
            return None
    except Exception as e:
        return None

def query_prometheus_batch(prom_url, queries):
    """
    Batch query multiple Prometheus metrics at once
    queries: list of tuples (promql, bdb, cluster, metric_name)
    returns: dict of {metric_name: value}
    """
    results = {}
    session = get_session()
    
    # Create all requests
    requests_data = []
    for promql, bdb, cluster, metric_name in queries:
        cluster_prom_url = prom_url.get(cluster) if isinstance(prom_url, dict) else prom_url
        requests_data.append({
            'url': f"{cluster_prom_url}/api/v1/query",
            'params': {"query": promql},
            'metric_name': metric_name,
            'bdb': bdb,
            'cluster': cluster
        })
    
    # Execute requests in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_request = {
            executor.submit(_execute_prometheus_query, session, req): req 
            for req in requests_data
        }
        
        for future in as_completed(future_to_request):
            request = future_to_request[future]
            try:
                result = future.result()
                results[request['metric_name']] = result
            except Exception as e:
                results[request['metric_name']] = None
    
    return results

def _execute_prometheus_query(session, request):
    """Helper function to execute a single Prometheus query"""
    try:
        resp = session.get(request['url'], params=request['params'], timeout=15, auth=get_prom_auth(request['url']))
        resp.raise_for_status()
        data = resp.json()
        if data["status"] == "success" and data["data"]["result"]:
            for result in data["data"]["result"]:
                metric = result.get("metric", {})
                if (request['bdb'] is None or metric.get("bdb") == request['bdb']) and \
                   (request['cluster'] is None or metric.get("cluster") == request['cluster']):
                    return float(result["value"][1])
        return None
    except Exception as e:
        return None

def get_metric_from_metrics_text(metrics_text, metric_name, labels):
    # Match the metric line and capture the label block and value
    pattern = rf'{re.escape(metric_name)}\{{([^}}]+)\}}\s+([0-9.eE+-]+)'
    regex = re.compile(pattern)
    for match in regex.finditer(metrics_text):
        label_block = match.group(1)
        value = match.group(2)
        # Check if all required labels are present in the label block
        if all(f'{k}="{v}"' in label_block for k, v in labels.items()):
            return float(value)
    return None

def check_database_metrics_prometheus(cluster_label, db, thresholds):
    bdb = str(db.get("databaseId"))
    cluster = db.get("subscriptionId")
    mem_limit_gb = db.get("memoryLimitInGb", 0)
    throughput_limit = db.get("throughputMeasurement", {}).get("value", 0)
    # Query Prometheus for each metric
    period = PROM_QUERY_PERIOD
    prom_url = PROM_SERVER_URL.get(cluster_label) if isinstance(PROM_SERVER_URL, dict) else PROM_SERVER_URL
    labels = f'cluster="{cluster_label}",bdb="{bdb}"'
    throughput = query_prometheus(prom_url, f'max_over_time(bdb_total_req_max{{{labels}}}[{period}])', bdb=bdb, cluster=cluster_label)
    memory = query_prometheus(prom_url, f'max_over_time(bdb_used_memory{{{labels}}}[{period}])', bdb=bdb, cluster=cluster_label)
    cpu = query_prometheus(prom_url, f'max_over_time(bdb_shard_cpu_user_max{{{labels}}}[{period}])', bdb=bdb, cluster=cluster_label)
    latency = query_prometheus(prom_url, f'max_over_time(bdb_avg_latency_max{{{labels}}}[{period}])', bdb=bdb, cluster=cluster_label)
    
    # Calculate average payload size (bytes per request)
    payload_size = None
    # Numerator: latest ingress + egress bytes
    ingress_bytes = query_prometheus(prom_url, f'bdb_ingress_bytes_max{{{labels}}}', bdb=bdb, cluster=cluster_label)
    egress_bytes = query_prometheus(prom_url, f'bdb_egress_bytes_max{{{labels}}}', bdb=bdb, cluster=cluster_label)
    # Denominator: max_over_time of bdb_total_req_max over the period
    throughput_max = query_prometheus(prom_url, f'max_over_time(bdb_total_req_max{{{labels}}}[{period}])', bdb=bdb, cluster=cluster_label)
    if ingress_bytes is not None and egress_bytes is not None and throughput_max is not None and throughput_max > 0:
        total_bytes = ingress_bytes + egress_bytes
        payload_size = total_bytes / throughput_max  # bytes per request

    throughput_ok = throughput is not None and throughput < thresholds["throughput_threshold"] * throughput_limit
    memory_ok = memory is not None and memory < thresholds["memory_threshold"] * mem_limit_gb * 1024 * 1024 * 1024  # bytes
    cpu_ok = cpu is not None and cpu < thresholds["cpu_threshold"] * 100
    latency_ok = latency is None or latency < thresholds["latency_threshold_ms"]
    payload_size_ok = payload_size is None or payload_size < thresholds.get("payload_size_threshold_kb", 1024) * 1024  # Convert KB to bytes

    result = {
        "subscription_id": cluster,
        "database_id": bdb,
        "database_name": db.get("name"),
        "metrics": {
            "throughput": throughput,
            "throughput_limit": throughput_limit,
            "memory": memory,
            "memory_limit_bytes": mem_limit_gb * 1024 * 1024 * 1024,
            "cpu": cpu,
            "latency_ms": latency,
            "payload_size_bytes": payload_size
        },
        "thresholds": thresholds,
        "status": {
            "throughput_ok": throughput_ok,
            "memory_ok": memory_ok,
            "cpu_ok": cpu_ok,
            "latency_ok": latency_ok,
            "payload_size_ok": payload_size_ok
        }
    }
    return result

def get_metrics_for_db(cluster_label, db, thresholds, subscription_name, period):
    bdb = str(db.get("databaseId"))
    cluster = db.get("subscriptionId")
    mem_limit_gb = db.get("memoryLimitInGb", 0)
    throughput_limit = db.get("throughputMeasurement", {}).get("value", 0)
    # Only set static/config data, no Prometheus queries
    metrics = {
        "throughput": None,
        "throughput_limit": throughput_limit,
        "memory": None,
        "memory_limit_bytes": mem_limit_gb * 1024 * 1024 * 1024,
        "cpu": None,
        "latency_ms": None,
        "payload_size_bytes": None
    }
    # Calculate status (all will be None, so all will be False)
    throughput_ok = False
    memory_ok = False
    cpu_ok = False
    latency_ok = False
    payload_size_ok = False
    # Calculate max scaling limits based on number of shards
    clustering = db.get("clustering", {})
    num_shards = clustering.get("numberOfShards", 1)
    replication = db.get("replication", False)
    max_throughput = num_shards * 25000  # 25K ops/sec per shard
    max_memory_gb = num_shards * 25 * (2 if replication else 1)  # 25GB per shard, doubled if replication
    result = {
        "subscription_id": cluster,
        "subscription_name": subscription_name,
        "database_id": bdb,
        "database_name": db.get("name"),
        "metrics": metrics,
        "thresholds": thresholds,
        "status": {
            "throughput_ok": throughput_ok,
            "memory_ok": memory_ok,
            "cpu_ok": cpu_ok,
            "latency_ok": latency_ok,
            "payload_size_ok": payload_size_ok
        },
        "max_scaling": {
            "memory_gb": max_memory_gb,
            "throughput_ops": max_throughput
        },
        "downscale_memory_mb": None,
        "downscale_throughput_ops": None,
        "downscale_price_suggestion": None
    }
    return result

def nice_memory_step(usage_bytes):
    """
    Calculate a nice memory step that leaves headroom below the threshold.
    Ensures current usage is comfortably below 80% of the suggested limit.
    """
    mb = usage_bytes / (1024 * 1024)
    threshold = 0.8  # 80% threshold
    
    # Calculate the minimum memory needed to keep usage below threshold
    min_memory_needed = mb / threshold
    
    # Apply nice rounding with headroom
    if min_memory_needed <= 100:
        suggested = 100
    elif min_memory_needed <= 500:
        suggested = 500
    elif min_memory_needed <= 1024:
        suggested = 1024
    else:
        # Round up to next GB
        suggested = int((min_memory_needed + 1023) // 1024) * 1024
    
    # Verify headroom: usage should be below threshold
    if mb / suggested >= threshold:
        # If still too close to threshold, go to next step
        if suggested <= 100:
            suggested = 500
        elif suggested <= 500:
            suggested = 1024
        elif suggested <= 1024:
            suggested = 2048
        else:
            # After 1GB, always jump by at least 1GB
            suggested += 1024
    
    return suggested

def nice_throughput_step(usage_ops):
    """
    Calculate a nice throughput step that leaves headroom below the threshold.
    Ensures current usage is comfortably below 80% of the suggested limit.
    """
    threshold = 0.8  # 80% threshold
    
    # Calculate the minimum throughput needed to keep usage below threshold
    min_throughput_needed = usage_ops / threshold
    
    # Apply nice rounding with headroom
    if min_throughput_needed <= 100:
        suggested = 100
    elif min_throughput_needed <= 500:
        suggested = 500
    elif min_throughput_needed <= 1000:
        suggested = 1000
    else:
        # Round up to next 1K
        suggested = int((min_throughput_needed + 999) // 1000) * 1000
    
    # Verify headroom: usage should be below threshold
    if usage_ops / suggested >= threshold:
        # If still too close to threshold, go to next step
        if suggested <= 100:
            suggested = 500
        elif suggested <= 500:
            suggested = 1000
        elif suggested <= 1000:
            suggested = 2000
        else:
            # After 1K, always jump by at least 1K
            suggested += 1000
    
    return suggested

def get_best_downscale_price(region, cloud, memory_mb, throughput_ops, ha_enabled):
    shard_types = get_shard_types()
    pricings = get_shard_type_pricings()
    best = None
    for st in shard_types:
        st_id = st.get('id')
        st_name = st.get('name')
        st_mem_gb = st.get('memory_size_gb')
        st_thr = st.get('throughput')
        st_mem_mb = st_mem_gb * 1024 if st_mem_gb else None  # Convert GB to MB
        if not st_mem_mb or not st_thr:
            continue
        units_needed = max(
            math.ceil(memory_mb / st_mem_mb),
            math.ceil(throughput_ops / st_thr)
        )
        # Find price for this unit type in the right region/cloud
        price_entry = next((p for p in pricings if p['shard_type_id'] == st_id and p['region_name'] == region and p['cloud_name'] == cloud), None)
        if not price_entry:
            continue
        price_per_unit = price_entry['price']
        total_price = price_per_unit * units_needed
        if ha_enabled:
            total_price *= 2
        if best is None or total_price < best['price']:
            best = {
                'price': round(total_price, 4),
                'unit_type': st_name,
                'units_needed': units_needed
            }
    return best

def get_all_metrics(period=None):
    prom_period = period if period else '5m'
    autoscale_period = AUTOSCALE_QUERY_PERIOD
    subscriptions = get_subscriptions_cached()
    thresholds = {
        "throughput_threshold": THROUGHPUT_THRESHOLD,
        "memory_threshold": MEMORY_THRESHOLD,
        "cpu_threshold": CPU_THRESHOLD,
        "latency_threshold_ms": LATENCY_THRESHOLD_MS,
        "payload_size_threshold_kb": PAYLOAD_SIZE_THRESHOLD_KB
    }
    results = []
    
    # Collect all databases first
    all_databases = []
    for sub in subscriptions:
        sub_id = sub.get("id")
        sub_name = sub.get("name")
        databases = get_databases_for_subscription_cached(sub_id)
        if not databases:
            continue
        for db in databases:
            if db.get("activeActiveRedis") and db.get("crdbDatabases"):
                continue
            all_databases.append((sub, db))
    
    # Batch collect all Prometheus queries
    all_queries = []
    db_query_map = {}  # Map to track which queries belong to which database
    
    for sub, db in all_databases:
        sub_id = sub.get("id")
        sub_name = sub.get("name")
        db0 = db
        cluster_label = db0.get("cluster", None)
        if not cluster_label:
            private_endpoint = db0.get("privateEndpoint", "")
            if ".internal." in private_endpoint:
                cluster_label = private_endpoint.split(".internal.", 1)[1].split(":")[0]
            else:
                cluster_label = ""
        
        bdb = str(db.get("databaseId"))
        cluster = db.get("subscriptionId")
        labels = f'cluster="{cluster_label}",bdb="{bdb}"'
        
        # Create unique identifier for this database
        db_key = f"{sub_id}_{bdb}"
        db_query_map[db_key] = {
            'sub': sub,
            'db': db,
            'cluster_label': cluster_label,
            'bdb': bdb,
            'cluster': cluster,
            'sub_name': sub_name
        }
        
        # Add all queries for this database
        queries = [
            # UI metrics (period_for_metrics)
            (f'max_over_time(bdb_total_req_max{{{labels}}}[{prom_period}])', bdb, cluster_label, f'{db_key}_throughput'),
            (f'max_over_time(bdb_used_memory{{{labels}}}[{prom_period}])', bdb, cluster_label, f'{db_key}_memory'),
            (f'max_over_time(bdb_shard_cpu_user_max{{{labels}}}[{prom_period}])', bdb, cluster_label, f'{db_key}_cpu'),
            (f'max_over_time(bdb_avg_latency_max{{{labels}}}[{prom_period}])', bdb, cluster_label, f'{db_key}_latency'),
            (f'max_over_time(bdb_ingress_bytes_max{{{labels}}}[{prom_period}])', bdb, cluster_label, f'{db_key}_ingress_bytes_max'),
            (f'max_over_time(bdb_egress_bytes_max{{{labels}}}[{prom_period}])', bdb, cluster_label, f'{db_key}_egress_bytes_max'),
            (f'max_over_time(bdb_total_req_max{{{labels}}}[{prom_period}])', bdb, cluster_label, f'{db_key}_throughput_max'),
            
            # Autoscaling metrics (period_for_autoscale)
            (f'max_over_time(bdb_total_req_max{{{labels}}}[{autoscale_period}])', bdb, cluster_label, f'{db_key}_throughput_autoscale'),
            (f'max_over_time(bdb_used_memory{{{labels}}}[{autoscale_period}])', bdb, cluster_label, f'{db_key}_memory_autoscale'),
            (f'max_over_time(bdb_shard_cpu_user_max{{{labels}}}[{autoscale_period}])', bdb, cluster_label, f'{db_key}_cpu_autoscale'),
            (f'max_over_time(bdb_avg_latency_max{{{labels}}}[{autoscale_period}])', bdb, cluster_label, f'{db_key}_latency_autoscale'),
            (f'bdb_ingress_bytes_max{{{labels}}}', bdb, cluster_label, f'{db_key}_ingress_bytes_autoscale'),
            (f'bdb_egress_bytes_max{{{labels}}}', bdb, cluster_label, f'{db_key}_egress_bytes_autoscale'),
        ]
        all_queries.extend(queries)
    
    # Execute all queries in parallel
    batch_results = query_prometheus_batch(PROM_SERVER_URL, all_queries)
    
    # Process results for each database
    for db_key, db_info in db_query_map.items():
        sub = db_info['sub']
        db = db_info['db']
        cluster_label = db_info['cluster_label']
        bdb = db_info['bdb']
        cluster = db_info['cluster']
        sub_name = db_info['sub_name']
        
        # Extract metrics from batch results
        throughput = batch_results.get(f'{db_key}_throughput')
        memory = batch_results.get(f'{db_key}_memory')
        cpu = batch_results.get(f'{db_key}_cpu')
        latency = batch_results.get(f'{db_key}_latency')
        ingress_bytes_max = batch_results.get(f'{db_key}_ingress_bytes_max')
        egress_bytes_max = batch_results.get(f'{db_key}_egress_bytes_max')
        throughput_max = batch_results.get(f'{db_key}_throughput_max')
        
        # Autoscaling metrics
        throughput_autoscale = batch_results.get(f'{db_key}_throughput_autoscale')
        memory_autoscale = batch_results.get(f'{db_key}_memory_autoscale')
        cpu_autoscale = batch_results.get(f'{db_key}_cpu_autoscale')
        latency_autoscale = batch_results.get(f'{db_key}_latency_autoscale')
        ingress_bytes_autoscale = batch_results.get(f'{db_key}_ingress_bytes_autoscale')
        egress_bytes_autoscale = batch_results.get(f'{db_key}_egress_bytes_autoscale')
        
        # Calculate payload sizes
        payload_size = None
        if ingress_bytes_max is not None and egress_bytes_max is not None and throughput_max is not None and throughput_max > 0:
            total_bytes_max = ingress_bytes_max + egress_bytes_max
            payload_size = total_bytes_max / throughput_max
        
        payload_size_autoscale = None
        if throughput_autoscale is not None and throughput_autoscale > 0:
            if ingress_bytes_autoscale is not None and egress_bytes_autoscale is not None:
                total_bytes_autoscale = ingress_bytes_autoscale + egress_bytes_autoscale
                payload_size_autoscale = total_bytes_autoscale / throughput_autoscale
        
        # Get database configuration
        mem_limit_gb = db.get("memoryLimitInGb", 0)
        throughput_limit = db.get("throughputMeasurement", {}).get("value", 0)
        
        # Use subscriptionPricing if present
        subscription_pricing = sub.get("subscriptionPricing", [])
        pricing_list = subscription_pricing if subscription_pricing else get_pricing_for_subscription(cluster)
        
        try:
            metrics_result = {
                "subscription_id": cluster,
                "subscription_name": sub_name,
                "database_id": bdb,
                "database_name": db.get("name"),
                "metrics": {
                    "throughput": throughput,
                    "throughput_limit": throughput_limit,
                    "memory": memory,
                    "memory_limit_bytes": mem_limit_gb * 1024 * 1024 * 1024,
                    "cpu": cpu,
                    "latency_ms": latency,
                    "payload_size_bytes": payload_size
                },
                "metrics_autoscale": {
                    "throughput": throughput_autoscale,
                    "throughput_limit": throughput_limit,
                    "memory": memory_autoscale,
                    "memory_limit_bytes": mem_limit_gb * 1024 * 1024 * 1024,
                    "cpu": cpu_autoscale,
                    "latency_ms": latency_autoscale,
                    "payload_size_bytes": payload_size_autoscale
                },
                "thresholds": thresholds,
                "status": {
                    "throughput_ok": throughput is not None and throughput < thresholds["throughput_threshold"] * throughput_limit,
                    "memory_ok": memory is not None and memory < thresholds["memory_threshold"] * mem_limit_gb * 1024 * 1024 * 1024,
                    "cpu_ok": cpu is not None and cpu < thresholds["cpu_threshold"] * 100,
                    "latency_ok": latency is None or latency < thresholds["latency_threshold_ms"],
                    "payload_size_ok": payload_size is None or payload_size < thresholds.get("payload_size_threshold_kb", 1024) * 1024
                }
            }
            
            # Add max_scaling calculation
            clustering = db.get("clustering", {})
            num_shards = clustering.get("numberOfShards", 1)
            replication = db.get("replication", False)
            max_throughput = num_shards * 25000  # 25K ops/sec per shard
            max_memory_gb = num_shards * 25 * (2 if replication else 1)  # 25GB per shard, doubled if replication
            metrics_result["max_scaling"] = {
                "memory_gb": max_memory_gb,
                "throughput_ops": max_throughput
            }
            
            # Downscale suggestion logic (use max_over_time for memory and throughput)
            downscale_memory_mb = None
            downscale_throughput_ops = None
            prom_url = PROM_SERVER_URL.get(cluster_label) if isinstance(PROM_SERVER_URL, dict) else PROM_SERVER_URL
            if metrics_result['status']['throughput_ok'] and metrics_result['status']['memory_ok'] and metrics_result['status']['cpu_ok'] and metrics_result['status']['latency_ok'] and metrics_result['status']['payload_size_ok']:
                # Use max_over_time for the period for safe downscale
                mem_used = query_prometheus(prom_url, f'max_over_time(bdb_used_memory{{{labels}}}[{prom_period}])', bdb=bdb, cluster=cluster_label) or 0
                thr_used = query_prometheus(prom_url, f'max_over_time(bdb_total_req_max{{{labels}}}[{prom_period}])', bdb=bdb, cluster=cluster_label) or 0
                downscale_memory_mb = nice_memory_step(mem_used)
                downscale_throughput_ops = nice_throughput_step(thr_used)
            metrics_result['downscale_memory_mb'] = downscale_memory_mb
            metrics_result['downscale_throughput_ops'] = downscale_throughput_ops
            
            downscale_price_suggestion = None
            if downscale_memory_mb and downscale_throughput_ops:
                region = db.get('region')
                # Get cloud provider from subscription cloudDetails
                cloud = None
                if sub.get('cloudDetails') and len(sub['cloudDetails']) > 0:
                    cloud = sub['cloudDetails'][0].get('provider')
                # Fallback to database provider if not found in subscription
                if not cloud:
                    cloud = db.get('provider') or db.get('cloudProvider') or db.get('cloud')
                ha_enabled = db.get('replication', False)
                downscale_price_suggestion = get_best_downscale_price(region, cloud, downscale_memory_mb, downscale_throughput_ops, ha_enabled)
            metrics_result['downscale_price_suggestion'] = downscale_price_suggestion
            
            # --- Price mapping logic ---
            price_hourly = None
            min_subscription_price = None
            db_type = db.get("typeDetails") or db.get("type")
            db_shards = db.get("clustering", {}).get("numberOfShards", 1)
            
            # Find matching Shards entry in pricing_list (ignore pricePeriod)
            price_entry = None
            for entry in pricing_list:
                if (
                    entry.get("type") == "Shards"
                    and (entry.get("typeDetails") == db_type or not db_type)
                    and entry.get("quantity") == db_shards
                ):
                    price_entry = entry
                    break
            
            # Fallback: use first Shards entry if no exact match
            if price_entry is None:
                for entry in pricing_list:
                    if entry.get("type") == "Shards":
                        price_entry = entry
                        break
            
            if price_entry:
                per_unit = price_entry.get("pricePerUnit")
                quantity = price_entry.get("quantity", db_shards)
                if per_unit is not None:
                    price_hourly = quantity * per_unit
            
            # Find MinimumPrice entry in pricing_list (ignore pricePeriod)
            for entry in pricing_list:
                if entry.get("type") == "MinimumPrice":
                    min_subscription_price = entry.get("pricePerUnit")
                    break
            
            metrics_result["price_hourly"] = price_hourly
            metrics_result["min_subscription_price"] = min_subscription_price
            result = metrics_result
            result["region"] = db.get("region")
            result["active_active"] = False
            result["subscription_id"] = sub_id
            result["db_status"] = db.get("status")
            results.append(result)
            
        except Exception as e:
            metrics_result = get_metrics_for_db(cluster_label, db, thresholds, sub_name, prom_period)
            results.append(metrics_result)
    return {"databases": results}

if __name__ == '__main__':
    subscriptions = get_subscriptions()
    thresholds = {
        "throughput_threshold": THROUGHPUT_THRESHOLD,
        "memory_threshold": MEMORY_THRESHOLD,
        "cpu_threshold": CPU_THRESHOLD,
        "latency_threshold_ms": LATENCY_THRESHOLD_MS
    }
    for sub in subscriptions:
        sub_id = sub.get("id")
        sub_name = sub.get("name")
        databases = get_databases_for_subscription(sub_id)
        if not databases:
            continue
        # Use the first database to get the cluster label
        db0 = databases[0]
        cluster_label = db0.get("cluster", None)
        if not cluster_label:
            private_endpoint = db0.get("privateEndpoint", "")
            if ".internal." in private_endpoint:
                cluster_label = private_endpoint.split(".internal.", 1)[1].split(":")[0]
            else:
                cluster_label = ""
        for db in databases:
            db_id = db.get("databaseId")
            db_name = db.get("name")
            check_database_metrics_prometheus(cluster_label, db, thresholds)
