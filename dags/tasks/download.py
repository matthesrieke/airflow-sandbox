from airflow.decorators import task
import requests
import json
import os
import datetime
import math

@task
def download_metadata(ids_json, worker_pos, total_workers, **kwargs):
    target_dir = kwargs["params"]["target_dir"]
    print("Downloading ids to directory %s: %s" % (target_dir, ids_json["ids"]))
    ids = json.loads(ids_json["ids"])
    
    ids_split = split(ids, total_workers)
    print("splitted ids: %s" % ids_split)
    print("Worker #%s with ids: %s" % (worker_pos, ids_split[worker_pos]))
    for i in ids_split[worker_pos]:
        resp = requests.get("https://api.eumetsat.int/data/download/products/%s/metadata?format=json" % i)
    
        f = open("%s/%s.json" % (target_dir, i), "w")
        f.write(resp.text)
        f.close()

    return target_dir


def split(arr, n):
    k, m = divmod(len(arr), n)
    result = []
    for i in range(n):
        result.append(arr[i*k + min(i, m) : (i+1)*k + min(i+1, m)])
    return result

