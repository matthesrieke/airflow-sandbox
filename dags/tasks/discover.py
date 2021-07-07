from airflow.decorators import task
import requests
import json

@task
def discover_ids(**kwargs):
    print("params: %s" % kwargs["params"])
    coll_id = kwargs["params"]["collection_id"]
    resp = requests.get("https://api.eumetsat.int/data/search-products/os?format=json&pi=%s&si=0&c=5&sort=start,time,0&dtstart=2021-06-17T09:02:58Z&dtend=2021-06-18T09:02:58Z" % coll_id)

    data = json.loads(resp.text)
    ids = list(map(lambda f: f["id"], data["features"]))
    result_value = json.dumps(ids)
    
    print(result_value)
    return {"ids": result_value}

