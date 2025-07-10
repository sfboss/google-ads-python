import subprocess
import json
import time

SEED_KEYWORDS = ["salesforce automation"]
CUSTOMER_ID = "3399365278"
MASTER_LIST_FILE = "keyword_master_list.txt"
RAW_DATA_FILE = "keyword_raw_data.jsonl"
MAX_DEPTH = 2  # Set recursion depth to avoid infinite loops

def get_keyword_ideas(keyword):
    cmd = [
        "python", "adwords_service.py", "keyword-ideas",
        "-k", keyword, "-c", CUSTOMER_ID
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    try:
        data = json.loads(result.stdout)
        return data
    except Exception:
        return {}

def save_keywords(keywords, filename):
    with open(filename, "a") as f:
        for kw in keywords:
            f.write(kw + "\n")

def save_raw_data(keyword, raw_data, filename):
    entry = {
        "query_keyword": keyword,
        "timestamp": time.time(),
        "raw_response": raw_data
    }
    with open(filename, "a") as f:
        f.write(json.dumps(entry) + "\n")

def recursive_keyword_search(seed_keywords, max_depth=2):
    seen = set()
    queue = [(kw, 0) for kw in seed_keywords]

    while queue:
        keyword, depth = queue.pop(0)
        if keyword in seen or depth > max_depth:
            continue
        seen.add(keyword)
        print(f"Querying: {keyword} (depth {depth})")
        
        raw_data = get_keyword_ideas(keyword)
        save_raw_data(keyword, raw_data, RAW_DATA_FILE)
        
        ideas = [item['text'] for item in raw_data.get('keyword_ideas', [])]
        new_ideas = [kw for kw in ideas if kw not in seen]
        save_keywords(new_ideas, MASTER_LIST_FILE)
        
        for new_kw in new_ideas:
            queue.append((new_kw, depth + 1))
        time.sleep(1)  # Be polite to the API

if __name__ == "__main__":
    # Clear both files at the start
    open(MASTER_LIST_FILE, "w").close()
    open(RAW_DATA_FILE, "w").close()
    recursive_keyword_search(SEED_KEYWORDS, MAX_DEPTH)