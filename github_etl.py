import logging
import sys
import os
import uuid
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
import tenacity
import yaml

# ================= LOGGING =================

LOG_FILE = 'github_etl.log'
LOG_FORMAT = '%(asctime)s - %(levelname)s - [%(etl_id)s] - %(message)s'

formatter = logging.Formatter(LOG_FORMAT)
handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf8')
handler.setFormatter(formatter)

logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger("github_etl")

# ================= CONFIG =================

def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

conf = load_config()

TELEGRAM_TOKEN = conf["telegram"]["token"]
TELEGRAM_CHAT_ID = conf["telegram"]["chat_id"]

# ================= TELEGRAM =================

def send_telegram_message(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram config not set")
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }
        requests.post(url, data=payload, timeout=10)
        logger.info("Telegram message sent")
    except Exception as e:
        logger.error(f"Telegram error: {e}")

# ================= RETRY =================

def log_retry_attempt(retry_state):
    logger.warning(
        f"Retry #{retry_state.attempt_number} failed: "
        f"{retry_state.outcome.exception()} | next in {retry_state.upcoming_sleep}s"
    )

retry_config = tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_fixed(2),
    before_sleep=log_retry_attempt,
    reraise=True
)

# ================= STATE =================

STATE_FILE = "state.txt"

def get_last_run():
    if not os.path.exists(STATE_FILE):
        return "1970-01-01T00:00:00Z"
    with open(STATE_FILE, "r") as f:
        return f.read().strip()

def save_last_run(value):
    with open(STATE_FILE, "w") as f:
        f.write(value)

# ================= EXTRACT =================

@retry_config
def fetch_repos(updated_after):
    url = "https://api.github.com/search/repositories"

    headers = {}
    if conf["api"].get("token"):
        headers["Authorization"] = f"Bearer {conf['api']['token']}"

    params = {
        "q": f"{conf['api']['query']} pushed:>{updated_after}",
        "sort": "updated",
        "order": "desc",
        "per_page": conf["api"]["per_page"]
    }

    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json().get("items", [])

# ================= TRANSFORM =================

def transform(data, etl_id):
    rows = []

    for item in data:
        rows.append({
            "repo_id": item["id"],
            "name": item["name"],
            "owner": item["owner"]["login"],
            "stars": item["stargazers_count"],
            "forks": item["forks_count"],
            "language": item["language"],
            "created_at": item["created_at"],
            "updated_at": item["updated_at"],
            "url": item["html_url"],
            "etl_id": etl_id
        })

    return pd.DataFrame(rows)

# ================= LOAD =================

def load_db(df):
    db = conf["postgres"]

    engine = create_engine(
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['db']}"
    )

    df.to_sql(db["table"], engine, if_exists="append", index=False)

def save_parquet(df):
    path = conf["storage"]["parquet_path"]
    os.makedirs(path, exist_ok=True)

    filename = f"{path}/data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(filename, compression="snappy", index=False)

# ================= MAIN =================

def main():
    etl_id = str(uuid.uuid4())
    adapter = logging.LoggerAdapter(logger, {"etl_id": etl_id})

    start_time = datetime.utcnow()
    total_rows = 0
    error = False

    try:
        last_run = get_last_run()
        adapter.info(f"Last run: {last_run}")

        data = fetch_repos(last_run)
        df = transform(data, etl_id)

        if not df.empty:
            load_db(df)
            save_parquet(df)

            new_last_run = df["updated_at"].max()
            save_last_run(new_last_run)

            total_rows = len(df)
            adapter.info(f"Loaded {total_rows} rows")
        else:
            adapter.info("No new data")

        duration = datetime.utcnow() - start_time

        message = (
            f"<b>GitHub ETL завершен</b>\n"
            f"Новых записей: {total_rows}\n"
            f"Длительность: {duration}"
        )

    except Exception as e:
        logger.exception("Critical error")
        error = True
        message = f"<b>Ошибка ETL</b>\n{str(e)}"

    finally:
        send_telegram_message(message)
        sys.exit(1 if error else 0)


if __name__ == "__main__":
    main()
