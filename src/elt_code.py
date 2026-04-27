import pandas as pd 
import requests
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from src.db_utils import DBLoader

log = logging.getLogger(__name__)

class ELT:

    def __init__(self):
        self.__BASE_URL = "https://xkcd.com/info.0.json"
        self.__db_util = DBLoader('raw_xkcd_comics')

    def __pre_process_data(self, raw_data):
        
        if not raw_data:
            return pd.DataFrame()
        
        raw_df = pd.DataFrame(raw_data)
        original_count = len(raw_df)
        print("the Raw data is: ", raw_df)
        logging.info("Preprocessing %s raw comic(s)...", original_count)

        # 1. De-duplicating the comics
        before = len(raw_df)
        processed_df = raw_df.drop_duplicates(subset=["num"], keep="first")
        if len(processed_df) < before:
            log.warning("Dropped %s duplicates of comics", before-len(processed_df))

        # 2. Ensure all expected columns exist in the latest comic
        expected_columns = ["num", "title", "safe_title", "alt", "img", 
                    "transcript", "link", "news", "year","month", "day"]
        
        # Add expected columns with None if it does not exist
        for col in expected_columns:
            if col not in processed_df.columns:
                processed_df[col] = None

        #Delete the columns in the dataframe that are not present in the expected columns list
        for col in processed_df.columns:
            if col not in expected_columns:
                processed_df = processed_df.drop(col, axis=1)

        # 3. Stripping leading or trailing whitespaces and convert empty strings to None
        TEXT_FIELDS = ["title", "safe_title", "alt", "transcript", "link", "news"]
        for col in TEXT_FIELDS:
            processed_df[col] = processed_df[col].astype(str).str.strip()
            processed_df[col] = processed_df[col].replace({"":None})

        # 4. Convert day, month, years from string to integer
        DATE_FIELDS = ["day", "month", "year"]
        for col in DATE_FIELDS:
            processed_df[col] = processed_df[col].astype(int)

        return processed_df
    
    def check_comic_availability(self, response):
        """Standalone response check for the Airflow HttpSensor."""
        try:
            log.info("Checking for new comic availability...")
            data = response.json()
            today = datetime.now()
        
            return (int(data["year"]) == today.year and 
                    int(data["month"]) == today.month and
                    int(data["day"]) == today.day)

        except Exception as e:
            log.error("Polling check failed: %s", e)
            return False
    
    def extract_and_load(self):
        """Fetch the latest comic and load it into the DB."""
        response = requests.get(self.__BASE_URL, timeout=30)
        response.raise_for_status()
        comic = response.json()

        # If missing comics detected, backfill before processing the latest comic to avoid gaps in the data.
        if int(response["num"])-1 != self.__db_util.get_latest_comic_num_in_db():
            missing_comics = int(response["num"]) - self.__db_util.get_latest_comic_num_in_db()
            if missing_comics > 0:
                # Get the missing comics in parallel to fill the gap
                log.info("Detected %s missing comic(s). Backfilling...", missing_comics)
                self.backfill_historical_comics(start_num=self.__db_util.get_latest_comic_num_in_db()+1, end_num=int(response["num"])-1)
            else:
                log.info("No missing comics detected. Proceeding with the latest comic.")


        pre_processed_data = self.__pre_process_data([comic])

        if pre_processed_data.empty:
            return False
        
        if_success = self.__db_util.insert_data(pre_processed_data)
        
        return if_success

    def __fetch_latest_comic_num(self):
        """Hit the API to get the most recent comic number."""
        resp = requests.get(self.__BASE_URL, timeout=10)
        resp.raise_for_status()
        return resp.json()["num"]

    def __fetch_one(self, num):
        """Helper to fetch a single comic for the thread pool."""
        try:
            resp = requests.get(f"https://xkcd.com/{num}/info.0.json", timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning("Could not fetch comic %s: %s", num, e)
            return None

    def backfill_historical_comics(self, start_num=1, end_num=None, max_workers=10):
        """Backfill comics from start_num to end_num using threads.
        If end_num is not provided, it fetches the latest comic number automatically."""
        if end_num is None:
            end_num = self.__fetch_latest_comic_num()
        
        log.info("Backfilling comics from %s to %s (threads=%s)...", start_num, end_num, max_workers)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(self.__fetch_one, range(start_num, end_num + 1)))
        
        comics = [r for r in results if r]
        
        if not comics:
            log.info("No comics fetched during backfill.")
            return 0
        
        df = self.__pre_process_data(comics)
        inserted = self.__db_util.insert_data(df)
        log.info("Backfill complete. Inserted %s comics.", inserted)
        return inserted

    def ensure_historical_data(self):
        """Check if raw table exists and has data; if not, backfill everything."""
        from sqlalchemy import inspect as sa_inspect, text
        engine = self.__db_util._DBLoader__engine
        
        inspector = sa_inspect(engine)
        if not inspector.has_table('raw_xkcd_comics'):
            log.info("raw_xkcd_comics table does not exist yet. Starting historical backfill...")
            return self.backfill_historical_comics(start_num=1)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM raw_xkcd_comics"))
            count = result.scalar()
        
        if count and count > 0:
            log.info("raw_xkcd_comics already has %s rows. Skipping backfill.", count)
            return 0
        
        log.info("raw_xkcd_comics is empty. Starting historical backfill...")
        return self.backfill_historical_comics(start_num=1)