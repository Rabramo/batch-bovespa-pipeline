# Purpose: Scraper for the B3 website
# Author: Rogério Abramo
# Date: 2025-07-30
# Version: 0.1
# Description: This script opens the B3 page, renders JavaScript, captures the final HTML,
# and extracts daily data to a .parquet file.

from selenium import webdriver  # Used to control the browser via Selenium
from selenium.webdriver.chrome.options import Options  # Configure Chrome for headless execution
from selenium.webdriver.common.by import By  # Enum for locating elements
from selenium.webdriver.support.ui import WebDriverWait  # Wait utility for dynamic content
from selenium.webdriver.support import expected_conditions as EC  # Conditions used with WebDriverWait
from datetime import date  # Provides current date for partitioning
from io import StringIO  # Used to convert HTML to pandas-readable format
import pandas as pd  # Data analysis library
import os  # Filesystem and path operations
import boto3  # AWS SDK for Python to interact with S3

def upload_to_s3(file_path, bucket, s3_key):
    """
    Uploads a local file to an S3 bucket.

    Parameters:
    - file_path (str): Path to the local file.
    - bucket (str): Target S3 bucket name.
    - s3_key (str): S3 object key (path within the bucket).
    """
    s3 = boto3.client("s3")
    s3.upload_file(file_path, bucket, s3_key)
    print(f"[OK] File uploaded to s3://{bucket}/{s3_key}")

def fetch_b3_table():
    """
    Extracts B3 (Brasil Bolsa Balcão) IBOV stock data using Selenium and stores it in S3.

    - Loads dynamic page using headless Chrome
    - Parses the first HTML table with stock data
    - Converts to a DataFrame with reference date
    - Saves to local Parquet file partitioned by date
    - Uploads file to raw zone in S3
    - Saves fallback HTML if no table is found
    """
    url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"

    # Configure headless Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)

    try:
        # Wait for the table to load (max 15 seconds)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        tables = driver.find_elements(By.TAG_NAME, "table")

        if not tables:
            print("[WARN] Table not found after WebDriverWait.")
            print("[INFO] Saving fallback HTML for inspection.")
            with open("fallback_page.html", "w") as f:
                f.write(driver.page_source)
            return

        # Parse HTML table into DataFrame
        table_html = tables[0].get_attribute("outerHTML")
        df = pd.read_html(StringIO(table_html))[0]
        data_ref = date.today().isoformat()
        df["data_ref"] = data_ref

        # Save locally
        local_dir = f"data/raw/data_ref={data_ref}/"
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, "b3_pregao.parquet")
        df.to_parquet(local_path, index=False)
        print(f"[OK] File saved locally to {local_path}")
        print(df.head())

        # Upload to S3
        bucket = "b3-batch-pipeline-rogerio"
        s3_key = f"raw/b3_ibov/data_ref={data_ref}/b3_pregao.parquet"
        upload_to_s3(local_path, bucket, s3_key)

    except Exception as e:
        print(f"[ERROR] Failed to locate the table: {e}")
        print("[INFO] Saving fallback HTML for inspection.")
        with open("fallback_page.html", "w") as f:
            f.write(driver.page_source)

    finally:
        driver.quit()

# Entry point
if __name__ == "__main__":
    fetch_b3_table()
