from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import logging
import os
import re
import time
from email.utils import parsedate_to_datetime # Untuk parse tanggal RSS (Inggris)

# DB Config
DB_PATH = '/opt/airflow/data/news_articles.db'

logger = logging.getLogger("airflow.task")

def create_table_if_not_exists():
    """Database Table Creation"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS news_articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT UNIQUE NOT NULL,
        published_at TEXT, 
        scraped_at TEXT
    );
    ''')
    # SQLite doesnt have Datetime or timestamp 
    conn.commit()
    conn.close()

def extract_news(**context):
    """News Extraction"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://www.google.com/'
    }

    raw_data = []

    logger.info("--- Processing Kompas ---")
    page_num = 3
    
    for page in range(1, page_num + 1):
        target_url = f'https://indeks.kompas.com/?page={page}'
        try:
            response = requests.get(target_url, headers=headers, timeout=20)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                all_links = soup.find_all('a', href=True) # Get all <a> tags
                
                for link in all_links:
                    url = link['href']
                    
                    # Get clean title
                    title_tag = link.find(['h2', 'h3', 'h4', 'div'], class_=re.compile(r'title|articleTitle', re.I))
                    title = title_tag.get_text(strip=True) if title_tag else link.get_text(strip=True)

                    # Ambil Tanggal (Raw) - Cari tag class 'date' di dalam link
                    raw_date_str = None
                    date_tag = link.find(['div', 'span'], class_=re.compile(r'date|articleDate', re.I))
                    if date_tag:
                        raw_date_str = date_tag.get_text(strip=True)

                    # Get link with '/read/' because kompas use it to identify that it is link to a news
                    # Get link with len>15 to avoid prev or next button
                    if '/read/' in url and title and len(title) > 15:
                        if '?' in url: url = url.split('?')[0] # Remove tracking url
                        
                        raw_data.append({
                            'source': 'Kompas',
                            'title': title,
                            'url': url,
                            'raw_date': raw_date_str,
                            'scraped_at': datetime.now().isoformat()
                        })
                time.sleep(1)
        except Exception as e:
            logger.error(f"Kompas Error: {e}")

    # Other Sources other than Kompas
    sources_config = [
        {'name': 'Liputan6', 'url': 'https://feed.liputan6.com/rss', 'type': 'rss'},
        {'name': 'Detik', 'url': 'https://news.detik.com/rss', 'type': 'rss'},
        {'name': 'Tempo', 'url': 'http://rss.tempo.co/nasional', 'type': 'rss'},
        {'name': 'CNN Indonesia', 'url': 'https://www.cnnindonesia.com/nasional/rss', 'type': 'rss'}
    ]

    for source in sources_config:
        logger.info(f"--- Processing {source['name']} ---")
        try:
            response = requests.get(source['url'], headers=headers, timeout=15)
            if response.status_code != 200: continue

            try:
                soup = BeautifulSoup(response.content, 'xml')
            except:
                soup = BeautifulSoup(response.content, 'html.parser')
            
            items = soup.find_all('item')
            if not items: items = soup.find_all('entry')

            for item in items:
                try:
                    title = item.find('title').text.strip() if item.find('title') else None
                    link = None
                    if item.find('link'): link = item.find('link').text.strip()
                    if not link and item.find('guid'): link = item.find('guid').text.strip()
                    
                    pub_date = None
                    if item.find('pubDate'): pub_date = item.find('pubDate').text.strip()
                    elif item.find('published'): pub_date = item.find('published').text.strip()

                    if title and link:
                        raw_data.append({
                            'source': source['name'],
                            'title': title,
                            'url': link,
                            'raw_date': pub_date, 
                            'scraped_at': datetime.now().isoformat()
                        })
                except: continue
        except Exception as e:
            logger.error(f"Error {source['name']}: {e}")

    context['ti'].xcom_push(key='raw_data', value=raw_data)

def transform_data(**context):
    """Cleaning data"""
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract_task')
    if not raw_data: return

    df = pd.DataFrame(raw_data)
    df.drop_duplicates(subset=['url'], inplace=True)

    # Parsing Date
    def smart_parse_date(date_str):
        if not date_str:
            return datetime.now().isoformat()

        try:
            dt = parsedate_to_datetime(date_str)
            return datetime(*dt[:6]).isoformat()
        except:
            pass

        # Parsing Indonesian Format
        bulan_indo = {
            'januari': '01', 'februari': '02', 'maret': '03', 'april': '04',
            'mei': '05', 'juni': '06', 'juli': '07', 'agustus': '08',
            'september': '09', 'oktober': '10', 'november': '11', 'desember': '12',
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'jun': '06',
            'jul': '07', 'agu': '08', 'sep': '09', 'okt': '10', 'nov': '11', 'des': '12'
        }
        
        try:
            # Cleaning date, remove wib, wita, wit and comma
            clean_str = date_str.lower().replace('wib', '').replace('wita', '').replace('wit', '').replace(',', '').strip()
            
            # GReplace month name to number
            for nama, angka in bulan_indo.items():
                if nama in clean_str:
                    clean_str = clean_str.replace(nama, angka)
                    break
            parts = clean_str.split()
            if len(parts) >= 3:
                day = int(parts[0])
                month = int(parts[1])
                year = int(parts[2])
                hour = 0
                minute = 0
                
                if len(parts) > 3 and ':' in parts[3]:
                    time_parts = parts[3].split(':')
                    hour = int(time_parts[0])
                    minute = int(time_parts[1])
                return datetime(year, month, day, hour, minute).isoformat()
        except:
            pass
        # If failed, return to NOW()
        return datetime.now().isoformat()

    df['published_at'] = df['raw_date'].apply(smart_parse_date)
    
    cleaned_data = df.to_dict('records')
    context['ti'].xcom_push(key='cleaned_data', value=cleaned_data)

def load_data(**context):
    """Load Data to SQLite"""
    cleaned_data = context['ti'].xcom_pull(key='cleaned_data', task_ids='transform_task')
    if not cleaned_data: return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    inserted = 0
    for row in cleaned_data:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO news_articles (source, title, url, published_at, scraped_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['source'], row['title'], row['url'], row['published_at'], row['scraped_at']))
            if cursor.rowcount > 0: inserted += 1
        except: pass

    conn.commit()
    conn.close()
    logger.info(f"Loaded {inserted} new articles.")

# --- DAG ---
default_args = {
    'owner': 'haikal',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 9),
    'retries': 0,
}

with DAG('news_etl_pipeline_v1', default_args=default_args, schedule='0 7 * * *', catchup=False) as dag:
    t1 = PythonOperator(task_id='init_db_task', python_callable=create_table_if_not_exists)
    t2 = PythonOperator(task_id='extract_task', python_callable=extract_news, provide_context=True)
    t3 = PythonOperator(task_id='transform_task', python_callable=transform_data, provide_context=True)
    t4 = PythonOperator(task_id='load_task', python_callable=load_data, provide_context=True)

    t1 >> t2 >> t3 >> t4