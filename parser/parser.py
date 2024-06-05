import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote
from datetime import datetime
import re
import json
import itertools
import os
from clickhouse_driver import Client

class NewsParsing:
    def __init__(self, base_url):
        self.base_url = base_url
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST')
        self.clickhouse_user = os.getenv('CLICKHOUSE_USER')
        self.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD')
        self.clickhouse_port = os.getenv('CLICKHOUSE_PORT', 9000)
        self.client = Client(host=self.clickhouse_host, user=self.clickhouse_user, password=self.clickhouse_password, port=self.clickhouse_port)

    def link_parsing(self, url):
        filtered_urls = []
        response = requests.get(url)

        if response.status_code == 200:
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')
            links = soup.find_all('a', href=True)
            if 'cnews' in self.base_url:
                filtered_urls = [(link['href'], None) for link in links if link['href'].startswith('http://www.cnews.ru/news')]
            else:
                filtered_urls = []
            filtered_urls = list(set(filtered_urls))

            return filtered_urls
        else:
            return []

    def fetch_news(self, link, date):
        response = requests.get(link)
        if response.status_code != 200:
            return None

        try:
            response.encoding = response.apparent_encoding
            soup = BeautifulSoup(response.content, 'html.parser')
            title = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'No title'

            if 'cnews' in self.base_url:
                title = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'No title'
                article_block = soup.find(class_='news_container')
                date_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
                match = date_pattern.search(link)

                if match:
                    year = match.group(1)
                    month = match.group(2)
                    day = match.group(3)
                    time_published = f"{day}.{month}.{year}"
                source = 'cnews'

            if not article_block:
                return None
            text = ''
            paragraphs = article_block.find_all('p')
            for paragraph in paragraphs:
                if len(text) > 5000:
                    text = text[:5000]
                    break
                paragraph_text = paragraph.get_text(strip=True) if not paragraph.find('a') else ' '.join([text for text in paragraph.stripped_strings])
                text += ' ' + paragraph_text
            keywords = soup.find('meta', attrs={'name': 'keywords'}).get('content') if soup.find('meta', attrs={'name': 'keywords'}) else ''

            if 'Å' in text or 'æ' in text or 'µ' in text:
                return None
            text = re.sub(r'Москва\.\s.*?INTERFAX\.RU\s-\s', '', text)
            return [source, link, title, time_published, keywords, text]
        except Exception as e:
            return None

    def parse_news(self, links):
        news_data = []
        k = 1
        with ThreadPoolExecutor(max_workers=20) as executor:
            results = executor.map(lambda x: self.fetch_news(*x), links)
            for result in results:
                if result:
                    k+=1
                    news_data.append(result)

        df = pd.DataFrame(news_data, columns=['source', 'url', 'title', 'time', 'keywords', 'text'])
        self.save_to_clickhouse(df)
        return df

    def save_to_clickhouse(self, df):
        records = df.to_dict('records')

        create_table_query = '''
            CREATE TABLE IF NOT EXISTS news_bd (
                source String,
                url String,
                title String,
                time String,
                keywords String,
                text String
            ) ENGINE = MergeTree()
            ORDER BY (source, url)
        '''
        self.execute_query(create_table_query)

        existing_urls_query = 'SELECT url FROM news_bd'
        existing_urls = self.execute_query(existing_urls_query)
        existing_urls = {url[0] for url in existing_urls}

        new_records = [record for record in records if record['url'] not in existing_urls]

        if new_records:
            insert_query = 'INSERT INTO news_bd (source, url, title, time, keywords, text) VALUES'
            self.client.execute(insert_query, new_records)

    def execute_query(self, query, data=None):
        try:
            if data:
                return self.client.execute(query, data)
            else:
                return self.client.execute(query)
        except Exception as e:
            return None


def fetch_all_links(base_url, start, end, step=1):
    links = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        if 'cnews' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}/page_{i}") for i in range(start, end, step)]
    return set(links)

cnews_url = 'https://www.cnews.ru/archive/type_top_lenta_articles'
links_1 = fetch_all_links(cnews_url, 1, 51)
cnews_parser_1 = NewsParsing(cnews_url)
news_df_1 = cnews_parser_1.parse_news(links_1)
