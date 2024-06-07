import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import unquote
from datetime import datetime
import re
import json
import itertools
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import hashlib

load_dotenv()

class NewsParsing:
    def __init__(self, base_url):
        self.base_url = base_url
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST')
        self.clickhouse_user = os.getenv('CLICKHOUSE_USER')
        self.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD')
        self.clickhouse_port = os.getenv('CLICKHOUSE_PORT')
        self.cert_path = os.getenv('CLICKHOUSE_CERT_PATH')
        self.db_name = os.getenv('CLICKHOUSE_DB_NAME')
        self.table_name = os.getenv('CLICKHOUSE_TABLE_NAME')

        print(self.clickhouse_host, self.clickhouse_user, self.clickhouse_password, self.clickhouse_port, self.cert_path)

        
        try:
            response = self.execute_query('SELECT version()')
            print(f"Connection to ClickHouse established successfully: {response}")
        except Exception as e:
            print(f"Failed to connect to ClickHouse: {e}")

    def execute_query(self, query):
        response = requests.get(
            f'https://{self.clickhouse_host}:{self.clickhouse_port}',
            params={
                'query': query,
            },
            headers={
                'X-ClickHouse-User': f'{self.clickhouse_user}',
                'X-ClickHouse-Key': f'{self.clickhouse_password}',
            },
            verify=f'{self.cert_path}'
        )
        return response.text

    def insert_dataframe(self, dataframe):
        unique_urls = dataframe['url']
        filtered_dataframe = dataframe.copy()

        for url in unique_urls:
            check_query = f"SELECT count() FROM {self.db_name}.{self.table_name} WHERE url = '{url}'"
            result = self.execute_query(check_query)

            if int(result.strip()) > 0:
                print(f"Record with URL {url} already exists. Removing from dataframe.")
                filtered_dataframe = filtered_dataframe[filtered_dataframe['url'] != url]

        if not filtered_dataframe.empty:
            first_url = filtered_dataframe.iloc[0]['url']
            hash_object = hashlib.md5(first_url.encode())
            file_name = f'{hash_object.hexdigest()}.csv'

            filtered_dataframe.to_csv(file_name, index=False, header=False)

            try:
                with open(file_name, 'rb') as f:
                    csv_data = f.read()

                query = f'INSERT INTO {self.db_name}.{self.table_name} FORMAT CSV'
                response = requests.post(
                    f'https://{self.clickhouse_host}:{self.clickhouse_port}',
                    params={'query': query},
                    headers={
                        'X-ClickHouse-User': self.clickhouse_user,
                        'X-ClickHouse-Key': self.clickhouse_password,
                    },
                    data=csv_data,
                    verify=self.cert_path
                )
                return response.text
            finally:
                if os.path.exists(file_name):
                    os.remove(file_name)
        else:
            return "No new records to insert."

    def link_parsing(self, url):
        filtered_urls = []
        response = requests.get(url)

        if response.status_code == 200:
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')
            links = soup.find_all('a', href=True)
            if 'cnews' in self.base_url:
                filtered_urls = [(link['href'], None) for link in links if link['href'].startswith('http://www.cnews.ru/news')]
            elif 'habr' in self.base_url:
                filtered_urls = [('https://habr.com' + link['href'], None) for link in links if
                                 link['href'].startswith('/ru/news/') and not 'page' in link[
                                     'href'] and not 'comment' in link['href'] and not link['href'].endswith(
                                     '/ru/news/')]
            elif 'tadviser' in self.base_url:
                date_match = re.search(r'(\d{1,2})\.\d{1,2}\.\d{4}', url)
                date_str = date_match.group(0) if date_match else None
                center_part = soup.find('div', class_='center_part')
                if center_part and date_str:
                    list_items = center_part.find_all('li')
                    filtered_urls = [
                        ('https://www.tadviser.ru' + unquote(link.find('a')['href']), date_str) for link in
                        list_items if link.find('a')['href'].startswith('/index.php/')]
            elif 'interfax' in self.base_url:
                filtered_urls = [('https://www.interfax.ru' + link['href'], None) for link in links if
                                 link['href'].startswith('/digital/9') or link['href'].startswith(
                                     '/business/9') or link in links if
                                 link['href'].startswith('/russia/9') or link['href'].startswith('/world/9')]
            elif 'metalinfo' in self.base_url:
                filtered_urls = [('https://www.metalinfo.ru' + link['href'], None) for link in links if 
                                 re.match(r'^/ru/news/\d+', link['href'])]
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

            elif 'habr' in self.base_url:
                title = soup.find('meta', property='og:title')['content'] if soup.find('meta',
                                                                                       property='og:title') else 'No title'
                article_block = soup.find('div', class_='tm-article-body')
                time_published = soup.find('meta', property='aiturec:datetime')['content'] if soup.find('meta',
                                                                                                        property='aiturec:datetime') else None
                date_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
                match = date_pattern.search(time_published)
                if match:
                    year = match.group(1)
                    month = match.group(2)
                    day = match.group(3)
                    time_published = f"{day}.{month}.{year}"
                source = 'habr'

            elif 'tadviser' in self.base_url:
                title = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'No title'
                article_block = soup.find('div', class_='js-mediator-article')
                time_published = date
                source = 'tadviser'

            elif 'interfax' in self.base_url:
                title = soup.find('meta', property='og:title')['content'] if soup.find('meta',
                                                                                       property='og:title') else 'No title'
                article_block = soup.find('article', itemprop='articleBody')
                time_published = soup.find('meta', property="article:published_time")['content'] if soup.find('meta',
                                                                                                              property="article:published_time") else None
                date_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
                match = date_pattern.search(time_published)
                if match:
                    year = match.group(1)
                    month = match.group(2)
                    day = match.group(3)
                    time_published = f"{day}.{month}.{year}"
                source = 'interfax'
                link = soup.find('link', rel='canonical').get('href')

            elif 'metalinfo' in self.base_url:
                title = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'No title'
                article_block = soup.find('div', class_='news-body')
                time_published = soup.find('meta', itemprop="datePublished")['content'] if soup.find('meta', itemprop="datePublished") else None
                date_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
                match = date_pattern.search(time_published)
                if match:
                    year = match.group(1)
                    month = match.group(2)
                    day = match.group(3)
                    time_published = f"{day}.{month}.{year}"
                source = 'metalinfo'

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
            text = re.sub(r'[\u2000-\u200F]', ' ', text)
            text = text.replace('\xa0',' ')
            title = title.replace('\xa0',' ')
            keywords = keywords.replace('\xa0',' ')
            return [source, link, title, time_published, keywords, text]
        except Exception as e:
            return None

    def parse_news(self, links):
        news_data = []
        k = 1

        if len(links) > 10:
            links = set(list(links)[:35])

        for link in links:
            result = self.fetch_news(*link)
            if result:
                k += 1
                news_data.append(result)

        df = pd.DataFrame(news_data, columns=['source', 'url', 'title', 'time', 'keywords', 'text'])
        self.insert_dataframe(df)
        return df

def fetch_all_links(base_url, start, end, step=1):
    links = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        if 'cnews' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}/page_{i}") for i in range(start, end, step)]
        elif 'habr' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}/page{i}/") for i in range(start, end, step)]
        elif 'tadviser' in base_url:
            today = datetime.now()
            dates = [(today - timedelta(days=i)).strftime("%-d.%-m.%Y") for i in range(start, end, step)]
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}{date}") for date in dates]
        elif 'interfax' in base_url:
            today = datetime.now()
            date_combinations = [(today - timedelta(days=i)).strftime("%m/%d") for i in range(start, end, step)]
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}{date}/all/page_{page}") 
                       for date, page in itertools.product(date_combinations, range(1, 3))]
        elif 'metalinfo' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}list.html?pn={i}") for i in range(start, end, step)]
        for future in futures:
            links.extend(future.result())
    return set(links)

# Период для анализа: последние 5 дней
days_to_analyze = 5

current_day = datetime.now().day
current_month = datetime.now().month
current_year = datetime.now().year

cnews_url = 'https://www.cnews.ru/archive/type_top_lenta_articles'
links_1 = fetch_all_links(cnews_url, 1, 11)
cnews_parser_1 = NewsParsing(cnews_url)
news_df_1 = cnews_parser_1.parse_news(links_1)

habr_url = 'https://habr.com/ru/news'
links_2 = fetch_all_links(habr_url, 1, 11)
cnews_parser_2 = NewsParsing(habr_url)
news_df_2 = cnews_parser_2.parse_news(links_2)

tadviser_url = 'https://www.tadviser.ru/index.php/Архив_новостей?cdate='
links_3 = fetch_all_links(tadviser_url, 0, days_to_analyze)
cnews_parser_3 = NewsParsing(tadviser_url)
news_df_3 = cnews_parser_3.parse_news(links_3)

interfax_url = 'https://www.interfax.ru/news/2024/'
links_4 = fetch_all_links(interfax_url, 0, days_to_analyze)
cnews_parser_4 = NewsParsing(interfax_url)
news_df_4 = cnews_parser_4.parse_news(links_4)

metalinfo_url = 'https://www.metalinfo.ru/ru/news/'
links_5 = fetch_all_links(metalinfo_url, 0, 11)
cnews_parser_5 = NewsParsing(metalinfo_url)
news_df_5 = cnews_parser_5.parse_news(links_5)
