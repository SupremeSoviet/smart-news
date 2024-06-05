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
from dotenv import load_dotenv

load_dotenv()

class NewsParsing:
    def __init__(self, base_url):
        self.base_url = base_url
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST')
        self.clickhouse_user = os.getenv('CLICKHOUSE_USER')
        self.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD')
        self.clickhouse_port = os.getenv('CLICKHOUSE_PORT')
        self.cert_path = os.getenv('CLICKHOUSE_CERT_PATH')

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
        response.raise_for_status()
        return response.json()

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
                date_match = re.search(r'cdate=(\d{1,2})\.5\.2024', url)
                date_str = date_match.group(1) if date_match else None
                center_part = soup.find('div', class_='center_part')
                if center_part and date_str:
                    list_items = center_part.find_all('li')
                    filtered_urls = [
                        ('https://www.tadviser.ru' + unquote(link.find('a')['href']), f'{date_str}.05.2024') for link in
                        list_items if link.find('a')['href'].startswith('/index.php/')]
            elif 'interfax' in self.base_url:
                filtered_urls = [('https://www.interfax.ru' + link['href'], None) for link in links if
                                 link['href'].startswith('/digital/9') or link['href'].startswith(
                                     '/business/9') or link in links if
                                 link['href'].startswith('/russia/9') or link['href'].startswith('/world/9')]
            elif 'theverge' in self.base_url:
                filtered_urls = [('https://www.theverge.com' + link['href'], None) for link in links if
                                 link['href'].startswith('/2024/')]
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
        # existing_urls = self.execute_query(existing_urls_query)
        # existing_urls = {url[0] for url in existing_urls}

        new_records = [record for record in records] #if record['url'] not in existing_urls]

        if new_records:
            insert_query = 'INSERT INTO news_bd (source, url, title, time, keywords, text) VALUES '
            values_list = [
                f"('{record['source']}', '{record['url']}', '{record['title']}', '{record['time']}', '{record['keywords']}', '{record['text']}')"
                for record in new_records
            ]
            full_query = insert_query + ", ".join(values_list)
            self.execute_query(full_query)


def fetch_all_links(base_url, start, end, step=1):
    links = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        if 'cnews' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}/page_{i}") for i in range(start, end, step)]
        elif 'habr' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}/page{i}/") for i in
                       range(start, end, step)]
        elif 'tadviser' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}{i}.5.2024") for i in
                       range(start, end, step)]
        elif 'interfax' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}{month}/{day}/all/page_{page}")
                       for month, day, page in itertools.product(range(start, end), range(1, 32), range(1, 3))]
        elif 'theverge' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}{i}") for i in
                       range(start, end, step)]
        for future in futures:
            links.extend(future.result())
    return set(links)


# Example usage
cnews_url = 'https://www.cnews.ru/archive/type_top_lenta_articles'
links_1 = fetch_all_links(cnews_url, 1, 51)
cnews_parser_1 = NewsParsing(cnews_url)
news_df_1 = cnews_parser_1.parse_news(links_1)

habr_url = 'https://habr.com/ru/news'
links_2 = fetch_all_links(habr_url, 1, 51)
cnews_parser_2 = NewsParsing(habr_url)
news_df_2 = cnews_parser_2.parse_news(links_2)

tadviser_url = 'https://www.tadviser.ru/index.php/Архив_новостей?cdate='
links_3 = fetch_all_links(tadviser_url, 16, 32)
cnews_parser_3 = NewsParsing(tadviser_url)
news_df_3 = cnews_parser_3.parse_news(links_3)

interfax_url = 'https://www.interfax.ru/news/2024/'
links_4 = fetch_all_links(interfax_url, 5, 7)
cnews_parser_4 = NewsParsing(interfax_url)
news_df_4 = cnews_parser_4.parse_news(links_4)