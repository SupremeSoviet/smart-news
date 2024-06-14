
import os
import re
import ssl
import json
import time
import hashlib
import certifi
import requests
import itertools
import numpy as np
import pandas as pd

from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from datetime import timedelta
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

folder_id = os.getenv('FOLDER_ID')
API_KEY = os.getenv('API_KEY')
FOLDER_ID = os.getenv('FOLDER_ID')
doc_uri = f"emb://{FOLDER_ID}/text-search-doc/latest"
yagpt3_uri = f'gpt://{FOLDER_ID}/yandexgpt/latest'
cls_url = "https://llm.api.cloud.yandex.net/foundationModels/v1/fewShotTextClassification"
embed_url = "https://llm.api.cloud.yandex.net:443/foundationModels/v1/textEmbedding"
headers = {"Authorization": f"Api-Key {API_KEY}", "x-folder-id": f"{FOLDER_ID}"}


def download_certificate(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as file:
        file.write(response.content)


def install_certificates(cert_dir):
    os.makedirs(cert_dir, exist_ok=True)

    root_ca_url = "https://storage.yandexcloud.net/cloud-certs/RootCA.pem"
    intermediate_ca_url = "https://storage.yandexcloud.net/cloud-certs/IntermediateCA.pem"

    root_ca_path = os.path.join(cert_dir, "RootCA.pem")
    intermediate_ca_path = os.path.join(cert_dir, "IntermediateCA.pem")

    download_certificate(root_ca_url, root_ca_path)
    download_certificate(intermediate_ca_url, intermediate_ca_path)

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    ssl_context.load_verify_locations(root_ca_path)
    ssl_context.load_verify_locations(intermediate_ca_path)

    return ssl_context

def translate(txt: str):
    body = {
        "targetLanguageCode": 'ru',
        "texts": [txt],
        "folderId": folder_id,
        "sourceLanguageCode": "en"
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Api-key {0}".format(API_KEY)
    }

    response = requests.post('https://translate.api.cloud.yandex.net/translate/v2/translate',
                             json=body,
                             headers=headers
                             )

    return response.json()['translations'][0]['text']


cert_dir = 'certs'
install_certificates(cert_dir)


def get_embedding(text: str) -> np.array:
    query_data = {
        "modelUri": doc_uri,
        "text": text,
    }

    try:
        response = requests.post(embed_url, json=query_data, headers=headers)
        if 'error' in dict(response.json()).keys():
            print(response.json()['error'])
            time.sleep(1)
        else:
            return np.array(response.json()['embedding'])

        return np.array(
        requests.post(embed_url, json=query_data, headers=headers).json()["embedding"]
        )

    except Exception as ex:
        print('embedding ex', ex)
        return np.array([])

def get_labels(text: str) -> np.array:
    print('Новый запрос')
    tags = ['Технологии',
            'Инновации',
            'Innovations',
            'Trends',
            'Цифровизация',
            'Автоматизация',
            'Цифровая трансформация',
            'Digital solutions',
            'Цифровые двойники',
            'Digital twins',
            'ИИ',
            'AI',
            'IoT',
            'Интернет вещей',
            'Big Data',
            'Блокчейн',
            'Process mining',
            'Облачные технологии',
            'Квантовые вычисления',
            'Смарт - контракты',
            'Робототехника',
            'VR / AR / MR',
            'Виртуальная и дополненная реальность',
            'Генеративный',
            'Распознавание',
            'Искусственный интеллект',
            'Машинное обучение',
            'Глубокое обучение',
            'Нейронные сети',
            'Компьютерное зрение',
            'Обработка естественного языка(NLP)',
            'Reinforcement Learning',
            'Low - code',
            'No - code',
            'Металлургический(ая)',
            'Сталь',
            'Steel',
            'LLM',
            'ML',
            'ChatGPT',
            'IT',
            'Кибербезопасность',
            'Стартапы',
            'Startups',
            'YandexGPT',
            'LLAMA',
            'GPT(GPT - 3, GPT - 4)',
            'BERT',
            'OpenAI',
            'DALL-E',
            'Transformer models',
            'Generative Adversarial Networks(GAN)',
            'DeepFake',
            'Машинное зрение',
            'Text - to - Image',
            'Voice - to - text',
            'Визуализация данных',
            'Управление цепочками поставок',
            'Снабжение',
            'Технологии 5G',
            'Суперкомпьютеры',
            'DevOps',
            'ФинТех',
            'Token',
            'Токен',
            'Микросервисы',
            'Kubernetes',
            'API',
            'Цифровой след',
            'Цифровая идентификация',
            'Интеллектуальный анализ данных',
            'Продвинутая аналитика',
            'Северсталь',
            'Евраз',
            'ММК',
            'ОМК',
            'Nippon',
            'steel', ]

    query_data = {
        "modelUri": yagpt3_uri,
        "completionOptions": {
            "stream": False,
            "temperature": 0,
        },
        "messages": [
            {
            'role': 'system',
            'text': f"""
            Ты отвечаешь в формате json, расставь тегам значения 0 или 1 в зависимости от приведенного текста. 
            Вот теги:
                {tags}
            """
            },
            {
            'role': 'user',
            'text': text,
            },
        ]
    }

    response = requests.post('https://llm.api.cloud.yandex.net/foundationModels/v1/completion', json=query_data, headers=headers)
    print('Первичный запрос')
    try:
        while 'error' in dict(response.json()):
            time.sleep(1)
            response = requests.post('https://llm.api.cloud.yandex.net/foundationModels/v1/completion', json=query_data,
                                     headers=headers)

        result_dict = json.loads(response.json()['result']['alternatives'][0]['message']['text'])
        return np.array([i for i in result_dict.keys() if (result_dict[i] and i in tags)])

    except Exception as ex:
        print('labels ex', ex)
        return np.array([])


class NewsParsing:
    def __init__(self, base_url):
        self.base_url = base_url
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST')
        self.clickhouse_user = os.getenv('CLICKHOUSE_USER')
        self.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD')
        self.clickhouse_port = os.getenv('CLICKHOUSE_PORT')
        self.cert_path = cert_dir + '/RootCA.pem'
        self.db_name = os.getenv('CLICKHOUSE_DB_NAME')
        self.table_name = os.getenv('CLICKHOUSE_TABLE_NAME')

        try:
            response = self.execute_query('SELECT version()')
            print(f"Connection to ClickHouse established successfully: {response}")
        except Exception as e:
            pass
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

    def insert_dataframe(self, dataframe, source):
        print(f'start inserting news from {source}')
        unique_urls = dataframe['url']
        filtered_dataframe = dataframe.copy()

        print(unique_urls[:10 if len(unique_urls) > 10 else len(unique_urls)])

        for url in unique_urls:
            self.execute_query('SELECT 1')

            check_query = f"SELECT count() FROM {self.db_name}.{self.table_name} WHERE url = '{url}'"
            result = self.execute_query(check_query)

            if int(result.strip()) > 0:
                print(f"Record with URL {url} already exists. Removing from dataframe.")
                filtered_dataframe = filtered_dataframe[filtered_dataframe['url'] != url]

        print('checked unique urls')

        if not filtered_dataframe.empty:

            text = filtered_dataframe['text']
            filtered_dataframe['embedding'] = filtered_dataframe['text'].apply(get_embedding)
            # filtered_dataframe['tags'] = filtered_dataframe['text'].apply(get_labels)

            filtered_dataframe['embedding'] = filtered_dataframe['embedding'].apply(
                lambda emb: "[" + ",".join(map(str, emb)) + "]")
            # filtered_dataframe['tags'] = filtered_dataframe['tags'].apply(
            #     lambda tags: "[" + ",".join(f"'{tag}'" for tag in tags) + "]")

            print('start')

            first_url = filtered_dataframe.iloc[0]['url']
            hash_object = hashlib.md5(first_url.encode())
            file_name = f'{hash_object.hexdigest()}.csv'

            filtered_dataframe.to_csv(file_name, index=False, header=False)

            try:
                with open(file_name, 'rb') as f:
                    csv_data = f.read()
                print('inserting')
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
                print(f'inserting news from {source} finished {response.text}')
                return response.text
            except Exception as e:
                print('inserting ex: ', e)
            finally:
                pass
                # if os.path.exists(file_name):
                #     os.remove(file_name)
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
                filtered_urls = [(link['href'], None) for link in links if
                                 link['href'].startswith('http://www.cnews.ru/news')]
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
            elif 'theverge' in self.base_url:
                filtered_urls = [('https://www.theverge.com' + link['href'], None) for link in links if
                                 link['href'].startswith('/2024/') and not 'Comments' in link['href']]
            elif 'technode' in self.base_url:
                filtered_urls = [(link['href'], None) for link in links if
                                 re.match(r'^https://technode.com/\d{4}/\d{2}/\d{2}/', link['href'])]
            elif 'techcrunch' in self.base_url:
                filtered_urls = [(link['href'], None) for link in links if
                                 re.match(r'^https://techcrunch.com/\d{4}/\d{2}/\d{2}/', link['href'])]
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
                time_published = soup.find('meta', itemprop="datePublished")['content'] if soup.find('meta',
                                                                                                     itemprop="datePublished") else None
                date_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
                match = date_pattern.search(time_published)
                if match:
                    year = match.group(1)
                    month = match.group(2)
                    day = match.group(3)
                    time_published = f"{day}.{month}.{year}"
                source = 'metalinfo'

            elif 'theverge' in self.base_url:
                title = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'No title'
                script_tag = soup.find('script', type='application/ld+json')
                if script_tag:
                    json_data = json.loads(script_tag.string)
                    text = json_data.get('articleBody', '')
                else:
                    text = ''
                time_published = soup.find('meta', property='article:published_time')['content'] if soup.find('meta',
                                                                                                              property='article:published_time') else None
                date_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
                match = date_pattern.search(time_published)
                if match:
                    year = match.group(1)
                    month = match.group(2)
                    day = match.group(3)
                    time_published = f"{day}.{month}.{year}"
                source = 'theverge'
                text = re.sub(r'[\u2000-\u200F]', ' ', text)
                text = text.replace('\xa0', ' ').replace('\n', ' ')
                title = title.replace('\xa0', ' ').replace('| TechCrunch', '').replace('\n', ' ')
                if not 'No title' in title:
                    text = re.sub(r'\[.*?\]', '', text)
                    # text = translate(text)
                    # title = translate(title)
                    print(link, title)
                    return [source, link, title, time_published, None, text]
                else:
                    print("Фигня", link, title)
                    return None

            elif 'technode' in self.base_url:
                title = soup.find('meta', property='og:title')['content'] if soup.find('meta',
                                                                                       property='og:title') else 'No title'
                article_block = soup.find('div', class_='entry-content')
                time_published = soup.find('meta', property="article:modified_time")['content'] if soup.find('meta',
                                                                                                             property="article:modified_time") else None
                date_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
                match = date_pattern.search(time_published)
                if match:
                    year = match.group(1)
                    month = match.group(2)
                    day = match.group(3)
                    time_published = f"{day}.{month}.{year}"
                source = 'technode'

            elif 'techcrunch' in self.base_url:
                title = soup.find('meta', property='og:title')['content'] if soup.find('meta',
                                                                                       property='og:title') else 'No title'
                article_block = soup.find('div',
                                          class_='entry-content wp-block-post-content is-layout-flow wp-block-post-content-is-layout-flow')
                time_published = soup.find('meta', property="article:published_time")['content'] if soup.find('meta',
                                                                                                              property="article:published_time") else None
                date_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
                match = date_pattern.search(time_published)
                if match:
                    year = match.group(1)
                    month = match.group(2)
                    day = match.group(3)
                    time_published = f"{day}.{month}.{year}"
                source = 'techcrunch'

            if not article_block:
                return None
            text = ''
            paragraphs = article_block.find_all('p')
            for paragraph in paragraphs:
                paragraph_text = paragraph.get_text(strip=True) if not paragraph.find('a') else ' '.join(
                    [text for text in paragraph.stripped_strings])
                text += ' ' + paragraph_text
            keywords = soup.find('meta', attrs={'name': 'keywords'}).get('content') if soup.find('meta', attrs={
                'name': 'keywords'}) else ''

            if 'Å' in text or 'æ' in text or 'µ' in text:
                return None
            text = re.sub(r'Москва\.\s.*?INTERFAX\.RU\s-\s', '', text)
            text = re.sub(r'[\u2000-\u200F]', ' ', text)
            text = text.replace('\xa0', ' ')
            title = title.replace('\xa0', ' ')
            keywords = keywords.replace('\xa0', ' ')
            if 'techcrunch' in self.base_url or 'technode' in self.base_url:
                # text = translate(text)
                # title = translate(title)
                # keywords = translate(keywords)
                pass
            return [source, link, title, time_published, keywords, text]
        except Exception as e:
            return None

    def parse_news(self, links):
        news_data = []
        k = 1

        for link in links:
            result = self.fetch_news(*link)
            if result:
                k += 1
                news_data.append(result)

        df = pd.DataFrame(news_data, columns=['source', 'url', 'title', 'time', 'keywords', 'text'])
        df.to_csv('test.csv', index=False)
        self.insert_dataframe(df, df.iloc[0, 0])
        return df


def fetch_all_links(base_url, start, end, step=1):
    links = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        if 'cnews' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}/page_{i}") for i in
                       range(start, end, step)]
        elif 'habr' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}/page{i}/") for i in
                       range(start, end, step)]
        elif 'tadviser' in base_url:
            today = datetime.now()
            dates = [(today - timedelta(days=i)).strftime("%-d.%-m.%Y") for i in range(start, end, step)]
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}{date}") for date in dates]
        elif 'interfax' in base_url:
            today = datetime.now()
            date_combinations = [(today - timedelta(days=i)).strftime("%m/%d") for i in range(start, end, step)]
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}{date}/all/page_{page}") for
                       date, page in itertools.product(date_combinations, range(1, 3))]
        elif 'metalinfo' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}list.html?pn={i}") for i in
                       range(start, end, step)]
        elif 'theverge' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}{i}") for i in
                       range(start, end, step)]
        elif 'technode' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}page/{i}/") for i in
                       range(start, end, step)]
        elif 'techcrunch' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}/page/{i}/") for i in
                       range(start, end, step)]
        for future in futures:
            links.extend(future.result())
    return set(links)


days_to_analyze = 5

current_day = datetime.now().day
current_month = datetime.now().month
current_year = datetime.now().year

cnews_url = 'https://www.cnews.ru/archive/type_top_lenta_articles'
links_1 = fetch_all_links(cnews_url, 1, 5)
news_parser_1 = NewsParsing(cnews_url)
news_df_1 = news_parser_1.parse_news(links_1)

habr_url = 'https://habr.com/ru/news'
links_2 = fetch_all_links(habr_url, 1, 5)
news_parser_2 = NewsParsing(habr_url)
news_df_2 = news_parser_2.parse_news(links_2)

# tadviser_url = 'https://www.tadviser.ru/index.php/Архив_новостей?cdate='
# links_3 = fetch_all_links(tadviser_url, 0, days_to_analyze)
# news_parser_3 = NewsParsing(tadviser_url)
# news_df_3 = news_parser_3.parse_news(links_3)

interfax_url = 'https://www.interfax.ru/news/2024/'
links_4 = fetch_all_links(interfax_url, 0, days_to_analyze)
news_parser_4 = NewsParsing(interfax_url)
news_df_4 = news_parser_4.parse_news(links_4)

metalinfo_url = 'https://www.metalinfo.ru/ru/news/'
links_5 = fetch_all_links(metalinfo_url, 0, 5)
news_parser_5 = NewsParsing(metalinfo_url)
news_df_5 = news_parser_5.parse_news(links_5)

# theverge_url = 'https://www.theverge.com/archives/'
# links_6 = fetch_all_links(theverge_url, 1, 3)
# news_parser_6 = NewsParsing(theverge_url)
# news_df_6 = news_parser_6.parse_news(links_6)

# technode_url = 'https://technode.com/category/news-feed/'
# links_7 = fetch_all_links(technode_url, 1, 3)
# news_parser_7 = NewsParsing(technode_url)
# news_df_7 = news_parser_7.parse_news(links_7)

# techcrunch_url = 'https://techcrunch.com'
# links_8 = fetch_all_links(techcrunch_url, 1, 3)
# news_parser_8 = NewsParsing(techcrunch_url)
# news_df_8 = news_parser_8.parse_news(links_8)
