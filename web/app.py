from flask import Flask, render_template, request, redirect, url_for, flash
import dotenv
import os
import hashlib
import pandas as pd
import re
import json
import requests
import numpy as np

dotenv.load_dotenv()

gpt_api_key = os.getenv('API_KEY')
FOLDER_ID = os.getenv('FOLDER_ID')
doc_uri = f"emb://{FOLDER_ID}/text-search-doc/latest"
yagpt3_uri = f'cls://{FOLDER_ID}/yandexgpt/latest'
embed_url = "https://llm.api.cloud.yandex.net:443/foundationModels/v1/textEmbedding"
completion_url = 'https://llm.api.cloud.yandex.net/foundationModels/v1/completion'
headers = {"Content-Type": "application/json", "Authorization": f"Api-Key {gpt_api_key}", "x-folder-id": f"{FOLDER_ID}"}

def get_labels(text: str) -> np.array:

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

    response = requests.post(completion_url, json=query_data, headers=headers)
    try:
        result_dict = json.loads(response.json()['result']['alternatives'][0]['message']['text'])
        return np.array([i for i in result_dict.keys() if (result_dict[i] and i in tags)])
    except Exception as ex:
        print('labels ex', ex)
        return np.array([])

def get_embedding(text: str) -> np.array:
    query_data = {
        "modelUri": doc_uri,
        "text": text,
    }

    try:
        return np.array(
        requests.post(embed_url, json=query_data, headers=headers).json()["embedding"]
        )
    except Exception as ex:
        print('embedding ex', ex)
        return np.array([])


app = Flask(__name__)
key = os.getenv("FLASK_KEY")
app.secret_key = key

AUTH_KEY_HASH = os.getenv("AUTH_KEY_HASH")

@app.route('/')
def auth():
    return render_template('auth.html')


@app.route('/login', methods=['POST'])
def login():
    key = request.form.get('auth_key')
    if hashlib.sha256(key.encode('utf-8')).hexdigest() == AUTH_KEY_HASH:
        flash('Авторизация прошла успешна!', 'success')
        return redirect(url_for('dashboard'))
    else:
        flash('Неверный ключ!', 'error')
        return redirect(url_for('auth'))


@app.route('/dashboard')
def dashboard():
    description = request.args.get('description', '')
    return render_template('dashboard.html', description=description)


@app.route('/upload', methods=['POST'])
def upload():
    description = request.form.get('description')
    if 'file' not in request.files:
        flash('Ошибка загрузки', 'error')
        return redirect(url_for('dashboard', description=description))

    file = request.files['file']
    if file.filename == '':
        flash('Нет выбранного файла', 'error')
        return redirect(url_for('dashboard', description=description))

    if file and file.filename.endswith('.txt'):
        filename = os.path.join('uploads', file.filename)
        file.save(filename)
        is_correct, ans = check_accs(filename)
        if is_correct:
            flash('Успешно загружено', 'success')
            mails = ans
        else:
            flash('Ошибка загрузки: ' + ans, 'error')
    else:
        flash('Неверный формат файла. Загрузите .txt или .xlsx файл', 'error')

    return redirect(url_for('dashboard', description=description))


@app.route('/save_description', methods=['POST'])
def save_description():
    description = request.form.get('description')
    if description:
        with open('description.txt', 'w') as file:
            file.write(description)
        flash('Успешно', 'success')
    else:
        flash('Введите описание', 'error')
    return redirect(url_for('dashboard', description=description))


@app.route('/help')
def help():
    help_message = """
    Чтобы использовать этот сервис:
    1. Введите ключ авторизации для доступа к панели управления.
    2. В панели управления введите описание новостей.
    3. Загрузите список получателей в текстовом формате.
    4. Сохраните описание новостей.
    """
    flash(help_message, 'info')
    return redirect(url_for('dashboard'))


def check_accs(filename):
    if not os.path.isfile(filename):
        return False, "Файл не существует"

    file_extension = os.path.splitext(filename)[1]

    if file_extension not in ['.txt', '.xlsx']:
        return False, "Расширение файла не поддерживается"

    try:
        emails = []
        if file_extension == '.txt':
            with open(filename, 'r') as file:
                emails = [line.strip() for line in file]
        elif file_extension == '.xlsx':
            df = pd.read_excel(filename)
            emails = df[df.columns[0]].dropna().astype(str).tolist()

        email_regex = re.compile(
            r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)'
        )

        invalid_emails = [email for email in emails if not email_regex.match(email)]

        if invalid_emails:
            return False, f"Неверный формат почты: {invalid_emails}"

        return True, emails
    except Exception as e:
        return False, f"Ошибка чтения файла"


if __name__ == '__main__':
    if not os.path.exists('uploads'):
        os.makedirs('uploads')
    app.run(debug=True)
