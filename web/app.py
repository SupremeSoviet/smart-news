from flask import Flask, render_template, request, redirect, url_for, flash
from flask_apscheduler import APScheduler
import dotenv
import hashlib
import ast
import re
import json
import time
import numpy as np
import concurrent.futures
import ssl
import os
import certifi
import requests
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import Image
from datetime import datetime
import base64

dotenv.load_dotenv()

pdf_path = 'news_digest.pdf'
img_path = 'photo.webp'
style_Arial_path = 'arialmt.ttf'
SMTP_SERVER = 'smtp.mail.ru'
SMTP_PORT = 587
SMTP_USERNAME = os.getenv('MAIL_USER')
SMTP_PASSWORD = os.getenv('MAIL_PWD')
Sent_to = []

print('START')

gpt_api_key = os.getenv('API_KEY')
FOLDER_ID = os.getenv('FOLDER_ID')
doc_uri = f"emb://{FOLDER_ID}/text-search-doc/latest"
yagpt3_uri = f'gpt://{FOLDER_ID}/yandexgpt/latest'
embed_url = "https://llm.api.cloud.yandex.net:443/foundationModels/v1/textEmbedding"
completion_url = 'https://llm.api.cloud.yandex.net/foundationModels/v1/completion'
headers = {"Content-Type": "application/json", "Authorization": f"Api-Key {gpt_api_key}", "x-folder-id": f"{FOLDER_ID}"}

cert_dir = 'certs'
clickhouse_host = os.getenv('CLICKHOUSE_HOST')
clickhouse_user = os.getenv('CLICKHOUSE_USER')
clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD')
clickhouse_port = os.getenv('CLICKHOUSE_PORT')
cert_path = cert_dir + '/RootCA.pem'
db_name = os.getenv('CLICKHOUSE_DB_NAME')
table_name = os.getenv('CLICKHOUSE_TABLE_NAME')

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

install_certificates(cert_dir)

def execute_query(query):
    response = requests.get(
        f'https://{clickhouse_host}:{clickhouse_port}',
        params={
            'query': query,
        },
        headers={
            'X-ClickHouse-User': f'{clickhouse_user}',
            'X-ClickHouse-Key': f'{clickhouse_password}',
        },
        verify=f'{cert_path}'
    )

    return response.text


def query_to_dataframe(result):
    # Split the result into lines
    data = [line.split('\t') for line in result.strip().split('\n')]
    # Extract headers and rows
    headers = ['source', 'url', 'title', 'time', 'text', 'vector', 'cosine']
    rows = data[:]
    # Create DataFrame
    df = pd.DataFrame(rows, columns=headers)
    return df[['source', 'url', 'title', 'time', 'text']]  # Select only required columns


def get_top_df(text):
    embedding = get_embedding(text)
    query = f"""
    SELECT
        source,
        url,
        title,
        time,
        text,
        embedding,
        1 - cosineDistance(embedding, [{",".join(map(str, embedding))}]) AS cosine_similarity
    FROM news_bd.news_articles
    WHERE length(embedding) = 256
    ORDER BY cosine_similarity DESC
    LIMIT 20
    """
    try:
        result = execute_query(query)
        df = query_to_dataframe(result)
        return df
    except Exception as ex:
        print('get top df ex: ', ex)


def get_summary(text: str):

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
            Ты пересказываешь новость в форамте дайджеста, сохраняя действующие лица, даты и факты. Без сложного форматирования, просто текст
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
        result = response.json()['result']['alternatives'][0]['message']['text']
        return result
    except Exception as ex:
        print('summary ex', ex)
        return text

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


def generate_image_yandex(prompt, path):
    prompt_data = {
        "modelUri": f"art://{FOLDER_ID}/yandex-art/latest",
        "generationOptions": {
            "seed": 17
        },
        "messages": [
            {
                "weight": 1,
                "text": prompt if len(prompt) < 500 else prompt[:500]
            }
        ]
    }

    headers = {
        "Authorization": f"Api-Key {gpt_api_key}",
        "Content-Type": "application/json"
    }

    response = requests.post(
        "https://llm.api.cloud.yandex.net/foundationModels/v1/imageGenerationAsync",
        headers=headers,
        json=prompt_data
    )

    if response.status_code != 200:
        raise Exception(f"Failed to start image generation: {response.status_code} {response.text}")

    operation_id = response.json()['id']

    # Ждем завершения операции
    while True:
        result_response = requests.get(
            f"https://llm.api.cloud.yandex.net/operations/{operation_id}",
            headers=headers
        )

        if result_response.status_code != 200:
            raise Exception(f"Failed to get operation status: {result_response.status_code} {result_response.text}")

        result_data = result_response.json()

        if result_data.get('done'):
            if 'response' in result_data:
                break
            elif 'error' in result_data:
                raise Exception(f"Error in operation: {result_data['error']}")
        else:
            time.sleep(1)

    image_base64 = result_response.json()['response']['image']

    image_data = base64.b64decode(image_base64)
    with open(f"{path}.jpeg", "wb") as file:
        file.write(image_data)

    print(f"Картинка успешно сохранена как {path}.jpeg")

app = Flask(__name__)
key = os.getenv("FLASK_KEY")
app.secret_key = key
app.config['UPLOAD_FOLDER'] = 'uploads'

if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

AUTH_KEY_HASH = os.getenv("AUTH_KEY_HASH")


scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

mailing_interval = 7


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
            Sent_to = ans
            print(Sent_to)
            path_to_mails = open('uploads/mails.txt', 'w')
            print(Sent_to, file=path_to_mails)
            path_to_mails.close()
        else:
            flash('Ошибка загрузки: ' + ans, 'error')
    else:
        flash('Неверный формат файла. Загрузите .txt или .xlsx файл', 'error')

    return redirect(url_for('dashboard', description=description))

@app.route('/save_description', methods=['POST'])
def save_description():
    description = request.form.get('description')
    if description:
        with open('uploads/description.txt', 'w') as file:
            file.write(description)
        flash('Успешно', 'success')
    else:
        flash('Введите описание', 'error')
    return redirect(url_for('dashboard', description=description))

@app.route('/set_interval', methods=['POST'])
def set_interval():
    global mailing_interval
    interval = request.form.get('interval')
    if interval and interval.isdigit():
        mailing_interval = int(interval)
        try:
            scheduler.remove_job('send_pdf_job')
        except:
            pass
        scheduler.add_job(id='send_pdf_job', func=send_pdf, trigger='interval', days=mailing_interval)
        flash('Интервал рассылки сохранен', 'success')
    else:
        flash('Введите корректный интервал', 'error')
    return redirect(url_for('dashboard'))

@app.route('/send_now', methods=['POST'])
def send_now():
    res = send_pdf()
    if res:
        flash('Рассылка отправлена', 'success')
    else:
        flash('Ошибка отправки', 'error')
    return redirect(url_for('dashboard'))

@app.route('/help')
def help():
    help_message = """
    Чтобы использовать этот сервис:
    1. Введите ключ авторизации для доступа к панели управления.
    2. В панели управления введите описание новостей.
    3. Загрузите список получателей в текстовом формате.
    4. Сохраните описание новостей.
    5. Установите интервал рассылки.
    6. Отправьте рассылку сразу или подождите, пока она будет отправлена автоматически.
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
        if emails:
            print(emails)
            return True, emails
        return False, f'Почты не найдены'
    except Exception as e:
        return False, f"Ошибка чтения файла"


def create_pdf(filename, data, style_Arial_path):
    doc = SimpleDocTemplate(filename, pagesize=letter)
    styles = getSampleStyleSheet()
    styleN = styles['Normal']
    styleH = styles['Heading1']
    styleJ = styles['BodyText']

    try:
        pdfmetrics.registerFont(TTFont('Arial', style_Arial_path))
    except Exception as e:
        print(f"Ошибка при загрузке шрифта: {e}")

    styleN.fontName = 'Arial'
    styleH.fontName = 'Arial'
    styleJ.fontName = 'Arial'

    styleH.fontSize = 20
    styleJ.fontSize = 12

    styleJ.alignment = 4

    story = []

    max_width = 3.25 * inch
    max_height = 4.5 * inch

    for index, row in data.iterrows():
        if 'сменим тему' in row['text']: continue

        if index != 0:
            story.append(PageBreak())

        story.append(Paragraph(f"{row['title']}", styleH))
        story.append(Paragraph(f"<font color='green'><u>{row['source']}</u></font>", styleN))
        story.append(Paragraph(f"Время: {row['time']}", styleN))
        story.append(Paragraph(f"<font color='blue'><u>{row['url']}</u></font>", styleN))
        story.append(Paragraph("<br></br>", styleN))
        story.append(Spacer(1, 12))
        story.append(Paragraph(f"{row['text']}", styleJ))
        story.append(Spacer(1, 12))

        try:
            img_path = f'{index}.jpeg'
            img = Image(img_path)

            img_width, img_height = img.wrap(0, 0)

            scale = min(max_width / img_width, max_height / img_height, 1)

            img.drawHeight = img_height * scale
            img.drawWidth = img_width * scale

            story.append(Spacer(1, 12))
            story.append(img)
            story.append(Spacer(1, 12))
        except Exception as ex:
            print('inserting img ex: ', ex)
            story.append(Spacer(1, 12))

        story.append(Paragraph("<br></br>", styleN))

    doc.build(story)

def send_email_with_attachment(to_email, subject, body, attachment_path):
    msg = MIMEMultipart()
    msg['From'] = SMTP_USERNAME
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    with open(attachment_path, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename= {os.path.basename(attachment_path)}')
    msg.attach(part)

    try:

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(SMTP_USERNAME, to_email, msg.as_string())
        print('Email sent successfully')
    except Exception as e:
        print(f'Failed to send email: {e}')

def send_pdf():
    try:
        print(1)
        descr_file = open('uploads/description.txt', 'r')
        descr = descr_file.read()
        descr_file.close()

        print(2)

        df = get_top_df(descr)

        print(3)

        current_day = datetime.now().day
        current_month = datetime.now().month
        current_year = datetime.now().year
        if current_day<10: current_day = '0'+str(current_day)
        if current_month<10: current_month = '0'+str(current_month)
        filtered_df = df[['source', 'url', 'title', 'time', 'text']]

        def process_text(text):
            summ = ''
            words = text.split()
            if (len(words) > 300):
                for batch in range(0, len(words), 300):
                    batch_text = ' '.join(words[batch:batch + 300])
                    summ += get_summary(batch_text) + ' '
            else:
                summ = get_summary(text)
            return summ.strip()

        text = filtered_df['text']
        summary = []

        for i in range(len(text)):
            summ = process_text(text[i])
            summary.append(summ)

        filtered_df['text'] = summary

        batch_size = 10

        for i in range(0, len(summary), batch_size):
            batch_summaries = summary[i:i + batch_size]
            indices = list(range(i, min(i + batch_size, len(summary))))

            with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
                futures = {executor.submit(generate_image_yandex, batch_summaries[j], indices[j]): indices[j] for j in
                           range(len(batch_summaries))}

                for future in concurrent.futures.as_completed(futures):
                    index = futures[future]
                    try:
                        future.result()
                    except Exception as ex:
                        print(f'Error generating image for index {index}: {ex}')


        pdf_filename = pdf_path
        create_pdf(pdf_filename, filtered_df, style_Arial_path)


        html_content = ""
        for index, row in filtered_df.iterrows():
            html_content += f"<h2>{row['title']}</h2>"
            html_content += f"<p><b>Источник:</b> {row['source']}</p>"
            html_content += f"<p><b>Время:</b> {row['time']}</p>"
            html_content += f"<p><b>URL:</b> <a href='{row['url']}'>{row['url']}</a></p>"
            html_content += f"<p><b>Текст:</b> {row['text']}</p>"
            html_content += "<hr>"

        email_body = f"<h1>Актуальный дайджест новостей:</h1>{html_content}"

        try:
            with open('uploads/mails.txt', 'r') as mail_file:
                mails = mail_file.read().strip()
                Sent_to = ast.literal_eval(mails)
                print(Sent_to)
        except Exception as ex:
            Sent_to = []
            print('!', ex)

        print(Sent_to)

        for email in Sent_to:
            send_email_with_attachment(email, f"Дайджест новостей от {current_day}.{current_month}.{current_year}", email_body, pdf_path)
        print("PDF Sent")

        return True
    except Exception as ex:
        print(ex)
        return False

if __name__ == '__main__':
    if not os.path.exists('uploads'):
        os.makedirs('uploads')

    scheduler.add_job(id='send_pdf_job', func=send_pdf, trigger='interval', days=mailing_interval)

    app.run(debug=True)