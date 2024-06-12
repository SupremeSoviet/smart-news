from flask import Flask, render_template, request, redirect, url_for, flash
import dotenv
import os

app = Flask(__name__)
app.secret_key = 'your_secret_key'

# Предопределенный ключ авторизации
dotenv.load_dotenv()
AUTH_KEY = os.getenv("AUTH_KEY")


@app.route('/')
def auth():
    return render_template('auth.html')


@app.route('/login', methods=['POST'])
def login():
    key = request.form.get('auth_key')
    if key == AUTH_KEY:
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
        if check_accs(filename):
            flash('Успешно загружено', 'success')
        else:
            flash('Ошибка загрузки', 'error')
    else:
        flash('Неверный формат файла. Загрузите .txt файл', 'error')

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
    return True



if __name__ == '__main__':
    if not os.path.exists('uploads'):
        os.makedirs('uploads')
    app.run(debug=True)
