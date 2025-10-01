import json
import requests
import socket
from pathlib import Path
from datetime import datetime
import os
import urllib3
import pymssql

# ОСНОВНАЯ ПРОГРАММА (функция получения записей о мероприятиях)

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Настройки прокси
PROXY_HOST = '192.168.1.2'
PROXY_PORT = 8080
PROXY_USER = 'kuzminiv'
PROXY_PASS = '12345678Q!'

# URL прокси
PROXY_URL = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"

# Настройки SQL Server
SQL_SERVER = '192.168.1.224'
SQL_PORT = 1433
SQL_DATABASE = 'master'
SQL_USERNAME = 'kuzmin'
SQL_PASSWORD = '76543210'

# Настройки API
API_BASE_URL = "https://tpsg.etpgpb.ru/v1"

# Глобальная переменная для токена
API_AUTH_TOKEN = None

def setup_proxy():
    # Настраиваем прокси для системы
    os.environ['HTTP_PROXY'] = PROXY_URL
    os.environ['HTTPS_PROXY'] = PROXY_URL
    os.environ['NO_PROXY'] = 'localhost,127.0.0.1'
    
    print(f"Настроен прокси: {PROXY_HOST}:{PROXY_PORT}")
    print(f"Пользователь: {PROXY_USER}")

def test_proxy_connection():
    # Тестируем соединение через прокси
    try:
        test_url = 'http://httpbin.org/ip'
        response = requests.get(
            test_url,
            proxies={"http": PROXY_URL, "https": PROXY_URL},
            timeout=10,
            verify=False
        )
        
        print(f"Прокси работает! Ваш IP: {response.json()['origin']}")
        return True
        
    except Exception as e:
        print(f"Ошибка прокси: {e}")
        return False

def get_auth_token():
    # Получает токен авторизации из API
    # Returns: str: Токен или None в случае ошибки
    
    global API_AUTH_TOKEN
    
    try:
        # Читаем данные авторизации из файла
        auth_file = "authdata.json"
        if not os.path.exists(auth_file):
            print(f"Файл авторизации {auth_file} не найден!")
            return None
            
        with open(auth_file, 'r', encoding='utf-8') as f:
            auth_data = json.load(f)
        
        # Формируем URL для авторизации
        url = f"{API_BASE_URL}/admin/token"
        
        # Заголовки запроса
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        print(f"Отправка запроса авторизации...")
        print(f"URL: {url}")
        print(f"Email: {auth_data.get('auth', {}).get('email', 'N/A')}")
        
        # Отправляем POST запрос через прокси
        response = requests.post(
            url,
            json=auth_data,
            headers=headers,
            proxies={
                "http": PROXY_URL,
                "https": PROXY_URL
            },
            timeout=30,
            verify=False  # Игнорируем SSL проверки
        )
        
        print(f"Статус ответа авторизации: {response.status_code}")
        
        # Пытаемся получить JSON ответ
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON: {e}")
            print(f"Текст ответа: {response.text[:500]}...")
            return None
        
        # Проверяем статус ответа
        if response.status_code == 201:
            jwt_token = result.get('jwt')
            if jwt_token:
                API_AUTH_TOKEN = f"Bearer {jwt_token}"
                print("Авторизация успешна!")
                print(f"Токен получен: {jwt_token[:50]}...")
                return API_AUTH_TOKEN
            else:
                print("В ответе отсутствует JWT токен")
                print(f"Полный ответ: {result}")
                return None
        else:
            print(f"Ошибка авторизации: {response.status_code}")
            print(f"Полный ответ: {result}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сетевого запроса при авторизации: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка при авторизации: {e}")
        return None

def get_additional_activities(service_id=29):
    # Получает список дополнительных мероприятий из API
    # Args: service_id (int): ID услуги (по умолчанию 29)   
    # Returns: dict: Ответ от API или None в случае ошибки
    
    global API_AUTH_TOKEN
    
    if not API_AUTH_TOKEN:
        print("Токен авторизации не получен. Сначала выполните авторизацию.")
        return None
        
    try:
        # Формируем URL запроса
        url = f"{API_BASE_URL}/additional_activities"
        params = {"service_id": service_id}
        
        # Заголовки запроса
        headers = {
            "Accept": "application/json",
            "Authorization": API_AUTH_TOKEN,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        print(f"Отправка запроса к API...")
        print(f"URL: {url}")
        print(f"Параметры: service_id={service_id}")
        print(f"Токен: {API_AUTH_TOKEN[:50]}...")
        
        # Отправляем запрос через прокси
        response = requests.get(
            url,
            params=params,
            headers=headers,
            proxies={
                "http": PROXY_URL,
                "https": PROXY_URL
            },
            timeout=30,
            verify=False  # Игнорируем SSL проверки
        )
        
        print(f"Статус ответа: {response.status_code}")
        
        # Пытаемся получить JSON ответ
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON: {e}")
            print(f"Текст ответа: {response.text[:500]}...")
            return None
        
        # Проверяем статус ответа
        if response.status_code == 200:
            print("Запрос выполнен успешно!")
            print(f"Найдено мероприятий: {len(result.get('data', []))}")
            return result
        else:
            print(f"Ошибка API: {response.status_code}")
            print(f"Полный ответ: {result}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сетевого запроса: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        return None

def display_activities_data(data):
    # Красиво отображает данные о мероприятиях
    if not data or 'data' not in data:
        print("Нет данных для отображения")
        return
    
    activities = data['data']
    print(f"\n{'='*80}")
    print(f"ДОПОЛНИТЕЛЬНЫЕ МЕРОПРИЯТИЯ (всего: {len(activities)})")
    print(f"{'='*80}")
    
    for i, activity in enumerate(activities, 1):
        attrs = activity.get('attributes', {})
        print(f"\n{i}. {attrs.get('name', 'Нет названия')}")
        print(f"   ID: {attrs.get('id', 'N/A')}")
        print(f"   Slug: {attrs.get('slug', 'N/A')}")
        print(f"   Активно: {'Да' if attrs.get('active') else 'Нет'}")
        print(f"   Тип: {attrs.get('kind', 'N/A')}")
        print(f"   Основное: {'Да' if attrs.get('main') else 'Нет'}")
        print(f"   Роли: {', '.join(attrs.get('roles', []))}")
        print(f"   {'-'*50}")

def save_to_file(data, filename=None):
    # Сохраняет данные в JSON файл
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"additional_activities_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Данные сохранены в файл: {filename}")
        return filename
    except Exception as e:
        print(f"Ошибка сохранения файла: {e}")
        return None

def test_sql_connection():
    # Тестирует подключение к SQL Server
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            port=SQL_PORT,
            user=SQL_USERNAME,
            password=SQL_PASSWORD,
            database=SQL_DATABASE
        )
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()
        print(f"Подключение к SQL Server успешно")
        print(f"Версия сервера: {version[0][:100]}...")
        conn.close()
        return True
    except Exception as e:
        print(f"Ошибка подключения к SQL Server: {e}")
        return False

def main():
    # Основная функция программы 
    print("ЗАПУСК ПРОГРАММЫ ПОЛУЧЕНИЯ ДАННЫХ ИЗ API")
    print("=" * 50)
    
    # Настраиваем прокси
    setup_proxy()
    
    # Тестируем прокси
    if not test_proxy_connection():
        print("Предупреждение: прокси не работает, продолжаем без него...")
    
    # АВТОРИЗАЦИЯ
    print("\n" + "="*50)
    print("АВТОРИЗАЦИЯ В API")
    print("="*50)
    
    token = get_auth_token()
    if not token:
        print("Не удалось выполнить авторизацию. Программа завершена.")
        return
    
    # Получаем данные из API
    print("\n" + "="*50)
    print("ПОЛУЧЕНИЕ ДАННЫХ ИЗ API")
    print("="*50)
    
    # Можно изменить service_id при необходимости
    service_id = 3
    data = get_additional_activities(service_id)
    
    if data:
        # Отображаем данные
        display_activities_data(data)
        
        # Сохраняем в файл
        saved_file = save_to_file(data)
        
        # Дополнительная информация
        print(f"\n{'='*50}")
        print("СВОДКА:")
        print(f"• Статус: Успешно")
        print(f"• Количество мероприятий: {len(data.get('data', []))}")
        print(f"• Service ID: {service_id}")
        print(f"• Файл с данными: {saved_file}")
        print(f"• Время выполнения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print(f"\nНе удалось получить данные из API")
        
    print(f"\nПрограмма завершена.")

# Запуск программы
if __name__ == "__main__":
    main()