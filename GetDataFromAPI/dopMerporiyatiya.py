import json
import requests
import socket
from pathlib import Path
from datetime import datetime
import os
import urllib3
import pymssql
import time

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

def get_additional_activities(service_id=29, active=True):
    # Получает список дополнительных мероприятий из API
    # Args: 
    #   service_id (int): ID услуги (по умолчанию 29)
    #   active (bool): фильтр по активности (по умолчанию True)
    # Returns: dict: Ответ от API или None в случае ошибки
    
    global API_AUTH_TOKEN
    
    if not API_AUTH_TOKEN:
        print("Токен авторизации не получен. Сначала выполните авторизацию.")
        return None
        
    try:
        # Формируем URL запроса - ИСПРАВЛЕНО: правильный endpoint
        url = f"{API_BASE_URL}/admin/additional_activities"
        params = {
            "service_id": service_id,
            "active": str(active).lower()
        }
        
        # Заголовки запроса
        headers = {
            "Accept": "application/json",
            "Authorization": API_AUTH_TOKEN,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        print(f"Запрос для service_id={service_id}, active={active}")
        
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
        
        print(f"Статус ответа для service_id {service_id}: {response.status_code}")
        
        # Пытаемся получить JSON ответ
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON для service_id {service_id}: {e}")
            print(f"Текст ответа: {response.text[:200]}...")
            return None
        
        # Проверяем статус ответа
        if response.status_code == 200:
            activities_count = len(result.get('data', []))
            print(f"Успешно! Найдено мероприятий: {activities_count}")
            return result
        elif response.status_code == 401:
            print(f"Ошибка авторизации для service_id {service_id}")
            return None
        else:
            print(f"Ошибка API для service_id {service_id}: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сетевого запроса для service_id {service_id}: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка для service_id {service_id}: {e}")
        return None

def display_activities_data(data, service_id):
    # Красиво отображает данные о мероприятиях
    if not data or 'data' not in data:
        print(f"Нет данных для service_id {service_id}")
        return
    
    activities = data['data']
    print(f"\n{'='*80}")
    print(f"ДОПОЛНИТЕЛЬНЫЕ МЕРОПРИЯТИЯ для service_id {service_id} (всего: {len(activities)})")
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

def save_to_file(data, service_id, folder="dopMeropriyatiyaData"):
    # Сохраняет данные в JSON файл в указанную папку
    try:
        # Создаем папку если не существует
        Path(folder).mkdir(exist_ok=True)
        
        filename = f"{folder}/additional_activities_service_{service_id}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Данные для service_id {service_id} сохранены в файл: {filename}")
        return filename
    except Exception as e:
        print(f"Ошибка сохранения файла для service_id {service_id}: {e}")
        return None

def scan_all_service_ids(start_id=1, end_id=100, active=True):
    # Сканирует все service_id в указанном диапазоне
    # Args:
    #   start_id (int): начальный ID
    #   end_id (int): конечный ID
    #   active (bool): фильтр по активности
    
    print(f"\n{'='*80}")
    print(f"СКАНИРОВАНИЕ SERVICE_ID ОТ {start_id} ДО {end_id}")
    print(f"{'='*80}")
    
    successful_requests = 0
    found_data_count = 0
    
    for service_id in range(start_id, end_id + 1):
        print(f"\n--- Проверка service_id: {service_id} ---")
        
        # Получаем данные для текущего service_id
        data = get_additional_activities(service_id, active)
        
        if data and 'data' in data and len(data['data']) > 0:
            # Если есть данные, отображаем и сохраняем
            display_activities_data(data, service_id)
            save_to_file(data, service_id)
            found_data_count += 1
            successful_requests += 1
        elif data:
            # Если ответ успешный, но данных нет
            print(f"Для service_id {service_id} нет дополнительных мероприятий")
            successful_requests += 1
        
        # Небольшая задержка чтобы не перегружать API
        time.sleep(0.5)
    
    # Выводим итоговую статистику
    print(f"\n{'='*80}")
    print("ИТОГИ СКАНИРОВАНИЯ:")
    print(f"• Проверено service_id: {end_id - start_id + 1}")
    print(f"• Успешных запросов: {successful_requests}")
    print(f"• Найдено service_id с данными: {found_data_count}")
    print(f"• Данные сохранены в папку: dopMeropriyatiyaData/")
    print(f"{'='*80}")
    
    return successful_requests, found_data_count

def test_specific_service_id(service_id=29, active=True):
    # Тестирует конкретный service_id для отладки
    print(f"\n{'='*80}")
    print(f"ТЕСТИРОВАНИЕ КОНКРЕТНОГО SERVICE_ID: {service_id}")
    print(f"{'='*80}")
    
    data = get_additional_activities(service_id, active)
    
    if data:
        display_activities_data(data, service_id)
        save_to_file(data, service_id)
        return True
    else:
        print(f"Не удалось получить данные для service_id {service_id}")
        return False

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
    
    # Меню выбора режима работы
    print("\n" + "="*50)
    print("ВЫБЕРИТЕ РЕЖИМ РАБОТЫ:")
    print("1. Тестирование конкретного service_id (29)")
    print("2. Сканирование всех service_id от 1 до 100")
    print("="*50)
    
    choice = input("Введите номер режима (1 или 2): ").strip()
    
    if choice == "1":
        # Тестируем конкретный service_id
        test_specific_service_id(29, True)
    elif choice == "2":
        # Сканируем все service_id от 1 до 300
        successful, found = scan_all_service_ids(1, 300, True)
        
        print(f"\nРезультат сканирования:")
        print(f"Успешно обработано: {successful} запросов")
        print(f"Найдено service_id с данными: {found}")
    else:
        print("Неверный выбор. Завершение программы.")
        return
        
    print(f"\nПрограмма завершена в {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Запуск программы
if __name__ == "__main__":
    main()