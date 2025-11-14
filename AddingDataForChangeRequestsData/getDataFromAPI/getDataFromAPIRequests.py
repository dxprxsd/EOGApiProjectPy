import os
import json
import requests
import urllib3
import time
from datetime import datetime

# Программа для получения данных с функции апи "GET /v1/admin/leads/{id} Просмотр заявки"

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Настройки прокси
PROXY_HOST = '192.168.1.2'
PROXY_PORT = 8080
PROXY_USER = 'kuzminiv'
PROXY_PASS = '12345678Q!'

# URL прокси
PROXY_URL = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"

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

def get_lead_by_id(lead_id):
    """
    Получает информацию о заявке по ID
    Args:
        lead_id (int): ID заявки
    Returns:
        dict: Данные заявки или None в случае ошибки
    """
    global API_AUTH_TOKEN
    
    if not API_AUTH_TOKEN:
        print("Токен авторизации не получен. Сначала выполните авторизацию.")
        return None
        
    try:
        # Формируем URL запроса для получения заявки
        url = f"{API_BASE_URL}/admin/leads/{lead_id}"
        
        # Параметры запроса - включаем все связанные модели
        params = {
            "included[]": [
                "user", "admin", "service", "region", "organization", 
                "source", "gas_object_address", "attachments", "contract", 
                "cancellation_reason", "correspondence_address", "rejection_reason",
                "protocol_mismatches", "parent", "children", "mfc_manager", 
                "mfc", "additional_activities"
            ]
        }
        
        # Заголовки запроса
        headers = {
            "Accept": "application/json",
            "Authorization": API_AUTH_TOKEN,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        print(f"Запрос заявки ID: {lead_id}")
        
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
        
        print(f"Статус ответа для заявки {lead_id}: {response.status_code}")
        
        # Пытаемся получить JSON ответ
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON для заявки {lead_id}: {e}")
            print(f"Текст ответа: {response.text[:200]}...")
            return None
        
        # Проверяем статус ответа
        if response.status_code == 200:
            print(f"Успешно получена заявка ID: {lead_id}")
            return result
        elif response.status_code == 401:
            print(f"Ошибка авторизации для заявки {lead_id}")
            return None
        elif response.status_code == 403:
            print(f"Недостаточно прав для заявки {lead_id}")
            return None
        elif response.status_code == 404:
            print(f"Заявка {lead_id} не найдена")
            return None
        else:
            print(f"Ошибка API для заявки {lead_id}: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сетевого запроса для заявки {lead_id}: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка для заявки {lead_id}: {e}")
        return None

def save_lead_to_file(lead_data, lead_id, output_dir="outputData"):
    """
    Сохраняет данные заявки в JSON файл
    Args:
        lead_data (dict): Данные заявки
        lead_id (int): ID заявки
        output_dir (str): Папка для сохранения
    """
    try:
        # Создаем папку если ее нет
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Создана папка: {output_dir}")
        
        # Формируем имя файла
        filename = f"lead_{lead_id}.json"
        filepath = os.path.join(output_dir, filename)
        
        # Сохраняем данные в файл
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(lead_data, f, ensure_ascii=False, indent=2)
        
        print(f"Заявка {lead_id} сохранена в: {filepath}")
        return True
        
    except Exception as e:
        print(f"Ошибка сохранения заявки {lead_id}: {e}")
        return False

def collect_all_leads(start_id=1, end_id=700, delay=0.5):
    """
    Собирает все заявки в указанном диапазоне ID
    Args:
        start_id (int): Начальный ID
        end_id (int): Конечный ID
        delay (float): Задержка между запросами в секундах
    """
    print(f"Начинаем сбор заявок с ID от {start_id} до {end_id}")
    print(f"Задержка между запросами: {delay} сек")
    
    # Статистика
    stats = {
        'total': 0,
        'success': 0,
        'failed': 0,
        'not_found': 0,
        'errors': 0,
        'start_time': datetime.now()
    }
    
    # Создаем папку для результатов
    output_dir = "outputData"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Основной цикл сбора данных
    for lead_id in range(start_id, end_id + 1):
        print(f"\n--- Обработка заявки {lead_id} ---")
        stats['total'] += 1
        
        # Получаем данные заявки
        lead_data = get_lead_by_id(lead_id)
        
        # Обрабатываем результат
        if lead_data:
            # Сохраняем в файл
            if save_lead_to_file(lead_data, lead_id, output_dir):
                stats['success'] += 1
                print(f"Успешно обработана заявка {lead_id}")
            else:
                stats['failed'] += 1
                print(f"Ошибка сохранения заявки {lead_id}")
        else:
            # Проверяем тип ошибки
            if lead_data is None:
                stats['errors'] += 1
                print(f"Ошибка получения заявки {lead_id}")
            else:
                stats['not_found'] += 1
                print(f"Заявка {lead_id} не найдена")
        
        # Задержка между запросами чтобы не перегружать сервер
        if lead_id < end_id:
            print(f"Ожидание {delay} сек...")
            time.sleep(delay)
    
    # Выводим статистику
    stats['end_time'] = datetime.now()
    stats['duration'] = stats['end_time'] - stats['start_time']
    
    print(f"\n{'='*50}")
    print("СТАТИСТИКА СБОРА ДАННЫХ:")
    print(f"{'='*50}")
    print(f"Всего обработано: {stats['total']}")
    print(f"Успешно собрано: {stats['success']}")
    print(f"Не найдено: {stats['not_found']}")
    print(f"Ошибок: {stats['errors']}")
    print(f"Не удалось сохранить: {stats['failed']}")
    print(f"Время выполнения: {stats['duration']}")
    print(f"Папка с результатами: {output_dir}")
    print(f"{'='*50}")
    
    # Сохраняем статистику в файл
    stats_file = os.path.join(output_dir, "collection_stats.json")
    with open(stats_file, 'w', encoding='utf-8') as f:
        # Конвертируем timedelta в строку для JSON
        stats_json = stats.copy()
        stats_json['start_time'] = stats_json['start_time'].isoformat()
        stats_json['end_time'] = stats_json['end_time'].isoformat()
        stats_json['duration'] = str(stats_json['duration'])
        json.dump(stats_json, f, ensure_ascii=False, indent=2)
    
    print(f"Статистика сохранена в: {stats_file}")

def main():
    """
    Основная функция выполнения скрипта
    """
    print("=== СКРИПТ СБОРА ДАННЫХ ЗАЯВОК ===")
    
    # Настраиваем прокси
    setup_proxy()
    
    # Тестируем подключение
    if not test_proxy_connection():
        print("Проверка прокси не удалась. Продолжаем с текущими настройками...")
    
    # Получаем токен авторизации
    token = get_auth_token()
    if not token:
        print("Не удалось получить токен авторизации. Завершение работы.")
        return
    
    # Запускаем сбор данных
    try:
        collect_all_leads(start_id=1, end_id=700, delay=0.3)
    except KeyboardInterrupt:
        print("\nСбор данных прерван пользователем.")
    except Exception as e:
        print(f"\nКритическая ошибка при сборе данных: {e}")

if __name__ == "__main__":
    main()