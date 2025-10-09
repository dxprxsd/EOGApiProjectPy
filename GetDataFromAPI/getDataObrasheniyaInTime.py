import json
import requests
import socket
from pathlib import Path
from datetime import datetime
import os
import urllib3
import time
import csv
import glob
import threading
from typing import List, Dict, Any

# ОСНОВНАЯ ПРОГРАММА (функция получения заявок) //заявки из функции GET /v1/admin/callbacks/{id} Просмотр заявки

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

# Глобальные переменные для мониторинга
MONITORING_ACTIVE = False
CURRENT_MONITORING_THREAD = None
LAST_CHECKED_IDS = set()

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

def refresh_auth_token():
    """Обновляет токен авторизации"""
    global API_AUTH_TOKEN
    print("Обновление токена авторизации...")
    API_AUTH_TOKEN = None
    return get_auth_token()

def get_callback_by_id(callback_id):
    # Получает заявку по ID из API
    # Args: 
    #   callback_id (int): ID заявки
    # Returns: dict: Ответ от API или None в случае ошибки
    
    global API_AUTH_TOKEN
    
    if not API_AUTH_TOKEN:
        print("Токен авторизации не получен. Сначала выполните авторизацию.")
        return None
        
    try:
        # Формируем URL запроса
        url = f"{API_BASE_URL}/admin/callbacks/{callback_id}"
        
        # Заголовки запроса
        headers = {
            "Accept": "application/json",
            "Authorization": API_AUTH_TOKEN,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        print(f"Запрос заявки с ID: {callback_id}")
        
        # Отправляем запрос через прокси
        response = requests.get(
            url,
            headers=headers,
            proxies={
                "http": PROXY_URL,
                "https": PROXY_URL
            },
            timeout=30,
            verify=False  # Игнорируем SSL проверки
        )
        
        print(f"Статус ответа для ID {callback_id}: {response.status_code}")
        
        # Пытаемся получить JSON ответ
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON для ID {callback_id}: {e}")
            print(f"Текст ответа: {response.text[:200]}...")
            return None
        
        # Проверяем статус ответа
        if response.status_code == 200:
            print(f"Успешно! Заявка с ID {callback_id} найдена")
            return result
        elif response.status_code == 401:
            print(f"Ошибка авторизации для ID {callback_id}")
            # Пробуем обновить токен
            if refresh_auth_token():
                return get_callback_by_id(callback_id)  # Рекурсивный вызов с новым токеном
            return None
        elif response.status_code == 403:
            print(f"Недостаточно прав для доступа к заявке ID {callback_id}")
            return None
        elif response.status_code == 404:
            print(f"Заявка с ID {callback_id} не найдена")
            return None
        elif response.status_code == 429:
            print(f"Превышен лимит запросов для ID {callback_id}. Ждем 60 секунд...")
            time.sleep(60)
            return get_callback_by_id(callback_id)  # Повторяем запрос после ожидания
        else:
            print(f"Ошибка API для ID {callback_id}: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сетевого запроса для ID {callback_id}: {e}")
        # Ждем и пробуем снова
        time.sleep(10)
        return get_callback_by_id(callback_id)
    except Exception as e:
        print(f"Неожиданная ошибка для ID {callback_id}: {e}")
        return None

def display_callback_data(data, callback_id):
    # Красиво отображает данные о заявке
    if not data or 'data' not in data:
        print(f"Нет данных для заявки ID {callback_id}")
        return
    
    callback = data['data']
    attrs = callback.get('attributes', {})
    
    print(f"\n{'='*80}")
    print(f"ЗАЯВКА ID: {callback_id}")
    print(f"{'='*80}")
    
    print(f"ID: {attrs.get('id', 'N/A')}")
    print(f"Имя: {attrs.get('name', 'N/A')}")
    print(f"Email: {attrs.get('email', 'N/A')}")
    print(f"Сообщение: {attrs.get('message', 'N/A')}")
    print(f"Дата создания: {attrs.get('created_at', 'N/A')}")
    
    # Отображаем информацию об администраторе
    relationships = callback.get('relationships', {})
    admin_data = relationships.get('admin', {}).get('data')
    if admin_data:
        print(f"Администратор: ID {admin_data.get('id', 'N/A')}")
    else:
        print(f"Администратор: не назначен")
    
    print(f"{'='*80}")

def save_callback_to_file(data, callback_id, folder="zayavki"):
    # Сохраняет данные заявки в JSON файл в указанную папку
    try:
        # Создаем папку если не существует
        Path(folder).mkdir(exist_ok=True)
        
        filename = f"{folder}/callback_{callback_id}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Данные заявки ID {callback_id} сохранены в файл: {filename}")
        return filename
    except Exception as e:
        print(f"Ошибка сохранения файла для заявки ID {callback_id}: {e}")
        return None

def monitor_callbacks_continuous(start_id=1, batch_size=200, interval_minutes=10):
    """
    Непрерывно мониторит заявки в режиме реального времени
    Проверяет batch_size ID каждые interval_minutes минут
    
    Args:
        start_id (int): начальный ID для мониторинга
        batch_size (int): количество ID для проверки за один цикл
        interval_minutes (int): интервал между проверками в минутах
    """
    global MONITORING_ACTIVE, LAST_CHECKED_IDS
    
    print(f"\n{'='*80}")
    print(f"ЗАПУСК НЕПРЕРЫВНОГО МОНИТОРИНГА ЗАЯВОК")
    print(f"Проверка {batch_size} ID каждые {interval_minutes} минут")
    print(f"Начальный ID: {start_id}")
    print(f"{'='*80}")
    
    MONITORING_ACTIVE = True
    current_start_id = start_id
    
    cycle_count = 0
    
    while MONITORING_ACTIVE:
        cycle_count += 1
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"\n\n{'='*80}")
        print(f"ЦИКЛ МОНИТОРИНГА #{cycle_count}")
        print(f"Время начала: {current_time}")
        print(f"Диапазон ID: {current_start_id} - {current_start_id + batch_size - 1}")
        print(f"{'='*80}")
        
        # Проверяем batch_size ID
        successful_requests = 0
        found_data_count = 0
        new_ids_found = 0
        
        for callback_id in range(current_start_id, current_start_id + batch_size):
            if not MONITORING_ACTIVE:
                break
                
            print(f"\n--- Проверка заявки ID: {callback_id} ---")
            
            # Проверяем, не проверяли ли мы уже этот ID
            if callback_id in LAST_CHECKED_IDS:
                print(f"ID {callback_id} уже проверялся ранее, пропускаем")
                continue
            
            # Получаем данные для текущего callback_id
            data = get_callback_by_id(callback_id)
            
            if data:
                if 'data' in data and data['data'] is not None:
                    # Если есть данные, отображаем и сохраняем
                    display_callback_data(data, callback_id)
                    save_callback_to_file(data, callback_id)
                    found_data_count += 1
                    new_ids_found += 1
                    LAST_CHECKED_IDS.add(callback_id)
                successful_requests += 1
            else:
                successful_requests += 1
            
            # Небольшая задержка чтобы не перегружать API
            time.sleep(0.5)
        
        # Выводим статистику цикла
        print(f"\n{'='*80}")
        print(f"СТАТИСТИКА ЦИКЛА #{cycle_count}:")
        print(f"* Проверено ID: {batch_size}")
        print(f"* Успешных запросов: {successful_requests}")
        print(f"* Найдено заявок: {found_data_count}")
        print(f"* Новых заявок: {new_ids_found}")
        print(f"* Всего проверенных ID: {len(LAST_CHECKED_IDS)}")
        print(f"{'='*80}")
        
        # Сохраняем статистику в файл
        save_monitoring_stats(cycle_count, current_start_id, batch_size, successful_requests, found_data_count, new_ids_found)
        
        # Сдвигаем диапазон для следующего цикла
        current_start_id += batch_size
        
        # Если мониторинг еще активен, ждем перед следующим циклом
        if MONITORING_ACTIVE:
            wait_seconds = interval_minutes * 60
            print(f"\nОжидание следующего цикла... ({interval_minutes} минут)")
            
            # Отсчет времени с прогресс-баром
            for i in range(wait_seconds):
                if not MONITORING_ACTIVE:
                    break
                if i % 60 == 0:  # Выводим сообщение каждую минуту
                    minutes_left = (wait_seconds - i) // 60
                    print(f"До следующего цикла: {minutes_left} минут...")
                time.sleep(1)
    
    print("Мониторинг остановлен")

def save_monitoring_stats(cycle_count, start_id, batch_size, successful_requests, found_data_count, new_ids_found):
    """Сохраняет статистику мониторинга в файл"""
    try:
        stats_file = "monitoring_stats.json"
        stats_data = {}
        
        # Загружаем существующую статистику если файл есть
        if os.path.exists(stats_file):
            with open(stats_file, 'r', encoding='utf-8') as f:
                stats_data = json.load(f)
        
        # Добавляем новую запись
        cycle_key = f"cycle_{cycle_count}"
        stats_data[cycle_key] = {
            "timestamp": datetime.now().isoformat(),
            "start_id": start_id,
            "end_id": start_id + batch_size - 1,
            "batch_size": batch_size,
            "successful_requests": successful_requests,
            "found_data_count": found_data_count,
            "new_ids_found": new_ids_found,
            "total_checked_ids": len(LAST_CHECKED_IDS)
        }
        
        # Сохраняем обновленную статистику
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats_data, f, ensure_ascii=False, indent=2)
        
        print(f"Статистика сохранена в файл: {stats_file}")
        
    except Exception as e:
        print(f"Ошибка сохранения статистики: {e}")

def start_monitoring(start_id=1, batch_size=200, interval_minutes=10):
    """Запускает мониторинг в отдельном потоке"""
    global MONITORING_ACTIVE, CURRENT_MONITORING_THREAD
    
    if MONITORING_ACTIVE:
        print("Мониторинг уже запущен!")
        return False
    
    MONITORING_ACTIVE = True
    CURRENT_MONITORING_THREAD = threading.Thread(
        target=monitor_callbacks_continuous,
        args=(start_id, batch_size, interval_minutes),
        daemon=True
    )
    CURRENT_MONITORING_THREAD.start()
    
    print(f"Мониторинг запущен в отдельном потоке")
    print(f"Проверка {batch_size} ID каждые {interval_minutes} минут")
    print(f"Начальный ID: {start_id}")
    print("Для остановки мониторинга выберите соответствующий пункт в меню")
    
    return True

def stop_monitoring():
    """Останавливает мониторинг"""
    global MONITORING_ACTIVE
    
    if not MONITORING_ACTIVE:
        print("Мониторинг не запущен!")
        return False
    
    MONITORING_ACTIVE = False
    print("Остановка мониторинга...")
    
    # Ждем завершения потока (максимум 10 секунд)
    if CURRENT_MONITORING_THREAD and CURRENT_MONITORING_THREAD.is_alive():
        CURRENT_MONITORING_THREAD.join(timeout=10)
    
    print("Мониторинг остановлен")
    return True

def get_monitoring_status():
    """Возвращает статус мониторинга"""
    global MONITORING_ACTIVE, LAST_CHECKED_IDS
    
    status = {
        "active": MONITORING_ACTIVE,
        "total_checked_ids": len(LAST_CHECKED_IDS),
        "last_check_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S') if LAST_CHECKED_IDS else "N/A"
    }
    
    return status

def safe_int_convert(value):
    """Безопасно преобразует значение в целое число"""
    try:
        if value is None:
            return 0
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return 0
    except:
        return 0

def json_to_csv(json_folder="zayavki", csv_filename="zayavki_report.csv"):
    """
    Преобразует все JSON файлы с заявками в CSV файл для печати
    
    Args:
        json_folder (str): Папка с JSON файлами
        csv_filename (str): Имя выходного CSV файла
    
    Returns:
        str: Путь к созданному CSV файлу или None в случае ошибки
    """
    try:
        print(f"\n{'='*80}")
        print("ПРЕОБРАЗОВАНИЕ JSON ФАЙЛОВ В CSV ДЛЯ ПЕЧАТИ")
        print(f"{'='*80}")
        
        # Проверяем существование папки
        if not os.path.exists(json_folder):
            print(f"Папка {json_folder} не существует!")
            return None
        
        # Ищем все JSON файлы в папке
        json_files = glob.glob(f"{json_folder}/callback_*.json")
        
        if not json_files:
            print(f"В папке {json_folder} не найдено JSON файлов заявок!")
            return None
        
        print(f"Найдено JSON файлов: {len(json_files)}")
        
        # Создаем список для хранения данных всех заявок
        all_callbacks_data = []
        
        # Читаем данные из каждого JSON файла
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Проверяем, что данные не None и содержат ожидаемую структуру
                if data is None:
                    print(f"Файл {json_file} содержит null данные, пропускаем")
                    continue
                
                # Извлекаем основные данные заявки с проверками на None
                callback_data = data.get('data')
                if callback_data is None:
                    print(f"Файл {json_file} не содержит данных заявки, пропускаем")
                    continue
                
                attributes = callback_data.get('attributes', {})
                relationships = callback_data.get('relationships', {})
                
                # Извлекаем данные администратора с проверками
                admin_relationship = relationships.get('admin', {})
                admin_data = admin_relationship.get('data', {}) if admin_relationship else {}
                
                # Получаем ID заявки из имени файла, если в данных его нет
                file_id = os.path.basename(json_file).replace('callback_', '').replace('.json', '')
                
                # Формируем строку для CSV
                row_data = {
                    'ID заявки': attributes.get('id', file_id),
                    'Имя': attributes.get('name', 'N/A'),
                    'Email': attributes.get('email', 'N/A'),
                    'Сообщение': attributes.get('message', 'N/A'),
                    'Дата создания': attributes.get('created_at', 'N/A'),
                    'ID администратора': admin_data.get('id', 'Не назначен') if admin_data else 'Не назначен',
                    'Тип администратора': admin_data.get('type', 'N/A') if admin_data else 'N/A',
                    'Файл источника': os.path.basename(json_file),
                    'Статус': 'Успешно'
                }
                
                all_callbacks_data.append(row_data)
                print(f"Обработан файл: {os.path.basename(json_file)}")
                
            except Exception as e:
                print(f"Ошибка обработки файла {json_file}: {e}")
                # Добавляем запись об ошибке
                file_id = os.path.basename(json_file).replace('callback_', '').replace('.json', '')
                error_row = {
                    'ID заявки': file_id,
                    'Имя': 'ОШИБКА',
                    'Email': 'ОШИБКА',
                    'Сообщение': f'Ошибка чтения файла: {str(e)}',
                    'Дата создания': 'N/A',
                    'ID администратора': 'N/A',
                    'Тип администратора': 'N/A',
                    'Файл источника': os.path.basename(json_file),
                    'Статус': 'Ошибка'
                }
                all_callbacks_data.append(error_row)
                continue
        
        if not all_callbacks_data:
            print("Не удалось извлечь данные из JSON файлов!")
            return None
        
        # Сортируем по ID заявки (безопасное преобразование)
        all_callbacks_data.sort(key=lambda x: safe_int_convert(x['ID заявки']))
        
        # Записываем в CSV файл
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if all_callbacks_data:
                fieldnames = all_callbacks_data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Записываем заголовки
                writer.writeheader()
                
                # Записываем данные
                for row in all_callbacks_data:
                    writer.writerow(row)
        
        # Создаем файл со статистикой
        stats_filename = "статистика_заявок.txt"
        successful_count = sum(1 for row in all_callbacks_data if row.get('Статус') == 'Успешно')
        error_count = sum(1 for row in all_callbacks_data if row.get('Статус') == 'Ошибка')
        
        with open(stats_filename, 'w', encoding='utf-8') as stats_file:
            stats_file.write("СТАТИСТИКА ПО ЗАЯВКАМ\n")
            stats_file.write("=" * 50 + "\n")
            stats_file.write(f"Общее количество заявок: {len(all_callbacks_data)}\n")
            stats_file.write(f"Успешно обработано: {successful_count}\n")
            stats_file.write(f"С ошибками: {error_count}\n")
            stats_file.write(f"Дата создания отчета: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            stats_file.write(f"Папка с исходными данными: {json_folder}\n")
            stats_file.write(f"CSV файл с данными: {csv_filename}\n")
            stats_file.write("\nКолонки в отчете:\n")
            stats_file.write("- ID заявки: Уникальный идентификатор заявки\n")
            stats_file.write("- Имя: Имя отправителя заявки\n")
            stats_file.write("- Email: Email отправителя\n")
            stats_file.write("- Сообщение: Текст сообщения заявки\n")
            stats_file.write("- Дата создания: Дата и время создания заявки\n")
            stats_file.write("- ID администратора: ID назначенного администратора\n")
            stats_file.write("- Тип администратора: Тип учетной записи администратора\n")
            stats_file.write("- Файл источника: Исходный JSON файл\n")
            stats_file.write("- Статус: Статус обработки заявки\n")
        
        print(f"\nCSV файл успешно создан: {csv_filename}")
        print(f"Файл статистики создан: {stats_filename}")
        print(f"Количество заявок в отчете: {len(all_callbacks_data)}")
        print(f"Успешно обработано: {successful_count}")
        print(f"С ошибками: {error_count}")
        
        return csv_filename
        
    except Exception as e:
        print(f"Ошибка при создании CSV файла: {e}")
        return None

def create_print_ready_csv(json_folder="zayavki", output_filename="zayavki_for_print.csv"):
    """
    Создает специальную версию CSV файла, оптимизированную для печати
    
    Args:
        json_folder (str): Папка с JSON файлами
        output_filename (str): Имя выходного файла
    
    Returns:
        str: Путь к созданному файлу
    """
    try:
        print(f"\nСоздание версии для печати...")
        
        # Проверяем существование папки
        if not os.path.exists(json_folder):
            print(f"Папка {json_folder} не существует!")
            return None
        
        # Ищем все JSON файлы в папке
        json_files = glob.glob(f"{json_folder}/callback_*.json")
        
        if not json_files:
            print(f"В папке {json_folder} не найдено JSON файлов заявок!")
            return None
        
        # Создаем список для хранения данных
        all_data = []
        
        # Читаем данные из JSON файлов
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if data is None:
                    continue
                    
                callback_data = data.get('data', {})
                if callback_data is None:
                    continue
                    
                attributes = callback_data.get('attributes', {})
                
                # Получаем ID из имени файла если в данных нет
                file_id = os.path.basename(json_file).replace('callback_', '').replace('.json', '')
                
                all_data.append({
                    '№': attributes.get('id', file_id),
                    'ФИО': attributes.get('name', 'N/A'),
                    'Контактный email': attributes.get('email', 'N/A'),
                    'Текст обращения': attributes.get('message', 'N/A'),
                    'Дата и время подачи': attributes.get('created_at', 'N/A')
                })
            except Exception as e:
                print(f"Ошибка обработки файла {json_file}: {e}")
                continue
        
        if not all_data:
            print("Не удалось извлечь данные из JSON файлов!")
            return None
        
        # Сортируем по ID (безопасное преобразование)
        all_data.sort(key=lambda x: safe_int_convert(x['№']))
        
        # Записываем в CSV
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if all_data:
                fieldnames = ['№', 'ФИО', 'Контактный email', 'Текст обращения', 'Дата и время подачи']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for row in all_data:
                    writer.writerow(row)
        
        print(f"Версия для печати создана: {output_filename}")
        print(f"Количество заявок: {len(all_data)}")
        return output_filename
        
    except Exception as e:
        print(f"Ошибка при создании версии для печати: {e}")
        return None

def json_to_text_report(json_folder="zayavki", txt_filename="zayavki_report.txt"):
    """
    Создает текстовый отчет с данными всех заявок
    
    Args:
        json_folder (str): Папка с JSON файлами
        txt_filename (str): Имя выходного текстового файла
    
    Returns:
        str: Путь к созданному файлу
    """
    try:
        print(f"\nСоздание текстового отчета...")
        
        # Проверяем существование папки
        if not os.path.exists(json_folder):
            print(f"Папка {json_folder} не существует!")
            return None
        
        # Ищем все JSON файлы в папке
        json_files = glob.glob(f"{json_folder}/callback_*.json")
        
        if not json_files:
            print(f"В папке {json_folder} не найдено JSON файлов заявок!")
            return None
        
        # Создаем текстовый отчет
        with open(txt_filename, 'w', encoding='utf-8') as txt_file:
            txt_file.write("ОТЧЕТ ПО ЗАЯВКАМ\n")
            txt_file.write("=" * 80 + "\n")
            txt_file.write(f"Дата формирования: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            txt_file.write(f"Всего файлов: {len(json_files)}\n")
            txt_file.write("=" * 80 + "\n\n")
            
            successful_count = 0
            error_count = 0
            
            # Обрабатываем каждый файл
            for i, json_file in enumerate(sorted(json_files), 1):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if data is None:
                        txt_file.write(f"ЗАЯВКА #{i} - ФАЙЛ: {os.path.basename(json_file)}\n")
                        txt_file.write("-" * 40 + "\n")
                        txt_file.write("СТАТУС: Файл содержит null данные\n")
                        txt_file.write("\n" + "=" * 80 + "\n\n")
                        error_count += 1
                        continue
                    
                    callback_data = data.get('data', {})
                    if callback_data is None:
                        txt_file.write(f"ЗАЯВКА #{i} - ФАЙЛ: {os.path.basename(json_file)}\n")
                        txt_file.write("-" * 40 + "\n")
                        txt_file.write("СТАТУС: Нет данных заявки\n")
                        txt_file.write("\n" + "=" * 80 + "\n\n")
                        error_count += 1
                        continue
                    
                    attributes = callback_data.get('attributes', {})
                    relationships = callback_data.get('relationships', {})
                    admin_data = relationships.get('admin', {}).get('data', {}) if relationships.get('admin') else {}
                    
                    txt_file.write(f"ЗАЯВКА #{i}\n")
                    txt_file.write("-" * 40 + "\n")
                    txt_file.write(f"Файл: {os.path.basename(json_file)}\n")
                    txt_file.write(f"ID заявки: {attributes.get('id', 'N/A')}\n")
                    txt_file.write(f"Имя: {attributes.get('name', 'N/A')}\n")
                    txt_file.write(f"Email: {attributes.get('email', 'N/A')}\n")
                    txt_file.write(f"Дата создания: {attributes.get('created_at', 'N/A')}\n")
                    txt_file.write(f"Администратор: {admin_data.get('id', 'Не назначен')}\n")
                    txt_file.write(f"Сообщение:\n{attributes.get('message', 'N/A')}\n")
                    txt_file.write("\n" + "=" * 80 + "\n\n")
                    successful_count += 1
                    
                except Exception as e:
                    txt_file.write(f"ЗАЯВКА #{i} - ФАЙЛ: {os.path.basename(json_file)}\n")
                    txt_file.write("-" * 40 + "\n")
                    txt_file.write(f"СТАТУС: Ошибка обработки - {str(e)}\n")
                    txt_file.write("\n" + "=" * 80 + "\n\n")
                    error_count += 1
                    continue
            
            # Добавляем статистику в конец файла
            txt_file.write("\n" + "=" * 80 + "\n")
            txt_file.write("СТАТИСТИКА ОБРАБОТКИ\n")
            txt_file.write("=" * 80 + "\n")
            txt_file.write(f"Успешно обработано: {successful_count}\n")
            txt_file.write(f"С ошибками: {error_count}\n")
            txt_file.write(f"Всего файлов: {len(json_files)}\n")
        
        print(f"Текстовый отчет создан: {txt_filename}")
        print(f"Успешно обработано: {successful_count}")
        print(f"С ошибками: {error_count}")
        return txt_filename
        
    except Exception as e:
        print(f"Ошибка при создании текстового отчета: {e}")
        return None

def scan_all_callbacks(start_id=1, end_id=300):
    # Сканирует все callback_id в указанном диапазоне
    # Args:
    #   start_id (int): начальный ID
    #   end_id (int): конечный ID
    
    print(f"\n{'='*80}")
    print(f"СКАНИРОВАНИЕ ЗАЯВОК ОТ ID {start_id} ДО {end_id}")
    print(f"{'='*80}")
    
    successful_requests = 0
    found_data_count = 0
    error_403_count = 0
    not_found_count = 0
    
    for callback_id in range(start_id, end_id + 1):
        print(f"\n--- Проверка заявки ID: {callback_id} ---")
        
        # Получаем данные для текущего callback_id
        data = get_callback_by_id(callback_id)
        
        if data:
            if 'data' in data and data['data'] is not None:
                # Если есть данные, отображаем и сохраняем
                display_callback_data(data, callback_id)
                save_callback_to_file(data, callback_id)
                found_data_count += 1
                successful_requests += 1
            else:
                # Если структура данных не соответствует ожидаемой
                print(f"Неожиданная структура данных для ID {callback_id}")
                successful_requests += 1
        else:
            # Увеличиваем счетчики ошибок
            if data is None:
                not_found_count += 1
            # Для других ошибок (401, 403 и т.д.) счетчики уже обработаны в get_callback_by_id
        
        # Небольшая задержка чтобы не перегружать API
        time.sleep(0.3)
    
    # Выводим итоговую статистику
    print(f"\n{'='*80}")
    print("ИТОГИ СКАНИРОВАНИЯ ЗАЯВОК:")
    print(f"* Проверено ID: {end_id - start_id + 1}")
    print(f"* Успешных запросов: {successful_requests}")
    print(f"* Найдено заявок: {found_data_count}")
    print(f"* Не найдено (404): {not_found_count}")
    print(f"* Ошибок доступа (403): {error_403_count}")
    print(f"* Данные сохранены в папку: zayavki/")
    print(f"{'='*80}")
    
    return successful_requests, found_data_count

def test_specific_callback_id(callback_id=29):
    # Тестирует конкретный callback_id для отладки
    print(f"\n{'='*80}")
    print(f"ТЕСТИРОВАНИЕ КОНКРЕТНОЙ ЗАЯВКИ ID: {callback_id}")
    print(f"{'='*80}")
    
    data = get_callback_by_id(callback_id)
    
    if data and 'data' in data:
        display_callback_data(data, callback_id)
        save_callback_to_file(data, callback_id)
        return True
    else:
        print(f"Не удалось получить данные для заявки ID {callback_id}")
        return False

def main_callbacks():
    # Основная функция программы для работы с заявками
    print("ЗАПУСК ПРОГРАММЫ ПОЛУЧЕНИЯ ДАННЫХ О ЗАЯВКАХ ИЗ API")
    print("=" * 60)
    
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
    while True:
        print("\n" + "="*50)
        print("ВЫБЕРИТЕ РЕЖИМ РАБОТЫ С ЗАЯВКАМИ:")
        print("1. Тестирование конкретной заявки (ID 29)")
        print("2. Сканирование всех заявок от ID 1 до 500")
        print("3. Сканирование в пользовательском диапазоне")
        print("4. Преобразовать JSON в CSV (из папки zayavki)")
        print("5. Создать CSV версию для печати")
        print("6. Создать текстовый отчет")
        print("7. ЗАПУСТИТЬ НЕПРЕРЫВНЫЙ МОНИТОРИНГ (200 ID каждые 10 мин)")
        print("8. Статус мониторинга")
        print("9. ОСТАНОВИТЬ мониторинг")
        print("0. Выход из программы")
        print("="*50)
        
        choice = input("Введите номер режима (0-9): ").strip()
        
        if choice == "0":
            # Останавливаем мониторинг перед выходом
            if get_monitoring_status()["active"]:
                stop_monitoring()
            print("Выход из программы...")
            break
            
        elif choice == "1":
            # Тестируем конкретную заявку
            callback_id = input("Введите ID заявки (по умолчанию 29): ").strip()
            if not callback_id:
                callback_id = 29
            else:
                callback_id = int(callback_id)
            test_specific_callback_id(callback_id)
            
        elif choice == "2":
            # Сканируем все заявки от 1 до 500
            successful, found = scan_all_callbacks(1, 500)
            
            print(f"\nРезультат сканирования заявок:")
            print(f"Успешно обработано: {successful} запросов")
            print(f"Найдено заявок: {found}")
            
        elif choice == "3":
            # Пользовательский диапазон
            start_id = input("Введите начальный ID (по умолчанию 1): ").strip()
            end_id = input("Введите конечный ID (по умолчанию 500): ").strip()
            
            start_id = int(start_id) if start_id else 1
            end_id = int(end_id) if end_id else 500
            
            successful, found = scan_all_callbacks(start_id, end_id)
            
            print(f"\nРезультат сканирования заявок:")
            print(f"Диапазон: от {start_id} до {end_id}")
            print(f"Успешно обработано: {successful} запросов")
            print(f"Найдено заявок: {found}")
        
        elif choice == "4":
            # Преобразование JSON в CSV
            folder = input("Введите папку с JSON файлами (по умолчанию zayavki): ").strip()
            if not folder:
                folder = "zayavki"
            
            csv_file = input("Введите имя CSV файла (по умолчанию zayavki_report.csv): ").strip()
            if not csv_file:
                csv_file = "zayavki_report.csv"
            
            result = json_to_csv(folder, csv_file)
            if result:
                print(f"\nCSV файл успешно создан: {result}")
                print("Файл можно открыть в Excel или любом другом редакторе таблиц")
            else:
                print("\nНе удалось создать CSV файл")
        
        elif choice == "5":
            # Создание CSV версии для печати
            folder = input("Введите папку с JSON файлами (по умолчанию zayavki): ").strip()
            if not folder:
                folder = "zayavki"
            
            print_file = input("Введите имя файла для печати (по умолчанию zayavki_for_print.csv): ").strip()
            if not print_file:
                print_file = "zayavki_for_print.csv"
            
            result = create_print_ready_csv(folder, print_file)
            if result:
                print(f"\nВерсия для печати создана: {result}")
                print("Файл содержит упрощенную структуру для удобства печати")
            else:
                print("\nНе удалось создать файл для печати")
        
        elif choice == "6":
            # Создание текстового отчета
            folder = input("Введите папку с JSON файлами (по умолчанию zayavki): ").strip()
            if not folder:
                folder = "zayavki"
            
            txt_file = input("Введите имя текстового файла (по умолчанию zayavki_report.txt): ").strip()
            if not txt_file:
                txt_file = "zayavki_report.txt"
            
            result = json_to_text_report(folder, txt_file)
            if result:
                print(f"\nТекстовый отчет создан: {result}")
                print("Файл готов для просмотра и печати в текстовом редакторе")
            else:
                print("\nНе удалось создать текстовый отчет")
        
        elif choice == "7":
            # Запуск непрерывного мониторинга
            start_id = input("Введите начальный ID (по умолчанию 1): ").strip()
            batch_size = input("Введите количество ID за цикл (по умолчанию 200): ").strip()
            interval = input("Введите интервал в минутах (по умолчанию 10): ").strip()
            
            start_id = int(start_id) if start_id else 1
            batch_size = int(batch_size) if batch_size else 200
            interval = int(interval) if interval else 10
            
            if start_monitoring(start_id, batch_size, interval):
                print("Мониторинг успешно запущен!")
                print("Программа продолжит работу в фоновом режиме.")
                print("Вы можете проверять статус или остановить мониторинг через меню.")
        
        elif choice == "8":
            # Статус мониторинга
            status = get_monitoring_status()
            print(f"\nСТАТУС МОНИТОРИНГА:")
            print(f"Активен: {'ДА' if status['active'] else 'НЕТ'}")
            print(f"Проверено ID: {status['total_checked_ids']}")
            print(f"Время последней проверки: {status['last_check_time']}")
        
        elif choice == "9":
            # Остановка мониторинга
            if stop_monitoring():
                print("Мониторинг остановлен!")
            else:
                print("Мониторинг не был запущен")
        
        else:
            print("Неверный выбор. Попробуйте снова.")
    
    print(f"\nПрограмма завершена в {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Запуск программы
if __name__ == "__main__":
    try:
        main_callbacks()
    except KeyboardInterrupt:
        print("\n\nПрограмма прервана пользователем")
        stop_monitoring()
    except Exception as e:
        print(f"\n\nКритическая ошибка: {e}")
        stop_monitoring()