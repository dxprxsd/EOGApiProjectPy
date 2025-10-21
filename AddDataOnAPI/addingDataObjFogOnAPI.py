import json
import requests
import socket
from pathlib import Path
from datetime import datetime
import time
import os
import urllib3
import pymssql
import uuid

# Программа для получения данных из БД и создания объектов газификации через API администратора 
# (для просмотра результата можно запустить программу getGasObjectData.py)
# для создания заявки использовать ID пользователя 4358

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
API_AUTH_TOKEN = None  
AUTH_RETRY_COUNT = 0
MAX_AUTH_RETRIES = 3

# ID пользователя для которого создаются объекты газификации
TARGET_USER_ID = 161  # Можно сделать настраиваемым

def setup_proxy():
    """Настраиваем прокси для системы"""
    os.environ['HTTP_PROXY'] = PROXY_URL
    os.environ['HTTPS_PROXY'] = PROXY_URL
    os.environ['NO_PROXY'] = 'localhost,127.0.0.1'
    
    print(f"Настроен прокси: {PROXY_HOST}:{PROXY_PORT}")
    print(f"Пользователь: {PROXY_USER}")

def test_proxy_connection():
    """Тестируем соединение через прокси"""
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

# ========== ФУНКЦИИ АВТОРИЗАЦИИ ==========

def get_auth_token():
    """Получает токен авторизации из API"""
    global API_AUTH_TOKEN, AUTH_RETRY_COUNT
    
    try:
        # Проверяем количество попыток
        if AUTH_RETRY_COUNT >= MAX_AUTH_RETRIES:
            print(f"Превышено максимальное количество попыток авторизации ({MAX_AUTH_RETRIES})")
            return None
            
        AUTH_RETRY_COUNT += 1
        
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
            verify=False
        )
        
        print(f"Статус ответа авторизации: {response.status_code}")
        
        # Обработка различных статусов
        if response.status_code == 429:
            print("Превышен лимит запросов (429). Ожидание 60 секунд...")
            time.sleep(60)
            return get_auth_token()  # Рекурсивный вызов после ожидания
            
        elif response.status_code == 201:
            # Пытаемся получить JSON ответ
            try:
                result = response.json()
            except json.JSONDecodeError as e:
                print(f"Ошибка декодирования JSON: {e}")
                print(f"Текст ответа: {response.text[:500]}...")
                return None
            
            jwt_token = result.get('jwt')
            if jwt_token:
                API_AUTH_TOKEN = f"Bearer {jwt_token}"
                AUTH_RETRY_COUNT = 0  # Сбрасываем счетчик при успешной авторизации
                print("Авторизация успешна!")
                print(f"Токен получен: {jwt_token[:50]}...")
                return API_AUTH_TOKEN
            else:
                print("В ответе отсутствует JWT токен")
                print(f"Полный ответ: {result}")
                return None
                
        else:
            print(f"Ошибка авторизации: {response.status_code}")
            print(f"Текст ответа: {response.text[:500]}...")
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

def ensure_auth():
    """Обеспечивает наличие действительного токена авторизации"""
    global API_AUTH_TOKEN
    if not API_AUTH_TOKEN:
        return get_auth_token()
    return API_AUTH_TOKEN

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ ==========

def get_sql_connection():
    """Устанавливаем соединение с SQL Server используя pymssql"""
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            port=SQL_PORT,
            user=SQL_USERNAME,
            password=SQL_PASSWORD,
            database=SQL_DATABASE,
            as_dict=True  # Возвращать результаты как словари
        )
        print("Успешное подключение к SQL Server!")
        return conn
        
    except pymssql.Error as e:
        print(f"Ошибка подключения к SQL Server: {e}")
        return None

def get_complete_address_data(limit=100):
    """Получение полных данных об адресах из связанных таблиц"""
    conn = get_sql_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        query = f"""
        SELECT TOP ({limit})
            pg.id AS object_id,
            pg.descr AS object_name,
            pg.a_dom AS house_number,
            pg.a_korp AS block,
            pg.adr AS full_address,
            p2.NAME AS settlement_name,
            p2.INDEX AS zip_code,
            p2.aoguid AS settlement_fias_id,
            p3.NAME AS street_name,
            p3.SOCR AS street_type,
            p3.aoguid AS street_fias_id,
            r.NAMEREG AS region_name,
            r.kladr_reg AS region_code
        FROM [dog].[dbo].[pto_grp] pg
        LEFT JOIN [dog].[dbo].[P2] p2 ON pg.a_p2 = p2.ID
        LEFT JOIN [dog].[dbo].[P3] p3 ON pg.a_p3 = p3.ID
        LEFT JOIN [dog].[dbo].[region] r ON p2.IDREGION = r.IDR
        WHERE pg.a_dom IS NOT NULL
        ORDER BY pg.id
        """
        
        print("Выполняем запрос для получения полных данных об адресах...")
        cursor.execute(query)
        
        results = cursor.fetchall()
        print(f"Получено {len(results)} записей с полными адресными данными")
        return results
            
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None
    finally:
        conn.close()

def create_gas_objects_from_db_data(db_data, limit=10):
    """Создание объектов газификации из данных БД в формате API"""
    gas_objects = []
    
    for i, row in enumerate(db_data):
        if i >= limit:
            break
            
        print(f"Подготовка объекта {i+1} из БД...")
        
        # Подготавливаем данные в формате API
        gas_object = prepare_gas_object_data(row)
        
        # Добавляем метаданные
        gas_object_with_metadata = {
            "template": gas_object,
            "metadata": {
                "source": "pto_grp",
                "source_id": row.get('object_id', i),
                "created_at": datetime.now().isoformat(),
                "batch_processed": False
            }
        }
        
        gas_objects.append(gas_object_with_metadata)
    
    return gas_objects

def prepare_gas_object_data(row):
    """Подготовка данных для создания объекта газификации в формате нового API"""
    
    # Формируем название объекта
    object_name = row['object_name'] or f"Объект {row['house_number']}"
    
    # Формируем полное название улицы
    street_full = f"{row.get('street_type', 'ул')} {row['street_name']}" if row.get('street_name') else "ул. Не указана"
    
    # Формируем заголовок адреса
    address_title = f"{row['region_name']}, {street_full} {row['house_number']}"
    
    # Функция для генерации корректного GUID
    def generate_valid_guid():
        return str(uuid.uuid4()).upper()
    
    # Функция для валидации и исправления FIAS ID
    def validate_fias_id(fias_id):
        if not fias_id:
            return generate_valid_guid()
        
        # Проверяем, является ли строка корректным GUID
        try:
            uuid.UUID(fias_id)
            return fias_id.upper()
        except (ValueError, AttributeError):
            # Если это не GUID, генерируем новый
            return generate_valid_guid()
    
    # Получаем и валидируем FIAS ID из базы данных
    settlement_fias_id = validate_fias_id(row.get('settlement_fias_id'))
    street_fias_id = validate_fias_id(row.get('street_fias_id'))
    
    # Для всех FIAS ID используем либо валидные значения из БД, либо генерируем новые
    region_fias_id = validate_fias_id(row.get('settlement_fias_id'))  # Используем settlement как fallback
    city_fias_id = settlement_fias_id
    area_fias_id = settlement_fias_id
    house_fias_id = settlement_fias_id
    
    gas_object_data = {
        "data": {
            "type": "gas_object",
            "attributes": {
                "name": object_name
            },
            "relationships": {
                "address": {
                    "data": {
                        "type": "address",
                        "attributes": {
                            "country": "Россия",
                            "region": row['region_name'],
                            "city": row['settlement_name'],
                            "settlement": row['settlement_name'],
                            "area": row.get('region_name', ''),
                            "zip_code": int(row.get('zip_code', 0)) if row.get('zip_code') else 0,
                            "street": street_full,
                            "house": str(row['house_number']),
                            "block": str(row.get('block')) if row.get('block') else None,
                            "flat": 1,
                            "room": None,
                            "region_fias_id": region_fias_id,
                            "city_fias_id": city_fias_id,
                            "settlement_fias_id": settlement_fias_id,
                            "area_fias_id": area_fias_id,
                            "house_fias_id": house_fias_id,
                            "street_fias_id": street_fias_id,
                            "extra": row.get('full_address', ''),
                            "oktmo": 123456789,
                            "cadastral_number": "123421W",
                            "cadastral_home_number": "123421H",
                            "okato": "123421H",
                            "title": address_title,
                            "has_capital_construction": True,
                            "room_type": "apartment_building"
                        }
                    }
                }
            }
        }
    }
    
    # Очистка None значений для числовых полей
    numeric_fields = ["oktmo", "flat", "room"]
    for field in numeric_fields:
        if gas_object_data["data"]["relationships"]["address"]["data"]["attributes"][field] is None:
            gas_object_data["data"]["relationships"]["address"]["data"]["attributes"][field] = 0
    
    # Преобразование в строку для полей, которые должны быть строками согласно API
    string_fields = ["house", "block", "flat", "room"]
    for field in string_fields:
        value = gas_object_data["data"]["relationships"]["address"]["data"]["attributes"][field]
        if value is not None and value != 0:
            gas_object_data["data"]["relationships"]["address"]["data"]["attributes"][field] = str(value)
    
    return gas_object_data

# ========== ФУНКЦИИ ДЛЯ СОЗДАНИЯ ОБЪЕКТОВ ГАЗИФИКАЦИИ ==========

def send_gas_object_to_api(gas_object_data, user_id, auth_token=None):
    """Отправка данных об объекте газификации в API администратора"""
    global AUTH_RETRY_COUNT
    
    # Новый URL с user_id в пути
    url = f"{API_BASE_URL}/admin/users/{user_id}/gas_objects"
    
    # Добавляем параметр included для включения связанных моделей
    params = {"included": "address"}
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    if auth_token:
        headers["Authorization"] = auth_token
    
    try:
        response = requests.post(
            url,
            headers=headers,
            params=params,
            json=gas_object_data,
            proxies={"http": PROXY_URL, "https": PROXY_URL},
            timeout=30,
            verify=False
        )
        
        print(f"Статус отправки: {response.status_code}")
        
        if response.status_code in [200, 201]:
            print("Объект газификации успешно создан!")
            AUTH_RETRY_COUNT = 0  # Сбрасываем счетчик при успешной операции
            return response.json()
            
        elif response.status_code == 401:
            print("Ошибка авторизации (401). Попытка обновить токен...")
            new_token = refresh_auth_token()
            if new_token:
                headers["Authorization"] = new_token
                response = requests.post(
                    url,
                    headers=headers,
                    params=params,
                    json=gas_object_data,
                    proxies={"http": PROXY_URL, "https": PROXY_URL},
                    timeout=30,
                    verify=False
                )
                if response.status_code in [200, 201]:
                    print("Объект успешно создан после обновления токена!")
                    return response.json()
                    
        elif response.status_code == 422:
            print("Ошибка валидации данных (422):")
            try:
                error_data = response.json()
                print("Детали ошибки:")
                for error in error_data.get('errors', []):
                    print(f"  - {error.get('title')}: {error.get('detail')}")
            except:
                print(f"Текст ответа: {response.text}")
            return None
            
        elif response.status_code == 404:
            print("Пользователь не найден (404)")
            return None
            
        elif response.status_code == 429:
            print("Превышен лимит запросов (429). Ожидание 60 секунд...")
            time.sleep(60)
            # Повторяем запрос после ожидания
            return send_gas_object_to_api(gas_object_data, user_id, auth_token)
            
        else:
            print(f"Ошибка API: {response.status_code}")
            print(f"Ответ: {response.text[:500]}...")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сетевого запроса: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка при отправке в API: {e}")
        return None

# ========== ФУНКЦИИ ДЛЯ ДИАГНОСТИКИ И ИСПРАВЛЕНИЯ ФАЙЛОВ ==========

def diagnose_existing_file(file_path):
    """Диагностика проблем в существующем файле"""
    print(f"\nДиагностика файла: {file_path}")
    
    data = load_data_from_file(file_path)
    if not data:
        return
    
    # Определяем структуру данных
    if 'template' in data:
        api_data = data['template']
        print("Формат: с метаданными (template)")
    else:
        api_data = data
        print("Формат: прямой API формат")
    
    # Проверяем FIAS ID
    address_attrs = api_data['data']['relationships']['address']['data']['attributes']
    fias_fields = ['region_fias_id', 'city_fias_id', 'settlement_fias_id', 'area_fias_id', 'house_fias_id', 'street_fias_id']
    
    print("\nПроверка FIAS ID:")
    issues_found = False
    for field in fias_fields:
        value = address_attrs.get(field)
        status = "Корректный GUID"
        
        # Проверяем формат
        if value:
            try:
                uuid.UUID(value)
            except (ValueError, AttributeError):
                status = "Некорректный формат GUID"
                issues_found = True
        else:
            status = "Отсутствует"
            issues_found = True
            
        print(f"  {field}: {value} -> {status}")
    
    if not issues_found:
        print("\nВсе FIAS ID корректны!")
    else:
        print("\nОбнаружены проблемы с FIAS ID!")
    
    return issues_found

def fix_existing_file(file_path):
    """Исправление FIAS ID в существующем файле"""
    print(f"\nИсправление файла: {file_path}")
    
    data = load_data_from_file(file_path)
    if not data:
        return None
    
    # Определяем структуру данных
    if 'template' in data:
        api_data = data['template']
        is_template_format = True
        print("Формат: с метаданными (template)")
    else:
        api_data = data
        is_template_format = False
        print("Формат: прямой API формат")
    
    # Функция для генерации корректного GUID
    def generate_valid_guid():
        return str(uuid.uuid4()).upper()
    
    # Функция для валидации и исправления FIAS ID
    def validate_fias_id(fias_id):
        if not fias_id:
            return generate_valid_guid()
        
        # Проверяем, является ли строка корректным GUID
        try:
            uuid.UUID(fias_id)
            return fias_id.upper()
        except (ValueError, AttributeError):
            # Если это не GUID, генерируем новый
            return generate_valid_guid()
    
    # Исправляем FIAS ID
    address_attrs = api_data['data']['relationships']['address']['data']['attributes']
    fias_fields = ['region_fias_id', 'city_fias_id', 'settlement_fias_id', 'area_fias_id', 'house_fias_id', 'street_fias_id']
    
    print("\nИсправление FIAS ID:")
    fixes_made = 0
    for field in fias_fields:
        old_value = address_attrs.get(field)
        new_value = validate_fias_id(old_value)
        
        if old_value != new_value:
            address_attrs[field] = new_value
            print(f"  {field}: {old_value} -> {new_value}")
            fixes_made += 1
        else:
            print(f"  {field}: {old_value} (без изменений)")
    
    print(f"\nИсправлено {fixes_made} FIAS ID")
    
    # Сохраняем исправленный файл
    fixed_file_path = file_path.parent / f"fixed_{file_path.name}"
    try:
        with open(fixed_file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        print(f"Исправленный файл сохранен: {fixed_file_path}")
        return fixed_file_path
    except Exception as e:
        print(f"Ошибка сохранения исправленного файла: {e}")
        return None

# ========== ФУНКЦИИ ДЛЯ ЗАГРУЗКИ ДАННЫХ ИЗ ФАЙЛОВ ==========

def load_data_from_file(file_path):
    """Загрузка данных из JSON файла"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        print(f"Успешно загружен файл: {file_path}")
        return data
    except Exception as e:
        print(f"Ошибка загрузки файла {file_path}: {e}")
        return None

def upload_single_file_to_api():
    """Загрузка данных из одного выбранного файла в API"""
    api_ready_dir = Path("api_ready")
    if not api_ready_dir.exists():
        print("Папка 'api_ready' не существует!")
        return
    
    # Получаем список JSON файлов
    json_files = list(api_ready_dir.glob("*.json"))
    if not json_files:
        print("В папке 'api_ready' нет JSON файлов!")
        return
    
    print("\nДоступные файлы для загрузки:")
    for i, file_path in enumerate(json_files, 1):
        print(f"{i}. {file_path.name}")
    
    try:
        choice = int(input("\nВыберите номер файла для загрузки: ")) - 1
        if 0 <= choice < len(json_files):
            selected_file = json_files[choice]
            print(f"Выбран файл: {selected_file.name}")
            
            # Дополнительные опции
            print("\nДополнительные опции:")
            print("1. Диагностика файла")
            print("2. Исправить FIAS ID и загрузить")
            print("3. Загрузить как есть")
            
            option = input("Выберите опцию (по умолчанию 3): ").strip()
            
            if option == "1":
                diagnose_existing_file(selected_file)
                return
            elif option == "2":
                fixed_file = fix_existing_file(selected_file)
                if fixed_file:
                    selected_file = fixed_file
                    print(f"Используется исправленный файл: {selected_file.name}")
            
            # Загружаем данные из файла
            data = load_data_from_file(selected_file)
            if data:
                # Проверяем авторизацию
                auth_token = ensure_auth()
                if not auth_token:
                    print("Ошибка авторизации! Невозможно загрузить данные.")
                    return
                
                # Запрашиваем user_id
                user_id = input(f"Введите ID пользователя (по умолчанию {TARGET_USER_ID}): ").strip()
                if not user_id:
                    user_id = TARGET_USER_ID
                else:
                    user_id = int(user_id)
                
                # Определяем структуру данных и отправляем в API
                if 'template' in data:
                    api_data = data['template']
                    print("Используется формат с метаданными (template)")
                else:
                    api_data = data
                    print("Используется прямой API формат")
                
                print("Отправка данных в API...")
                result = send_gas_object_to_api(api_data, user_id, auth_token)
                if result:
                    print("Данные успешно загружены в API!")
                    save_api_response(result, "response_single_object")
                else:
                    print("Ошибка загрузки данных в API!")
        else:
            print("Неверный выбор!")
    except ValueError:
        print("Пожалуйста, введите число!")
    except Exception as e:
        print(f"Ошибка при загрузке файла: {e}")

def upload_all_files_from_folder():
    """Загрузка всех файлов из папки api_ready в API"""
    api_ready_dir = Path("api_ready")
    if not api_ready_dir.exists():
        print("Папка 'api_ready' не существует!")
        return
    
    # Получаем список JSON файлов
    json_files = list(api_ready_dir.glob("*.json"))
    if not json_files:
        print("В папке 'api_ready' нет JSON файлов!")
        return
    
    print(f"Найдено {len(json_files)} файлов для загрузки:")
    for file_path in json_files:
        print(f"  - {file_path.name}")
    
    # Запрашиваем user_id
    user_id = input(f"Введите ID пользователя (по умолчанию {TARGET_USER_ID}): ").strip()
    if not user_id:
        user_id = TARGET_USER_ID
    else:
        user_id = int(user_id)
    
    # Проверяем авторизацию
    auth_token = ensure_auth()
    if not auth_token:
        print("Ошибка авторизации! Невозможно загрузить данные.")
        return
    
    success_count = 0
    error_count = 0
    
    for i, file_path in enumerate(json_files, 1):
        print(f"\n[{i}/{len(json_files)}] Загрузка файла: {file_path.name}")
        
        # Загружаем данные из файла
        data = load_data_from_file(file_path)
        if data:
            # Отправляем данные в API
            result = send_gas_object_to_api(data, user_id, auth_token)
            if result:
                success_count += 1
                print(f"Файл {file_path.name} успешно загружен")
                save_api_response(result, f"response_{file_path.stem}")
            else:
                error_count += 1
                print(f"Ошибка загрузки файла {file_path.name}")
                
            # Добавляем задержку между файлами
            if i < len(json_files):
                print("Ожидание 5 секунд перед следующим файлом...")
                time.sleep(5)
        else:
            error_count += 1
            print(f"Ошибка чтения файла {file_path.name}")
    
    print(f"\nИтоги загрузки:")
    print(f"Успешно: {success_count}")
    print(f"С ошибками: {error_count}")
    print(f"Всего: {len(json_files)}")

def upload_data_menu():
    """Меню выбора способа загрузки данных в API"""
    print("\n" + "=" * 60)
    print("ЗАГРУЗКА ДАННЫХ В API (СОЗДАНИЕ ОБЪЕКТОВ ГАЗИФИКАЦИИ)")
    print("=" * 60)
    
    while True:
        print("\nВыберите вариант загрузки:")
        print("1. Загрузить один конкретный файл")
        print("2. Загрузить все файлы из папки api_ready")
        print("3. Вернуться в главное меню")
        
        choice = input("\nВведите номер варианта: ").strip()
        
        if choice == "1":
            upload_single_file_to_api()
        elif choice == "2":
            upload_all_files_from_folder()
        elif choice == "3":
            break
        else:
            print("Неверный выбор! Пожалуйста, введите 1, 2 или 3.")

# ========== ФУНКЦИИ ДЛЯ СОХРАНЕНИЯ ДАННЫХ ==========

def convert_to_json_serializable(data):
    """Конвертируем данные в JSON-сериализуемый формат"""
    if isinstance(data, datetime):
        return data.isoformat()
    elif hasattr(data, 'isoformat'):
        return data.isoformat()
    return data

def save_api_response(response_data, filename_prefix):
    """Сохранение ответа от API для отладки"""
    try:
        output_dir = Path("api_responses")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.json"
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(response_data, file, ensure_ascii=False, indent=2)
        
        print(f"Ответ API сохранен: {filepath}")
        return True
        
    except Exception as e:
        print(f"Ошибка при сохранении ответа API: {e}")
        return False

def save_api_ready_data(data, filename_prefix="gas_objects_api_ready"):
    """Сохранение данных готовых для API в папку api_ready"""
    try:
        api_ready_dir = Path("api_ready")
        api_ready_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.json"
        filepath = api_ready_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        
        print(f"Данные готовые для API сохранены в файл: {filepath}")
        return True
        
    except Exception as e:
        print(f"Ошибка при сохранении данных для API: {e}")
        return False

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========

def main():
    print("=" * 60)
    print("СОЗДАНИЕ ОБЪЕКТОВ ГАЗИФИКАЦИИ ЧЕРЕЗ API АДМИНИСТРАТОРА")
    print("=" * 60)
    
    print("\nНастройка прокси...")
    setup_proxy()
    
    print("\nТестируем прокси соединение...")
    if not test_proxy_connection():
        print("Прокси не работает. Продолжаем без прокси?")
    
    # Основной цикл программы
    while True:
        print("\n" + "=" * 60)
        print("ГЛАВНОЕ МЕНЮ")
        print("=" * 60)
        print("1. Сбор данных из БД и создание объектов газификации")
        print("2. Загрузка данных в API")
        print("3. Тест авторизации")
        print("4. Выход")
        
        choice = input("\nВведите номер варианта: ").strip()
        
        if choice == "1":
            collect_and_prepare_data()
        elif choice == "2":
            upload_data_menu()
        elif choice == "3":
            test_auth_mode()
        elif choice == "4":
            print("Выход из программы...")
            break
        else:
            print("Неверный выбор! Пожалуйста, введите 1, 2, 3 или 4.")

def collect_and_prepare_data():
    """Режим сбора данных из БД и подготовки объектов газификации"""
    print("\n" + "=" * 60)
    print("СОЗДАНИЕ ОБЪЕКТОВ ГАЗИФИКАЦИИ ИЗ ДАННЫХ БД")
    print("=" * 60)
    
    # Получаем полные адресные данные для создания объектов
    print("\n1. Получаем данные для создания объектов газификации...")
    address_data = get_complete_address_data(limit=10)
    
    if address_data:
        print(f"Успешно получено {len(address_data)} адресных записей!")
        
        # Создаем объекты газификации в правильном формате
        gas_objects = create_gas_objects_from_db_data(address_data, limit=5)
        
        # Сохраняем подготовленные объекты в папку api_ready
        save_api_ready_data(gas_objects, "gas_objects_api_ready")
        
        # Выводим информацию о созданных объектах
        for i, gas_object in enumerate(gas_objects, 1):
            print(f"\n--- Объект {i} ---")
            print(f"Название: {gas_object['template']['data']['attributes']['name']}")
            print(f"Адрес: {gas_object['template']['data']['relationships']['address']['data']['attributes']['title']}")
        
        print(f"\nВсе объекты сохранены в папку 'api_ready' для последующей загрузки")
        
    else:
        print("Не удалось получить адресные данные для создания объектов")
    
    print("\n" + "=" * 60)
    print("ПОДГОТОВКА ДАННЫХ ЗАВЕРШЕНА!")
    print("=" * 60)
    print("\nДанные готовые для API сохранены в папку 'api_ready/'")
    print("Для загрузки данных в API используйте пункт меню 'Загрузка данных в API'")

def test_auth_mode():
    """Режим тестирования авторизации"""
    print("\n" + "=" * 60)
    print("ТЕСТ АВТОРИЗАЦИИ")
    print("=" * 60)
    
    token = get_auth_token()
    if token:
        print("Авторизация успешна!")
    else:
        print("Ошибка авторизации!")

if __name__ == "__main__":
    main()