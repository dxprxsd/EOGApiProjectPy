import json
import requests
import os
from pathlib import Path
from datetime import datetime
import time
import urllib3
import csv

# Программа для получения данных об объектах газификации через API администратора
# Использует GET /v1/admin/users/{user_id}/gas_objects

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
API_AUTH_TOKEN = None  
AUTH_RETRY_COUNT = 0
MAX_AUTH_RETRIES = 3

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

# ========== ФУНКЦИИ ДЛЯ ПОЛУЧЕНИЯ ДАННЫХ ОБ ОБЪЕКТАХ ГАЗИФИКАЦИИ ==========

def get_gas_objects_from_api(user_id, page=1, per_page=10, included="address", search_query=None):
    """Получение списка объектов газификации через GET API"""
    global AUTH_RETRY_COUNT
    
    # Формируем URL запроса
    url = f"{API_BASE_URL}/admin/users/{user_id}/gas_objects"
    
    # Параметры запроса
    params = {
        "page": page,
        "per": per_page,
        "included": included
    }
    
    # Добавляем параметр поиска если указан
    if search_query:
        params["query"] = search_query
    
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    if API_AUTH_TOKEN:
        headers["Authorization"] = API_AUTH_TOKEN
    
    try:
        print(f"Запрос объектов газификации для пользователя {user_id}...")
        print(f"URL: {url}")
        print(f"Параметры: {params}")
        
        response = requests.get(
            url,
            headers=headers,
            params=params,
            proxies={"http": PROXY_URL, "https": PROXY_URL},
            timeout=30,
            verify=False
        )
        
        print(f"Статус ответа: {response.status_code}")
        
        if response.status_code == 200:
            print("Данные об объектах газификации успешно получены!")
            AUTH_RETRY_COUNT = 0  # Сбрасываем счетчик при успешной операции
            return response.json()
            
        elif response.status_code == 401:
            print("Ошибка авторизации (401). Попытка обновить токен...")
            new_token = refresh_auth_token()
            if new_token:
                headers["Authorization"] = new_token
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    proxies={"http": PROXY_URL, "https": PROXY_URL},
                    timeout=30,
                    verify=False
                )
                if response.status_code == 200:
                    print("Данные успешно получены после обновления токена!")
                    return response.json()
                    
        elif response.status_code == 404:
            print("Пользователь или объекты не найдены (404)")
            return None
            
        elif response.status_code == 422:
            print("Ошибка валидации параметров (422):")
            try:
                error_data = response.json()
                print("Детали ошибки:")
                for error in error_data.get('errors', []):
                    print(f"  - {error.get('title')}: {error.get('detail')}")
            except:
                print(f"Текст ответа: {response.text}")
            return None
            
        elif response.status_code == 429:
            print("Превышен лимит запросов (429). Ожидание 60 секунд...")
            time.sleep(60)
            # Повторяем запрос после ожидания
            return get_gas_objects_from_api(user_id, page, per_page, included, search_query)
            
        else:
            print(f"Ошибка API: {response.status_code}")
            print(f"Ответ: {response.text[:500]}...")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сетевого запроса: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка при получении данных: {e}")
        return None

def get_all_gas_objects(user_id, per_page=50, included="address", search_query=None):
    """Получение всех объектов газификации с пагинацией"""
    all_objects = []
    current_page = 1
    included_data = []
    
    print(f"Начинаем сбор всех объектов газификации для пользователя {user_id}...")
    
    while True:
        print(f"Получение страницы {current_page}...")
        
        response_data = get_gas_objects_from_api(
            user_id=user_id,
            page=current_page,
            per_page=per_page,
            included=included,
            search_query=search_query
        )
        
        if not response_data:
            print(f"Ошибка при получении страницы {current_page}")
            break
        
        # Извлекаем данные об объектах
        objects_data = response_data.get('data', [])
        if not objects_data:
            print("Больше объектов нет")
            break
        
        # Добавляем объекты в общий список
        all_objects.extend(objects_data)
        
        # Сохраняем included данные (только с первой страницы для экономии места)
        if current_page == 1:
            included_data = response_data.get('included', [])
        
        # Проверяем пагинацию
        meta = response_data.get('meta', {})
        total_pages = meta.get('total_pages', 1)
        
        print(f"Получено {len(objects_data)} объектов со страницы {current_page}/{total_pages}")
        
        # Проверяем, есть ли следующая страница
        if current_page >= total_pages:
            print("Все страницы получены")
            break
        
        current_page += 1
        
        # Добавляем задержку между запросами чтобы не превысить лимиты
        time.sleep(1)
    
    print(f"Всего получено объектов: {len(all_objects)}")
    
    # Формируем полный ответ с included данными
    full_response = {
        "data": all_objects,
        "included": included_data,
        "meta": {
            "total_objects": len(all_objects),
            "retrieved_at": datetime.now().isoformat(),
            "user_id": user_id
        }
    }
    
    return full_response

# ========== ФУНКЦИИ ДЛЯ СОХРАНЕНИЯ ДАННЫХ ==========

def save_gas_objects_data(data, user_id, filename_prefix="gas_objects"):
    """Сохранение данных об объектах газификации в папку output/gas_objects_out"""
    try:
        output_dir = Path("output/gas_objects_out")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_user_{user_id}_{timestamp}.json"
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        
        print(f"Данные об объектах газификации сохранены в файл: {filepath}")
        return filepath
        
    except Exception as e:
        print(f"Ошибка при сохранении данных: {e}")
        return None

def convert_to_csv(data, user_id):
    """Конвертация данных об объектах газификации в CSV формат"""
    try:
        # Создаем папку для CSV если ее нет
        csv_dir = Path("output/gas_objects_out/csv_view")
        csv_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"gas_objects_user_{user_id}_{timestamp}.csv"
        csv_filepath = csv_dir / csv_filename
        
        if not data or 'data' not in data:
            print("Нет данных для конвертации в CSV")
            return None
        
        objects = data['data']
        included = data.get('included', [])
        
        # Создаем словарь адресов для быстрого доступа
        addresses = {addr['id']: addr for addr in included if addr['type'] == 'address'}
        
        # Определяем заголовки CSV
        fieldnames = [
            'object_id', 'object_name', 'user_id',
            'address_full', 'country', 'region', 'area', 'city', 'settlement',
            'street', 'house', 'block', 'flat', 'room', 'zip_code',
            'cadastral_number', 'oktmo', 'okato',
            'has_capital_construction', 'room_type',
            'region_fias_id', 'city_fias_id', 'settlement_fias_id',
            'area_fias_id', 'street_fias_id', 'house_fias_id'
        ]
        
        with open(csv_filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for obj in objects:
                row = {
                    'object_id': obj['id'],
                    'object_name': obj['attributes']['name'],
                    'user_id': user_id
                }
                
                # Добавляем информацию об адресе
                address_data = obj['relationships']['address']['data']
                if address_data and address_data['id'] in addresses:
                    address = addresses[address_data['id']]
                    attrs = address['attributes']
                    
                    row.update({
                        'address_full': attrs.get('title', ''),
                        'country': attrs.get('country', ''),
                        'region': attrs.get('region', ''),
                        'area': attrs.get('area', ''),
                        'city': attrs.get('city', ''),
                        'settlement': attrs.get('settlement', ''),
                        'street': attrs.get('street', ''),
                        'house': attrs.get('house', ''),
                        'block': attrs.get('block', ''),
                        'flat': attrs.get('flat', ''),
                        'room': attrs.get('room', ''),
                        'zip_code': attrs.get('zip_code', ''),
                        'cadastral_number': attrs.get('cadastral_number', ''),
                        'oktmo': attrs.get('oktmo', ''),
                        'okato': attrs.get('okato', ''),
                        'has_capital_construction': attrs.get('has_capital_construction', ''),
                        'room_type': attrs.get('room_type', ''),
                        'region_fias_id': attrs.get('region_fias_id', ''),
                        'city_fias_id': attrs.get('city_fias_id', ''),
                        'settlement_fias_id': attrs.get('settlement_fias_id', ''),
                        'area_fias_id': attrs.get('area_fias_id', ''),
                        'street_fias_id': attrs.get('street_fias_id', ''),
                        'house_fias_id': attrs.get('house_fias_id', '')
                    })
                else:
                    # Заполняем пустыми значениями если адреса нет
                    for field in fieldnames[3:]:  # Пропускаем первые три поля
                        if field not in row:
                            row[field] = ''
                
                writer.writerow(row)
        
        print(f"Данные конвертированы в CSV: {csv_filepath}")
        return csv_filepath
        
    except Exception as e:
        print(f"Ошибка при конвертации в CSV: {e}")
        return None

def create_consolidated_csv():
    """Создание объединенного CSV файла из всех JSON файлов в папке"""
    try:
        json_dir = Path("output/gas_objects_out")
        csv_dir = Path("output/gas_objects_out/csv_view")
        csv_dir.mkdir(parents=True, exist_ok=True)
        
        # Ищем все JSON файлы
        json_files = list(json_dir.glob("gas_objects_user_*.json"))
        if not json_files:
            print("Не найдено JSON файлов для консолидации")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        consolidated_csv = csv_dir / f"consolidated_gas_objects_{timestamp}.csv"
        
        all_rows = []
        fieldnames = set()
        
        for json_file in json_files:
            print(f"Обработка файла: {json_file.name}")
            
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if not data or 'data' not in data:
                    continue
                
                user_id = data.get('meta', {}).get('user_id', 'unknown')
                objects = data['data']
                included = data.get('included', [])
                addresses = {addr['id']: addr for addr in included if addr['type'] == 'address'}
                
                for obj in objects:
                    row = {
                        'object_id': obj['id'],
                        'object_name': obj['attributes']['name'],
                        'user_id': user_id,
                        'source_file': json_file.name
                    }
                    
                    # Добавляем информацию об адресе
                    address_data = obj['relationships']['address']['data']
                    if address_data and address_data['id'] in addresses:
                        address = addresses[address_data['id']]
                        attrs = address['attributes']
                        
                        address_fields = {
                            'address_full': attrs.get('title', ''),
                            'country': attrs.get('country', ''),
                            'region': attrs.get('region', ''),
                            'area': attrs.get('area', ''),
                            'city': attrs.get('city', ''),
                            'settlement': attrs.get('settlement', ''),
                            'street': attrs.get('street', ''),
                            'house': attrs.get('house', ''),
                            'block': attrs.get('block', ''),
                            'flat': attrs.get('flat', ''),
                            'room': attrs.get('room', ''),
                            'zip_code': attrs.get('zip_code', ''),
                            'cadastral_number': attrs.get('cadastral_number', ''),
                            'oktmo': attrs.get('oktmo', ''),
                            'okato': attrs.get('okato', ''),
                            'has_capital_construction': attrs.get('has_capital_construction', ''),
                            'room_type': attrs.get('room_type', ''),
                            'region_fias_id': attrs.get('region_fias_id', ''),
                            'city_fias_id': attrs.get('city_fias_id', ''),
                            'settlement_fias_id': attrs.get('settlement_fias_id', ''),
                            'area_fias_id': attrs.get('area_fias_id', ''),
                            'street_fias_id': attrs.get('street_fias_id', ''),
                            'house_fias_id': attrs.get('house_fias_id', '')
                        }
                        
                        row.update(address_fields)
                    
                    all_rows.append(row)
                    fieldnames.update(row.keys())
                    
            except Exception as e:
                print(f"Ошибка при обработке файла {json_file.name}: {e}")
                continue
        
        if not all_rows:
            print("Нет данных для создания консолидированного CSV")
            return None
        
        # Записываем все данные в CSV
        with open(consolidated_csv, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
            writer.writeheader()
            writer.writerows(all_rows)
        
        print(f"Создан консолидированный CSV файл: {consolidated_csv}")
        print(f"Всего записей: {len(all_rows)}")
        print(f"Всего файлов обработано: {len(json_files)}")
        
        return consolidated_csv
        
    except Exception as e:
        print(f"Ошибка при создании консолидированного CSV: {e}")
        return None

def display_gas_objects_summary(data, user_id):
    """Вывод сводной информации об объектах газификации"""
    if not data or 'data' not in data:
        print("Нет данных для отображения")
        return
    
    objects = data['data']
    included = data.get('included', [])
    
    print(f"\n" + "=" * 80)
    print(f"СВОДКА ПО ОБЪЕКТАМ ГАЗИФИКАЦИИ (Пользователь ID: {user_id})")
    print("=" * 80)
    print(f"Всего объектов: {len(objects)}")
    
    # Создаем словарь адресов для быстрого доступа
    addresses = {addr['id']: addr for addr in included if addr['type'] == 'address'}
    
    # Выводим информацию по каждому объекту
    for i, obj in enumerate(objects, 1):
        print(f"\n--- Объект {i} ---")
        print(f"ID объекта: {obj['id']}")
        print(f"Название: {obj['attributes']['name']}")
        
        # Получаем информацию об адресе
        address_data = obj['relationships']['address']['data']
        if address_data and address_data['id'] in addresses:
            address = addresses[address_data['id']]
            attrs = address['attributes']
            print(f"Адрес: {attrs.get('title', 'N/A')}")
            print(f"  Регион: {attrs.get('region', 'N/A')}")
            print(f"  Город/населенный пункт: {attrs.get('city', 'N/A')}")
            print(f"  Улица: {attrs.get('street', 'N/A')}")
            print(f"  Дом: {attrs.get('house', 'N/A')}")
        else:
            print("Адрес: не указан")
    
    print(f"\n" + "=" * 80)

def save_detailed_report(data, user_id):
    """Сохранение детального отчета в текстовом формате"""
    try:
        output_dir = Path("output/gas_objects_out")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"detailed_report_user_{user_id}_{timestamp}.txt"
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write("=" * 80 + "\n")
            file.write(f"ДЕТАЛЬНЫЙ ОТЧЕТ ПО ОБЪЕКТАМ ГАЗИФИКАЦИИ\n")
            file.write(f"Пользователь ID: {user_id}\n")
            file.write(f"Дата формирования: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            file.write("=" * 80 + "\n\n")
            
            if not data or 'data' not in data:
                file.write("Нет данных об объектах газификации\n")
                return filepath
            
            objects = data['data']
            included = data.get('included', [])
            addresses = {addr['id']: addr for addr in included if addr['type'] == 'address'}
            
            file.write(f"Всего объектов: {len(objects)}\n\n")
            
            for i, obj in enumerate(objects, 1):
                file.write(f"ОБЪЕКТ {i}\n")
                file.write(f"ID: {obj['id']}\n")
                file.write(f"Название: {obj['attributes']['name']}\n")
                
                # Информация об адресе
                address_data = obj['relationships']['address']['data']
                if address_data and address_data['id'] in addresses:
                    address = addresses[address_data['id']]
                    attrs = address['attributes']
                    file.write("АДРЕС:\n")
                    file.write(f"  Полный адрес: {attrs.get('title', 'N/A')}\n")
                    file.write(f"  Страна: {attrs.get('country', 'N/A')}\n")
                    file.write(f"  Регион: {attrs.get('region', 'N/A')}\n")
                    file.write(f"  Город: {attrs.get('city', 'N/A')}\n")
                    file.write(f"  Населенный пункт: {attrs.get('settlement', 'N/A')}\n")
                    file.write(f"  Район: {attrs.get('area', 'N/A')}\n")
                    file.write(f"  Улица: {attrs.get('street', 'N/A')}\n")
                    file.write(f"  Дом: {attrs.get('house', 'N/A')}\n")
                    file.write(f"  Корпус/строение: {attrs.get('block', 'N/A')}\n")
                    file.write(f"  Квартира: {attrs.get('flat', 'N/A')}\n")
                    file.write(f"  Помещение: {attrs.get('room', 'N/A')}\n")
                    file.write(f"  Индекс: {attrs.get('zip_code', 'N/A')}\n")
                    file.write(f"  Кадастровый номер: {attrs.get('cadastral_number', 'N/A')}\n")
                    file.write(f"  ОКТМО: {attrs.get('oktmo', 'N/A')}\n")
                else:
                    file.write("АДРЕС: не указан\n")
                
                file.write("-" * 50 + "\n\n")
        
        print(f"Детальный отчет сохранен: {filepath}")
        return filepath
        
    except Exception as e:
        print(f"Ошибка при сохранении детального отчета: {e}")
        return None

# ========== ОСНОВНАЯ ФУНКЦИОНАЛЬНОСТЬ ==========

def get_gas_objects_for_user():
    """Получение объектов газификации для конкретного пользователя"""
    print("\n" + "=" * 60)
    print("ПОЛУЧЕНИЕ ОБЪЕКТОВ ГАЗИФИКАЦИИ ДЛЯ ПОЛЬЗОВАТЕЛЯ")
    print("=" * 60)
    
    # Запрашиваем ID пользователя
    try:
        user_id = input("Введите ID пользователя: ").strip()
        if not user_id:
            print("ID пользователя не может быть пустым!")
            return
        
        user_id = int(user_id)
    except ValueError:
        print("ID пользователя должен быть числом!")
        return
    
    # Настройки запроса
    per_page = input("Количество объектов на странице (по умолчанию 50): ").strip()
    per_page = int(per_page) if per_page.isdigit() else 50
    
    search_query = input("Поисковый запрос (опционально, Enter чтобы пропустить): ").strip()
    if not search_query:
        search_query = None
    
    # Проверяем авторизацию
    auth_token = ensure_auth()
    if not auth_token:
        print("Ошибка авторизации! Невозможно получить данные.")
        return
    
    print(f"\nНачинаем получение объектов газификации для пользователя {user_id}...")
    
    # Получаем все объекты
    all_objects_data = get_all_gas_objects(
        user_id=user_id,
        per_page=per_page,
        included="address",
        search_query=search_query
    )
    
    if not all_objects_data or not all_objects_data['data']:
        print("Не удалось получить данные об объектах газификации или объекты отсутствуют")
        return
    
    # Выводим сводную информацию
    display_gas_objects_summary(all_objects_data, user_id)
    
    # Сохраняем данные в JSON
    json_file = save_gas_objects_data(all_objects_data, user_id)
    
    # Конвертируем в CSV
    csv_file = convert_to_csv(all_objects_data, user_id)
    
    # Сохраняем детальный отчет
    report_file = save_detailed_report(all_objects_data, user_id)
    
    print(f"\nРезультаты сохранены в папке 'output/gas_objects_out':")
    if json_file:
        print(f"  - JSON данные: {json_file.name}")
    if csv_file:
        print(f"  - CSV файл: csv_view/{csv_file.name}")
    if report_file:
        print(f"  - Детальный отчет: {report_file.name}")

def batch_get_gas_objects():
    """Получение объектов газификации для нескольких пользователей"""
    print("\n" + "=" * 60)
    print("ПАКЕТНОЕ ПОЛУЧЕНИЕ ОБЪЕКТОВ ГАЗИФИКАЦИИ")
    print("=" * 60)
    
    # Запрашиваем список ID пользователей
    user_ids_input = input("Введите ID пользователей через запятую: ").strip()
    if not user_ids_input:
        print("Не указаны ID пользователей!")
        return
    
    try:
        user_ids = [int(uid.strip()) for uid in user_ids_input.split(',')]
    except ValueError:
        print("Все ID должны быть числами!")
        return
    
    print(f"Будет обработано пользователей: {len(user_ids)}")
    
    # Проверяем авторизацию
    auth_token = ensure_auth()
    if not auth_token:
        print("Ошибка авторизации! Невозможно получить данные.")
        return
    
    results = []
    
    for i, user_id in enumerate(user_ids, 1):
        print(f"\n[{i}/{len(user_ids)}] Обработка пользователя {user_id}...")
        
        # Получаем объекты для текущего пользователя
        user_objects_data = get_all_gas_objects(
            user_id=user_id,
            per_page=50,
            included="address"
        )
        
        if user_objects_data and user_objects_data['data']:
            # Сохраняем отдельный файл для каждого пользователя
            save_gas_objects_data(user_objects_data, user_id, f"user_{user_id}_gas_objects")
            convert_to_csv(user_objects_data, user_id)
            save_detailed_report(user_objects_data, user_id)
            
            # Добавляем в общие результаты
            results.append({
                "user_id": user_id,
                "objects_count": len(user_objects_data['data']),
                "retrieved_at": datetime.now().isoformat()
            })
            
            print(f"Пользователь {user_id}: получено {len(user_objects_data['data'])} объектов")
        else:
            print(f"Пользователь {user_id}: объекты не найдены или ошибка получения")
            results.append({
                "user_id": user_id,
                "objects_count": 0,
                "error": "Объекты не найдены или ошибка получения",
                "retrieved_at": datetime.now().isoformat()
            })
        
        # Задержка между запросами для разных пользователей
        if i < len(user_ids):
            print("Ожидание 3 секунды перед следующим пользователем...")
            time.sleep(3)
    
    # Сохраняем сводный отчет
    try:
        output_dir = Path("output/gas_objects_out")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_file = output_dir / f"batch_summary_{timestamp}.json"
        
        summary = {
            "batch_processed_at": datetime.now().isoformat(),
            "total_users": len(user_ids),
            "results": results
        }
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\nСводный отчет сохранен: {summary_file}")
        
        # Создаем консолидированный CSV
        print("Создание консолидированного CSV файла...")
        consolidated_csv = create_consolidated_csv()
        if consolidated_csv:
            print(f"Консолидированный CSV создан: {consolidated_csv}")
        
        # Выводим итоги
        successful = sum(1 for r in results if r.get('objects_count', 0) > 0)
        total_objects = sum(r.get('objects_count', 0) for r in results)
        
        print(f"\nИТОГИ ПАКЕТНОЙ ОБРАБОТКИ:")
        print(f"Обработано пользователей: {len(user_ids)}")
        print(f"Успешно: {successful}")
        print(f"Всего объектов получено: {total_objects}")
        
    except Exception as e:
        print(f"Ошибка при сохранении сводного отчета: {e}")

def create_csv_from_existing_files():
    """Создание CSV файлов из существующих JSON файлов"""
    print("\n" + "=" * 60)
    print("СОЗДАНИЕ CSV ИЗ СУЩЕСТВУЮЩИХ JSON ФАЙЛОВ")
    print("=" * 60)
    
    print("1. Создать отдельные CSV для каждого JSON файла")
    print("2. Создать консолидированный CSV из всех JSON файлов")
    print("3. Создать и отдельные и консолидированный CSV")
    
    choice = input("\nВыберите вариант (по умолчанию 3): ").strip()
    
    if choice == "1":
        create_individual_csv_files()
    elif choice == "2":
        create_consolidated_csv()
    else:
        create_individual_csv_files()
        create_consolidated_csv()

def create_individual_csv_files():
    """Создание отдельных CSV файлов для каждого JSON файла"""
    try:
        json_dir = Path("output/gas_objects_out")
        if not json_dir.exists():
            print("Папка output/gas_objects_out не существует!")
            return
        
        json_files = list(json_dir.glob("gas_objects_user_*.json"))
        if not json_files:
            print("Не найдено JSON файлов для конвертации")
            return
        
        print(f"Найдено {len(json_files)} JSON файлов для конвертации")
        
        for json_file in json_files:
            print(f"Обработка файла: {json_file.name}")
            
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Извлекаем user_id из meta или из имени файла
                user_id = data.get('meta', {}).get('user_id')
                if not user_id:
                    # Пытаемся извлечь из имени файла
                    import re
                    match = re.search(r'user_(\d+)', json_file.name)
                    if match:
                        user_id = match.group(1)
                    else:
                        user_id = 'unknown'
                
                csv_file = convert_to_csv(data, user_id)
                if csv_file:
                    print(f"  -> Создан CSV: {csv_file.name}")
                    
            except Exception as e:
                print(f"  Ошибка при обработке файла {json_file.name}: {e}")
                continue
        
        print(f"\nКонвертация завершена! Создано CSV файлов: {len([f for f in json_files if f])}")
        
    except Exception as e:
        print(f"Ошибка при создании CSV файлов: {e}")

# ========== ГЛАВНОЕ МЕНЮ ==========

def main():
    print("=" * 60)
    print("ПОЛУЧЕНИЕ ДАННЫХ ОБ ОБЪЕКТАХ ГАЗИФИКАЦИИ ЧЕРЕЗ API")
    print("=" * 60)
    
    print("\nНастройка прокси...")
    setup_proxy()
    
    print("\nТестируем прокси соединение...")
    if not test_proxy_connection():
        print("Прокси не работает. Продолжаем без прокси?")
        response = input("Продолжить без прокси? (y/N): ").strip().lower()
        if response != 'y':
            return
    
    # Основной цикл программы
    while True:
        print("\n" + "=" * 60)
        print("ГЛАВНОЕ МЕНЮ - ПОЛУЧЕНИЕ ДАННЫХ ОБ ОБЪЕКТАХ ГАЗИФИКАЦИИ")
        print("=" * 60)
        print("1. Получить объекты газификации для одного пользователя")
        print("2. Пакетное получение объектов для нескольких пользователей")
        print("3. Создать CSV из существующих JSON файлов")
        print("4. Тест авторизации")
        print("5. Выход")
        
        choice = input("\nВведите номер варианта: ").strip()
        
        if choice == "1":
            get_gas_objects_for_user()
        elif choice == "2":
            batch_get_gas_objects()
        elif choice == "3":
            create_csv_from_existing_files()
        elif choice == "4":
            test_auth()
        elif choice == "5":
            print("Выход из программы...")
            break
        else:
            print("Неверный выбор! Пожалуйста, введите 1, 2, 3, 4 или 5.")

def test_auth():
    """Тестирование авторизации"""
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