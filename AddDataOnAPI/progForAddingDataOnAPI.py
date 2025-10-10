import json
import requests
import socket
from pathlib import Path
from datetime import datetime
import os
import urllib3
import pymssql

# Программа для получения данных из БД и добавления данных в API 

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
API_AUTH_TOKEN = None  # Задайте токен авторизации если требуется

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

def ensure_auth():
    """Обеспечивает наличие действительного токена авторизации"""
    global API_AUTH_TOKEN
    if not API_AUTH_TOKEN:
        return get_auth_token()
    return API_AUTH_TOKEN

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С API ==========

def get_appeals_categories():
    """Получение категорий обращений через прокси"""
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    params = {"page": 1, "per": 10}
    url = "https://tpsg.etpgpb.ru/v1/appeals/categories"
    
    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            proxies={"http": PROXY_URL, "https": PROXY_URL},
            timeout=15,
            verify=False
        )
        
        print(f"Статус API категорий: {response.status_code}")
        if response.status_code == 200:
            print("Успешно получены категории обращений!")
            return response.json()
        else:
            print(f"Ответ сервера: {response.status_code}")
            print(f"Текст ответа: {response.text[:200]}...")
            return None
    except Exception as e:
        print(f"Ошибка при запросе категорий: {e}")
        return None

def get_appeals_subjects(category_id=3, page=1, per=10):
    """Получение тем обращений для указанной категории"""
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    params = {"page": page, "per": per}
    url = f"https://tpsg.etpgpb.ru/v1/appeals/categories/{category_id}/subjects"
    
    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            proxies={"http": PROXY_URL, "https": PROXY_URL},
            timeout=15,
            verify=False
        )
        
        print(f"Статус API тем: {response.status_code}")
        if response.status_code == 200:
            print("Успешно получены темы обращений!")
            return response.json()
        else:
            print(f"Ответ сервера: {response.status_code}")
            print(f"Текст ответа: {response.text[:200]}...")
            return None
    except Exception as e:
        print(f"Ошибка при запросе тем обращений: {e}")
        return None

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

def get_gez_data(limit=1000):
    """Получаем данные из таблицы gez"""
    conn = get_sql_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # SQL запрос для получения данных
        query = f"""
        SELECT TOP ({limit})
            [ind], [num], [Dat], [PROEKT], [ZAKAZ], [ADRES], [DATA], [prisoed], [davl], [gazosnab],
            [numpr], [d], [harakter], [len], [isp], [p], [s], [dopl], [dv], [tu], [r], [dvtp], [prtype],
            [el], [kor_povr], [agr_gr], [ist_tok], [prinadl], [pom], [dadd], [id_zvk], [rsh_kub], [rsh_tut],
            [grs], [rsh_mln], [obl], [mat], [rsh_tsm], [upd_us], [dvtup], [prl], [selo], [n_zhurn], [vvod],
            [l_type], [adm], [num2], [isp2], [prinadl_txt], [p1314], [prokl], [d_rsch], [mat_rsch], [d_m1],
            [d_m2], [p2_obj], [p3_obj], [str_obj], [dom_obj], [korpus_obj], [adr_txt_obj], [p2_gp], [p3_gp],
            [str_gp], [dom_gp], [korpus_gp], [adr_txt_gp], [tu_status], [tu_kat], [prokl1], [srok2], [isp1],
            [tu1], [gzpr_net], [komm], [dogno], [lg_vrz], [prg_d], [prg_prkl], [prg_dvl], [ehztu], [kat_z],
            [ind_zd], [rsh_curr], [in_home], [snt], [ravetti], [id_uch], [rsh_add], [dvtp2], [vn_uch], [vn_grp],
            [cpr_no], [pometka_s], [isp_pr], [s_vn], [ds_dogaz], [cgno], [coksno], [cgiono], [dat_pr], [date_upd],
            [kmpl], [vn_no], [kub_mlp], [pu_kol], [tmp], [sezon], [kat_ptr], [dat_rsch], [num_1tu], [rsh_dem], [grs_out1]
        FROM [gez].[dbo].[gez]
        """
        
        print("Выполняем запрос к базе данных...")
        cursor.execute(query)
        
        # Получаем все результаты
        results = cursor.fetchall()
        
        print(f"Получено {len(results)} записей из таблицы gez")
        return results
            
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None
    finally:
        conn.close()

# ========== ФУНКЦИИ ДЛЯ СОЗДАНИЯ ОБЪЕКТОВ ГАЗИФИКАЦИИ ==========

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
    """Подготовка данных для создания объекта газификации в формате API"""
    
    # Формируем название объекта
    object_name = row['object_name'] or f"Объект {row['house_number']}"
    
    # Формируем полное название улицы
    street_full = f"{row.get('street_type', 'ул')} {row['street_name']}" if row.get('street_name') else "ул. Не указана"
    
    # Формируем заголовок адреса
    address_title = f"{row['region_name']}, {street_full} {row['house_number']}"
    
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
                            "area": row.get('region_name', ''),  # Используем регион как район
                            "zip_code": row.get('zip_code', 0),
                            "street": street_full,
                            "house": row['house_number'],
                            "block": row.get('block'),
                            "flat": 1,  # Значение по умолчанию
                            "room": None,
                            "region_fias_id": f"region_{row.get('region_code', '')}",
                            "city_fias_id": row.get('settlement_fias_id', ''),
                            "settlement_fias_id": row.get('settlement_fias_id', ''),
                            "area_fias_id": row.get('settlement_fias_id', ''),  # Используем settlement_fias_id
                            "house_fias_id": row.get('settlement_fias_id', ''),  # Используем settlement_fias_id
                            "street_fias_id": row.get('street_fias_id'),
                            "extra": row.get('full_address', ''),
                            "oktmo": None,
                            "cadastral_number": None,
                            "cadastral_home_number": None,
                            "okato": None,
                            "title": address_title,
                            "has_capital_construction": True,
                            "room_type": "apartment_building"
                        }
                    }
                }
            }
        }
    }
    
    # Обработка числовых значений
    try:
        if gas_object_data["data"]["relationships"]["address"]["data"]["attributes"]["zip_code"]:
            gas_object_data["data"]["relationships"]["address"]["data"]["attributes"]["zip_code"] = int(
                gas_object_data["data"]["relationships"]["address"]["data"]["attributes"]["zip_code"]
            )
    except (ValueError, TypeError):
        gas_object_data["data"]["relationships"]["address"]["data"]["attributes"]["zip_code"] = 0
    
    try:
        if gas_object_data["data"]["relationships"]["address"]["data"]["attributes"]["house"]:
            gas_object_data["data"]["relationships"]["address"]["data"]["attributes"]["house"] = int(
                gas_object_data["data"]["relationships"]["address"]["data"]["attributes"]["house"]
            )
    except (ValueError, TypeError):
        # Если не удалось преобразовать в число, оставляем как строку
        pass
    
    # Очистка None значений для числовых полей
    numeric_fields = ["oktmo", "flat", "room"]
    for field in numeric_fields:
        if gas_object_data["data"]["relationships"]["address"]["data"]["attributes"][field] is None:
            gas_object_data["data"]["relationships"]["address"]["data"]["attributes"][field] = 0
    
    return gas_object_data


def send_gas_object_to_api(gas_object_data, auth_token=None):
    """Отправка данных об объекте газификации в API"""
    url = f"{API_BASE_URL}/gas_objects"
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    if auth_token:
        headers["Authorization"] = auth_token
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json=gas_object_data,
            proxies={"http": PROXY_URL, "https": PROXY_URL},
            timeout=30,
            verify=False
        )
        
        print(f"Статус отправки: {response.status_code}")
        
        if response.status_code in [200, 201]:
            print("Объект успешно создан!")
            return response.json()
        elif response.status_code == 401:
            print("Ошибка авторизации. Попытка обновить токен...")
            new_token = refresh_auth_token()
            if new_token:
                headers["Authorization"] = new_token
                response = requests.post(
                    url,
                    headers=headers,
                    json=gas_object_data,
                    proxies={"http": PROXY_URL, "https": PROXY_URL},
                    timeout=30,
                    verify=False
                )
                if response.status_code in [200, 201]:
                    print("Объект успешно создан после обновления токена!")
                    return response.json()
        else:
            print(f"Ошибка: {response.status_code}")
            print(f"Ответ: {response.text[:200]}...")
            return None
            
    except Exception as e:
        print(f"Ошибка при отправке в API: {e}")
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
    output_dir = Path("output")
    if not output_dir.exists():
        print("Папка 'output' не существует!")
        return
    
    # Получаем список JSON файлов
    json_files = list(output_dir.glob("*.json"))
    if not json_files:
        print("В папке 'output' нет JSON файлов!")
        return
    
    print("\nДоступные файлы для загрузки:")
    for i, file_path in enumerate(json_files, 1):
        print(f"{i}. {file_path.name}")
    
    try:
        choice = int(input("\nВыберите номер файла для загрузки: ")) - 1
        if 0 <= choice < len(json_files):
            selected_file = json_files[choice]
            print(f"Выбран файл: {selected_file.name}")
            
            # Загружаем данные из файла
            data = load_data_from_file(selected_file)
            if data:
                # Проверяем авторизацию
                auth_token = ensure_auth()
                if not auth_token:
                    print("Ошибка авторизации! Невозможно загрузить данные.")
                    return
                
                # Определяем формат данных и отправляем в API
                if isinstance(data, list):
                    # Если это список объектов
                    print(f"Найдено {len(data)} объектов для загрузки...")
                    success_count = 0
                    
                    for i, item in enumerate(data, 1):
                        print(f"[{i}/{len(data)}] Загрузка объекта...")
                        
                        # Определяем структуру данных
                        if 'template' in item:
                            # Формат с метаданными - используем только template
                            api_data = item['template']
                        else:
                            # Прямой формат API
                            api_data = item
                        
                        result = send_gas_object_to_api(api_data, auth_token)
                        if result:
                            success_count += 1
                            print(f"✓ Объект {i} успешно загружен")
                        else:
                            print(f"✗ Ошибка загрузки объекта {i}")
                    
                    print(f"\nИтоги: Успешно {success_count}/{len(data)}")
                    
                else:
                    # Если это одиночный объект
                    if 'template' in data:
                        # Формат с метаданными - используем только template
                        api_data = data['template']
                    else:
                        # Прямой формат API
                        api_data = data
                    
                    print("Отправка данных в API...")
                    result = send_gas_object_to_api(api_data, auth_token)
                    if result:
                        print("Данные успешно загружены в API!")
                    else:
                        print("Ошибка загрузки данных в API!")
        else:
            print("Неверный выбор!")
    except ValueError:
        print("Пожалуйста, введите число!")
    except Exception as e:
        print(f"Ошибка при загрузке файла: {e}")

def upload_all_files_from_folder():
    """Загрузка всех файлов из папки output в API"""
    output_dir = Path("output")
    if not output_dir.exists():
        print("Папка 'output' не существует!")
        return
    
    # Получаем список JSON файлов
    json_files = list(output_dir.glob("*.json"))
    if not json_files:
        print("В папке 'output' нет JSON файлов!")
        return
    
    print(f"Найдено {len(json_files)} файлов для загрузки:")
    for file_path in json_files:
        print(f"  - {file_path.name}")
    
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
            result = send_gas_object_to_api(data, auth_token)
            if result:
                success_count += 1
                print(f"Файл {file_path.name} успешно загружен")
            else:
                error_count += 1
                print(f"Ошибка загрузки файла {file_path.name}")
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
    print("ЗАГРУЗКА ДАННЫХ В API")
    print("=" * 60)
    
    while True:
        print("\nВыберите вариант загрузки:")
        print("1. Загрузить один конкретный файл")
        print("2. Загрузить все файлы из папки output")
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

def save_api_categories_to_file(data, filename="api_categories.txt"):
    """Сохранение данных о категориях из API в файл"""
    try:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write("КАРТОЧКИ КАТЕГОРИЙ ОБРАЩЕНИЙ (API)\n")
            file.write("=" * 60 + "\n")
            file.write(f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n")
            file.write("=" * 60 + "\n\n")
            
            if data and 'data' in data:
                for i, category in enumerate(data['data'], 1):
                    attributes = category.get('attributes', {})
                    
                    file.write(f"КАРТОЧКА #{i}\n")
                    file.write("-" * 40 + "\n")
                    file.write(f"ID: {attributes.get('id', 'N/A')}\n")
                    file.write(f"Название: {attributes.get('name', 'N/A')}\n")
                    file.write(f"Slug: {attributes.get('slug', 'N/A')}\n")
                    file.write(f"Внешний ID: {attributes.get('external_id', 'N/A')}\n")
                    file.write(f"Тип показа: {attributes.get('shown_for_kind', 'N/A')}\n")
                    file.write(f"Активна: {'Да' if attributes.get('active') else 'Нет'}\n")
                    file.write("\n")
                
                file.write("=" * 60 + "\n")
                file.write(f"Всего категорий: {len(data['data'])}\n")
            else:
                file.write("Данные о категориях не получены\n")
        
        print(f"Файл сохранен: {filepath}")
        return True
        
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")
        return False

def save_api_subjects_to_file(data, filename="api_subjects.txt"):
    """Сохранение данных о темах обращений из API в файл"""
    try:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write("ТЕМЫ ОБРАЩЕНИЙ (API)\n")
            file.write("=" * 50 + "\n")
            file.write(f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n")
            file.write("=" * 50 + "\n\n")
            
            if data and 'data' in data:
                for i, subject in enumerate(data['data'], 1):
                    attributes = subject.get('attributes', {})
                    
                    file.write(f"ТЕМА #{i}\n")
                    file.write("-" * 30 + "\n")
                    file.write(f"ID: {attributes.get('id', 'N/A')}\n")
                    file.write(f"Название: {attributes.get('name', 'N/A')}\n")
                    file.write(f"Slug: {attributes.get('slug', 'N/A')}\n")
                    file.write(f"Внешний ID: {attributes.get('external_id', 'N/A')}\n")
                    file.write(f"Активна: {'Да' if attributes.get('active') else 'Нет'}\n")
                    file.write("\n")
                
                file.write("=" * 50 + "\n")
                file.write(f"Всего тем: {len(data['data'])}\n")
            else:
                file.write("Данные о темах обращений не получены\n")
        
        print(f"Файл сохранен: {filepath}")
        return True
        
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")
        return False

def save_db_data_to_file(data, filename="db_data.json"):
    """Сохранение данных из БД в JSON файл"""
    try:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        # Конвертируем данные в JSON-сериализуемый формат
        serializable_data = []
        for record in data:
            serializable_record = {}
            for key, value in record.items():
                serializable_record[key] = convert_to_json_serializable(value)
            serializable_data.append(serializable_record)
        
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(serializable_data, file, ensure_ascii=False, indent=2, default=str)
        
        print(f"Данные из БД сохранены в файл: {filepath}")
        return True
        
    except Exception as e:
        print(f"Ошибка при сохранении данных БД: {e}")
        return False

def save_raw_json_to_file(data, filename_prefix="raw"):
    """Сохранение сырых JSON данных в файл без парсинга"""
    try:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.json"
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        
        print(f"Сырые JSON данные сохранены в файл: {filepath}")
        return True
        
    except Exception as e:
        print(f"Ошибка при сохранении сырых данных: {e}")
        return False

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========

def main():
    print("=" * 60)
    print("ЗАПУСК СБОРЩИКА ДАННЫХ ИЗ API И БАЗЫ ДАННЫХ")
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
        print("1. Сбор данных из API и БД")
        print("2. Загрузка данных в API")
        print("3. Тест авторизации")
        print("4. Выход")
        
        choice = input("\nВведите номер варианта: ").strip()
        
        if choice == "1":
            collect_data_mode()
        elif choice == "2":
            upload_data_menu()
        elif choice == "3":
            test_auth_mode()
        elif choice == "4":
            print("Выход из программы...")
            break
        else:
            print("Неверный выбор! Пожалуйста, введите 1, 2, 3 или 4.")

def collect_data_mode():
    """Режим сбора данных из API и БД"""
    print("\n" + "=" * 60)
    print("ПОЛУЧЕНИЕ ДАННЫХ ИЗ API")
    print("=" * 60)
    
    # Получаем категории обращений из API
    print("\n1. Получаем категории обращений из API...")
    categories_data = get_appeals_categories()
    
    if categories_data:
        print(f"Успешно получено {len(categories_data.get('data', []))} категорий!")
        save_api_categories_to_file(categories_data, "api_categories.txt")
        save_raw_json_to_file(categories_data, "api_categories_raw")
        
        # Выводим краткую информацию
        print("\nПолученные категории:")
        for cat in categories_data.get('data', []):
            attrs = cat.get('attributes', {})
            print(f"- {attrs.get('name', 'N/A')} (ID: {attrs.get('id', 'N/A')})")
        
        # Получаем темы обращений для первой категории
        if categories_data.get('data'):
            first_category_id = categories_data['data'][0].get('attributes', {}).get('id')
            if first_category_id:
                print(f"\n2. Получаем темы обращений для категории ID: {first_category_id}...")
                subjects_data = get_appeals_subjects(category_id=first_category_id, page=1, per=50)
                
                if subjects_data:
                    print(f"Успешно получено {len(subjects_data.get('data', []))} тем обращений!")
                    save_api_subjects_to_file(subjects_data, f"api_subjects_category_{first_category_id}.txt")
                    save_raw_json_to_file(subjects_data, f"api_subjects_raw_{first_category_id}")
    else:
        print("Не удалось получить данные категорий от API")
    
    print("\n" + "=" * 60)
    print("ПОЛУЧЕНИЕ ДАННЫХ ИЗ БАЗЫ ДАННЫХ")
    print("=" * 60)
    
    # Получаем данные из базы данных
    print("\n3. Получаем данные из таблицы gez...")
    gez_data = get_gez_data(limit=1000)
    
    if gez_data:
        print(f"Успешно получено {len(gez_data)} записей из БД!")
        save_db_data_to_file(gez_data, "db_gez_data.json")
        save_raw_json_to_file(gez_data, "db_gez_raw")
    
    print("\n" + "=" * 60)
    print("СОЗДАНИЕ ОБЪЕКТОВ ГАЗИФИКАЦИИ")
    print("=" * 60)
    
    # Получаем полные адресные данные для создания объектов
    print("\n4. Получаем данные для создания объектов газификации...")
    address_data = get_complete_address_data(limit=10)
    
    if address_data:
        print(f"Успешно получено {len(address_data)} адресных записей!")
        
        # Создаем объекты газификации в правильном формате
        gas_objects = create_gas_objects_from_db_data(address_data, limit=5)
        
        # Сохраняем подготовленные объекты
        for i, gas_object in enumerate(gas_objects, 1):
            print(f"\n--- Объект {i} ---")
            print(f"Название: {gas_object['template']['data']['attributes']['name']}")
            print(f"Адрес: {gas_object['template']['data']['relationships']['address']['data']['attributes']['title']}")
            
            # Сохраняем для отладки
            save_raw_json_to_file(gas_object, f"gas_object_formatted_{i}")
            
            # Для реальной отправки используем только template
            api_data = gas_object['template']
            
            # РАСКОММЕНТИРУЙТЕ ДЛЯ РЕАЛЬНОЙ ОТПРАВКИ:
            # print("Отправляем данные в API...")
            # result = send_gas_object_to_api(api_data, API_AUTH_TOKEN)
            # if result:
            #     print("Успешно отправлено в API")
            # else:
            #     print("Ошибка отправки")
        
        # Сохраняем все объекты в один файл
        save_raw_json_to_file(gas_objects, "all_gas_objects_formatted")
        
    else:
        print("Не удалось получить адресные данные для создания объектов")
    
    print("\n" + "=" * 60)
    print("СБОР ДАННЫХ ЗАВЕРШЕН!")
    print("=" * 60)
    print("\nВсе данные сохранены в папке 'output/'")
    print("Для реальной отправки используйте пункт меню 'Загрузка данных в API'")

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