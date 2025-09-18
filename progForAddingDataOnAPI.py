import json
import requests
import socket
from pathlib import Path
from datetime import datetime
import os
import urllib3
import pymssql

# ОСНОВНАЯ ПРОГРАММА

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

def prepare_gas_object_data(row):
    """Подготовка данных для создания объекта газификации"""
    
    # Формируем название объекта
    object_name = row['object_name'] or f"Объект {row['house_number']}"
    
    # Формируем полное название улицы
    street_full = f"{row.get('street_type', 'ул')} {row['street_name']}"
    
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
                            "street": street_full,
                            "house": row['house_number'],
                            "block": row['block'] or None,
                            "flat": 1,  # По умолчанию
                            "room": None,
                            "zip_code": row['zip_code'],
                            "region_fias_id": f"region_{row['region_code']}",
                            "city_fias_id": row['settlement_fias_id'],
                            "settlement_fias_id": row['settlement_fias_id'],
                            "street_fias_id": row['street_fias_id'],
                            "has_capital_construction": True,
                            "extra": row.get('full_address', '')
                        }
                    }
                }
            }
        }
    }
    
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
        headers["Authorization"] = f"Bearer {auth_token}"
    
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
            print("✓ Объект успешно создан!")
            return response.json()
        else:
            print(f"✗ Ошибка: {response.status_code}")
            print(f"Ответ: {response.text[:200]}...")
            return None
            
    except Exception as e:
        print(f"Ошибка при отправке в API: {e}")
        return None

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
    address_data = get_complete_address_data(limit=5)
    
    if address_data:
        print(f"Успешно получено {len(address_data)} адресных записей!")
        
        for i, row in enumerate(address_data, 1):
            print(f"\n--- Подготавливаем объект {i} ---")
            
            # Подготавливаем данные для API
            gas_object_data = prepare_gas_object_data(row)
            
            # Сохраняем для отладки
            save_raw_json_to_file(gas_object_data, f"gas_object_{i}_prepared")
            
            # Выводим информацию
            print(f"Объект: {row['object_name']}")
            print(f"Адрес: {row['region_name']}, {row['settlement_name']}, {row['street_name']}, д. {row['house_number']}")
            
            # РАСКОММЕНТИРУЙТЕ ДЛЯ РЕАЛЬНОЙ ОТПРАВКИ:
            # print("Отправляем данные в API...")
            # result = send_gas_object_to_api(gas_object_data, API_AUTH_TOKEN)
            # if result:
            #     print("✓ Успешно отправлено в API")
            # else:
            #     print("✗ Ошибка отправки")
    else:
        print("Не удалось получить адресные данные для создания объектов")
    
    print("\n" + "=" * 60)
    print("ВЫПОЛНЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 60)
    print("\nВсе данные сохранены в папке 'output/'")
    print("Для реальной отправки раскомментируйте код отправки в API")

if __name__ == "__main__":
    main()