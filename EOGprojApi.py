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
        # Можно продолжить без прокси, но API запросы могут не работать
    
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
                    
                    # Выводим краткую информацию о темах
                    print("\nПолученные темы:")
                    for subject in subjects_data.get('data', []):
                        attrs = subject.get('attributes', {})
                        print(f"- {attrs.get('name', 'N/A')} (ID: {attrs.get('id', 'N/A')})")
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
        
        # Выводим краткую информацию о первых записях
        print("\nПервые 5 записей из БД:")
        for i, record in enumerate(gez_data[:5], 1):
            print(f"{i}. ID: {record.get('ind', 'N/A')}, Номер: {record.get('num', 'N/A')}")
    else:
        print("Не удалось получить данные из базы данных")
    
    print("\n" + "=" * 60)
    print("ВЫПОЛНЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 60)
    print("\nВсе данные сохранены в папке 'output/'")
    print("Проверьте созданные файлы:")

def main():
    print("=" * 60)
    print("ЗАПУСК СБОРЩИКА ДАННЫХ ИЗ API И БАЗЫ ДАННЫХ")
    print("=" * 60)
    
    print("\nНастройка прокси...")
    setup_proxy()
    
    print("\nТестируем прокси соединение...")
    if not test_proxy_connection():
        print("Прокси не работает. Продолжаем без прокси?")
        # Можно продолжить без прокси, но API запросы могут не работать
    
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
                    
                    # Выводим краткую информацию о темах
                    print("\nПолученные темы:")
                    for subject in subjects_data.get('data', []):
                        attrs = subject.get('attributes', {})
                        print(f"- {attrs.get('name', 'N/A')} (ID: {attrs.get('id', 'N/A')})")
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
        
        # Выводим краткую информацию о первых записях
        print("\nПервые 5 записей из БД:")
        for i, record in enumerate(gez_data[:5], 1):
            print(f"{i}. ID: {record.get('ind', 'N/A')}, Номер: {record.get('num', 'N/A')}")
    else:
        print("Не удалось получить данные из базы данных")
    
    print("\n" + "=" * 60)
    print("ВЫПОЛНЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 60)
    print("\nВсе данные сохранены в папке 'output/'")
    print("Проверьте созданные файлы:")

if __name__ == "__main__":
    main()