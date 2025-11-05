import json
import requests
import socket
from pathlib import Path
from datetime import datetime
import os
import urllib3
import pymssql
import decimal
import uuid
import time

# программа для получения данных необходимых для работы с API, данные выгружаются в папку requests_data_output

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
SQL_DATABASE = 'master'  # Программа будет автоматически искать правильную БД
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

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ ==========

def get_sql_connection(database=None):
    """Устанавливаем соединение с SQL Server используя pymssql"""
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            port=SQL_PORT,
            user=SQL_USERNAME,
            password=SQL_PASSWORD,
            database=database or SQL_DATABASE,
            as_dict=True
        )
        return conn
        
    except pymssql.Error as e:
        print(f"Ошибка подключения к SQL Server: {e}")
        return None

def get_available_databases():
    """Получаем список доступных баз данных"""
    conn = get_sql_connection('master')
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT name 
        FROM sys.databases 
        WHERE state = 0 AND name NOT IN ('master', 'model', 'msdb', 'tempdb')
        ORDER BY name
        """)
        
        databases = [row['name'] for row in cursor.fetchall()]
        return databases
        
    except pymssql.Error as e:
        print(f"Ошибка получения списка БД: {e}")
        return []
    finally:
        conn.close()

def find_correct_database():
    """Находим базу данных, где есть нужные таблицы"""
    databases = get_available_databases()
    if not databases:
        print("Не удалось получить список баз данных")
        return None
    
    print("Поиск базы данных с нужными таблицами...")
    
    for db_name in databases:
        print(f"Проверяем базу данных: {db_name}...")
        conn = get_sql_connection(db_name)
        if not conn:
            continue
            
        try:
            cursor = conn.cursor()
            # Проверка наличия всех трех таблиц
            cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME IN ('one_pg_limitations', 'one_pg_demand_exec', 'one_tn')
            """)
            
            tables = [row['TABLE_NAME'] for row in cursor.fetchall()]
            print(f"  Найдены таблицы: {tables}")
            
            if len(tables) == 3:  # Если найдены все три таблицы
                print(f"✓ Найдена подходящая БД: {db_name}")
                return db_name
                
        except pymssql.Error as e:
            print(f"  Ошибка проверки БД {db_name}: {e}")
        finally:
            conn.close()
    
    print("Не найдена БД со всеми тремя таблицами, ищем БД с основной таблицей one_pg_limitations...")
    
    # Если все три таблицы не были найдены, ищем хотя бы с основной
    for db_name in databases:
        conn = get_sql_connection(db_name)
        if not conn:
            continue
            
        try:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'one_pg_limitations'
            """)
            
            if cursor.fetchone():
                print(f"Найдена БД с таблицей one_pg_limitations: {db_name}")
                return db_name
                
        except pymssql.Error:
            continue
        finally:
            conn.close()
    
    return None

def get_data_by_demand_id(demand_id, database):
    """Получаем данные по конкретному demand_id"""
    conn = get_sql_connection(database)
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # Основной запрос с JOIN вместо подзапросов для лучшей производительности
        query = """
        SELECT 
            lim.demand_id,
            lim.pir_planned_date,
            lim.smr_planned_date,
            lim.tu_notice_date,
            lim.tu_check_planned_date,
            lim.tu_check_fact_date,
            de_pir.w_end_fact as pir_fact_date,
            de_smr.w_end_fact as smr_fact_date,
            tn.date_act_signed
        FROM one_pg_limitations lim
        LEFT JOIN one_pg_demand_exec de_pir ON 
            lim.demand_id = de_pir.demand_id 
            AND de_pir.type_ex = 2 
            AND de_pir.actual = 1 
            AND de_pir.is_inner = 0
        LEFT JOIN one_pg_demand_exec de_smr ON 
            lim.demand_id = de_smr.demand_id 
            AND de_smr.type_ex = 4 
            AND de_smr.actual = 1 
            AND de_smr.is_inner = 0
        LEFT JOIN one_tn tn ON 
            lim.demand_id = tn.demand_id 
            AND tn.p1314 = 0
        WHERE lim.demand_id = %s
        """
        
        print(f"Выполняем запрос для demand_id: {demand_id}")
        cursor.execute(query, (demand_id,))
        result = cursor.fetchone()
        
        if result:
            print("Данные найдены!")
        else:
            print("Данные не найдены")
            
        return result
    
    except pymssql.Error as e:
        print(f"Ошибка выполнения SQL запроса: {e}")
        return None
    finally:
        conn.close()

def convert_datetime(obj):
    """Конвертируем datetime объекты в строки для JSON"""
    if isinstance(obj, datetime):
        return obj.isoformat() if obj else None
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    return obj

def save_to_json(data, demand_id):
    """Сохраняем данные в JSON файл"""
    try:
        # Создание папки если ее нет
        output_dir = Path("requests_data_output")
        output_dir.mkdir(exist_ok=True)
        
        # Формирование имени файла
        filename = output_dir / f"demand_data_{demand_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Сохранение данных
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=convert_datetime)
        
        print(f"Данные сохранены в файл: {filename}")
        return True
        
    except Exception as e:
        print(f"Ошибка сохранения в JSON: {e}")
        return False

def main():
    """Основная функция программы"""
    print("=== Программа для получения данных из БД по demand_id ===\n")
    
    # Настройка прокси
    setup_proxy()
    
    # Тестирование подключения
    if not test_proxy_connection():
        print("Предупреждение: проблемы с прокси соединением")
    
    # Нахождение правильной базы данных
    print("\nПоиск правильной базы данных...")
    correct_database = find_correct_database()
    
    if not correct_database:
        # Если автоматический поиск не удался, запрос у пользователя
        print("\nАвтоматический поиск не удался.")
        print("Пожалуйста, введите имя базы данных вручную:")
        correct_database = input("Имя базы данных: ").strip()
    
    if not correct_database:
        print("Ошибка: Не указана база данных")
        return
    
    print(f"\nИспользуем базу данных: {correct_database}")
    
    # Получение demand_id от пользователя
    try:
        demand_id = int(input("\nВведите demand_id для поиска данных: "))
    except ValueError:
        print("Ошибка: Введите целое число для demand_id")
        return
    
    print(f"\nПоиск данных для demand_id: {demand_id} в БД {correct_database}...")
    
    # Получение данных из БД
    data = get_data_by_demand_id(demand_id, correct_database)
    
    if data:
        print("\n" + "="*50)
        print("НАЙДЕНЫ СЛЕДУЮЩИЕ ДАННЫЕ:")
        print("="*50)
        
        # Форматирование вывода данных
        result_data = {
            "demand_id": demand_id,
            "database": correct_database,
            "export_date": datetime.now().isoformat(),
            "Выполнение ПИР": {
                "Плановая дата завершения": data.get('pir_planned_date'),
                "Фактическая дата завершения": data.get('pir_fact_date')
            },
            "Выполнение СМР": {
                "Плановая дата завершения": data.get('smr_planned_date'),
                "Фактическая дата завершения": data.get('smr_fact_date')
            },
            "Технические условия": {
                "Дата получения уведомления": data.get('tu_notice_date'),
                "Плановая дата проверки": data.get('tu_check_planned_date'),
                "Фактическая дата проверки": data.get('tu_check_fact_date')
            },
            "Акт": {
                "Дата подписания": data.get('date_act_signed')
            }
        }
        
        # Вывод данных в консоль
        for category, values in result_data.items():
            if category not in ["demand_id", "database", "export_date"]:
                print(f"\n{category}:")
                for key, value in values.items():
                    display_value = value.strftime('%Y-%m-%d %H:%M:%S') if value else "Не указано"
                    print(f"  {key}: {display_value}")
        
        # Сохранение в JSON
        if save_to_json(result_data, demand_id):
            print(f"\nДанные успешно сохранены в папку requests_data_output")
        else:
            print(f"\nОшибка при сохранении данных")
            
    else:
        print(f"\nДанные для demand_id {demand_id} не найдены в базе {correct_database}")

if __name__ == "__main__":
    main()