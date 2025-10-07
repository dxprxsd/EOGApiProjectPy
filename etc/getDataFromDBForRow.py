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

# ПРОГРАММА ДЛЯ ПОЛУЧЕНИЯ ДАННЫХ ИЗ БД (таблицы pto_grp, pto_obj_adr)

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

# Настройки пагинации
BATCH_SIZE = 10000  # Количество записей за один запрос к БД

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

def get_sql_connection():
    """Устанавливаем соединение с SQL Server используя pymssql"""
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            port=SQL_PORT,
            user=SQL_USERNAME,
            password=SQL_PASSWORD,
            database=SQL_DATABASE,
            as_dict=True
        )
        return conn
        
    except pymssql.Error as e:
        print(f"Ошибка подключения к SQL Server: {e}")
        return None

def get_complete_address_data_batch(offset=0, batch_size=BATCH_SIZE):
    """Получение данных об адресах пачками - ИСПРАВЛЕННЫЙ ЗАПРОС"""
    conn = get_sql_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        query = f"""
        SELECT
            pg.id AS object_id,
            pg.descr AS object_name,
            pg.a_dom AS house_number,
            pg.a_korp AS block,
            pg.adr AS full_address,
            p2.NAME AS settlement_name,
            p2.[INDEX] AS zip_code,
            CAST(p2.aoguid AS VARCHAR(36)) AS settlement_fias_id,
            p3.NAME AS street_name,
            p3.SOCR AS street_type,
            CAST(p3.aoguid AS VARCHAR(36)) AS street_fias_id,
            r.NAMEREG AS region_name,
            r.kladr_reg AS region_code
        FROM [dog].[dbo].[pto_grp] pg
        LEFT JOIN [dog].[dbo].[P2] p2 ON pg.a_p2 = p2.ID
        LEFT JOIN [dog].[dbo].[P3] p3 ON pg.a_p3 = p3.ID
        LEFT JOIN [dog].[dbo].[region] r ON p2.IDREGION = r.IDR
        WHERE pg.a_dom IS NOT NULL
        ORDER BY pg.id
        OFFSET {offset} ROWS
        FETCH NEXT {batch_size} ROWS ONLY
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        return results
            
    except Exception as e:
        print(f"Ошибка при выполнении запроса pto_grp: {e}")
        return None
    finally:
        conn.close()

def get_pto_obj_adr_data_batch(offset=0, batch_size=BATCH_SIZE):
    """Получение данных из pto_obj_adr пачками - ИСПРАВЛЕННЫЙ ЗАПРОС"""
    conn = get_sql_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        query = f"""
        SELECT
            poa.*,
            p2.NAME AS p2_name,
            p2.SOCR AS p2_socr,
            p2.[INDEX] AS p2_index,
            CAST(p2.aoguid AS VARCHAR(36)) AS p2_fias_id,
            p3.NAME AS p3_name,
            p3.SOCR AS p3_socr,
            p3.[INDEX] AS p3_index,
            CAST(p3.aoguid AS VARCHAR(36)) AS p3_fias_id,
            s.NAME AS street_name,
            s.SOCR AS street_socr,
            s.CODE AS street_code,
            r.NAMEREG AS region_name,
            r.kladr_reg AS region_code
        FROM [dog].[dbo].[pto_obj_adr] poa
        LEFT JOIN [dog].[dbo].[P2] p2 ON poa.p2 = p2.ID
        LEFT JOIN [dog].[dbo].[P3] p3 ON poa.p3 = p3.ID
        LEFT JOIN [dog].[dbo].[STREET] s ON poa.street = s.ID
        LEFT JOIN [dog].[dbo].[region] r ON p2.IDREGION = r.IDR
        WHERE poa.dom IS NOT NULL
        ORDER BY poa.id
        OFFSET {offset} ROWS
        FETCH NEXT {batch_size} ROWS ONLY
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        return results
            
    except Exception as e:
        print(f"Ошибка при выполнении запроса pto_obj_adr: {e}")
        return None
    finally:
        conn.close()

def get_total_count(table_name):
    """Получение общего количества записей в таблице"""
    conn = get_sql_connection()
    if not conn:
        return 0
    
    try:
        cursor = conn.cursor()
        
        if table_name == "pto_grp":
            query = "SELECT COUNT(*) as total FROM [dog].[dbo].[pto_grp] WHERE a_dom IS NOT NULL"
        else:  # pto_obj_adr
            query = "SELECT COUNT(*) as total FROM [dog].[dbo].[pto_obj_adr] WHERE dom IS NOT NULL"
        
        cursor.execute(query)
        result = cursor.fetchone()
        return result['total'] if result else 0
            
    except Exception as e:
        print(f"Ошибка при получении количества записей: {e}")
        return 0
    finally:
        conn.close()

# ========== ФУНКЦИИ ДЛЯ СОЗДАНИЯ ШАБЛОНОВ API ==========

def clean_value(value):
    """Очистка значений от некорректных данных"""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value in ['<отсутствует>', 'NULL', 'null', '']:
            return None
    return value

def format_street_name(street_type, street_name):
    """Форматирование названия улицы"""
    if not street_type or street_type == 'None':
        street_type = 'ул'
    if not street_name or street_name == 'None':
        street_name = 'Не указана'
    return f"{street_type}. {street_name}"

def prepare_gas_object_template(row, template_type="pto_obj_adr"):
    """Подготовка шаблона данных для API на основе данных из БД"""
    
    # Очищаем все значения
    cleaned_row = {key: clean_value(value) for key, value in row.items()}
    
    if template_type == "pto_grp":
        # Шаблон для данных из pto_grp
        object_name = cleaned_row.get('object_name') or f"Объект газификации {cleaned_row.get('object_id', 'N/A')}"
        
        # Форматируем адресные данные
        street_type = cleaned_row.get('street_type', 'ул')
        street_name = cleaned_row.get('street_name', 'Не указана')
        street_full = format_street_name(street_type, street_name)
        
        zip_code = cleaned_row.get('zip_code', '606000')
        if zip_code is None:
            zip_code = '606000'
        
        # Получаем значения для недостающих полей с безопасной обработкой
        house_number = cleaned_row.get('house_number', '')
        block = cleaned_row.get('block')
        region_name = cleaned_row.get('region_name', 'Нижегородская область')
        area = f"{region_name} район" if region_name else "Октябрьский район"
        
        template = {
            "data": {
                "type": "gas_object",
                "attributes": {
                    "name": object_name,
                    "description": f"Объект газификации из базы данных. ID: {cleaned_row.get('object_id', 'N/A')}",
                    "status": "active",
                    "custom_data": {
                        "source_database": "dog",
                        "source_table": "pto_grp",
                        "source_id": cleaned_row.get('object_id'),
                        "original_address": cleaned_row.get('full_address', '')
                    }
                },
                "relationships": {
                    "address": {
                        "data": {
                            "type": "address",
                            "attributes": {
                                "country": "Россия",
                                "region": region_name,
                                "city": cleaned_row.get('settlement_name', 'Не указан'),
                                "settlement": cleaned_row.get('settlement_name', 'Не указан'),
                                "area": area,
                                "zip_code": str(zip_code),
                                "street": street_full,
                                "house": str(house_number) if house_number else "2",
                                "block": str(block) if block else None,
                                "flat": 123,
                                "room": 3,
                                "region_fias_id": f"region_{cleaned_row.get('region_code', '52')}",
                                "city_fias_id": cleaned_row.get('settlement_fias_id'),
                                "settlement_fias_id": cleaned_row.get('settlement_fias_id'),
                                "area_fias_id": cleaned_row.get('settlement_fias_id') or "f6e148a1-c9d0-4141-a608-93e3bd95e6c4",
                                "house_fias_id": cleaned_row.get('settlement_fias_id') or "f6e148a1-c9d0-4141-a608-93e3bd95e6c4",
                                "street_fias_id": cleaned_row.get('street_fias_id'),
                                "extra": cleaned_row.get('full_address', '') or "новый дом",
                                "oktmo": 123456789,
                                "cadastral_number": "123421W",
                                "cadastral_home_number": "123421H",
                                "okato": "123421H",
                                "title": f"{region_name or 'МСК'}, ул {street_name} {house_number}",
                                "has_capital_construction": True,
                                "room_type": "apartment_building"
                            }
                        }
                    }
                }
            }
        }
    
    else:  # template_type == "pto_obj_adr"
        # Шаблон для данных из pto_obj_adr
        object_name = f"Объект {cleaned_row.get('id', 'N/A')}"
        
        # Обработка названий улиц и населенных пунктов
        street_name = cleaned_row.get('street_name', 'Не указана')
        p3_name = cleaned_row.get('p3_name', 'Не указан')
        p2_name = cleaned_row.get('p2_name', 'Не указан')
        
        # Заменяем <отсутствует> на более понятные значения
        if p3_name == '<отсутствует>' or p3_name is None:
            p3_name = p2_name
        
        street_full = format_street_name(cleaned_row.get('street_socr'), street_name)
        
        zip_code = cleaned_row.get('p2_index', '606000')
        if zip_code is None:
            zip_code = '606000'
        
        # Получаем значения для недостающих полей с безопасной обработкой
        house_number = cleaned_row.get('dom', '2')
        block = cleaned_row.get('korpus')
        region_name = cleaned_row.get('region_name', 'Нижегородская область')
        area = f"{region_name} район" if region_name else "Октябрьский район"
        
        # Безопасное получение FIAS ID
        p2_fias_id = cleaned_row.get('p2_fias_id') or "f6e148a1-c9d0-4141-a608-93e3bd95e6c4"
        p3_fias_id = cleaned_row.get('p3_fias_id') or p2_fias_id
        
        template = {
            "data": {
                "type": "gas_object",
                "attributes": {
                    "name": object_name,
                    "description": f"Объект из pto_obj_adr. ID: {cleaned_row.get('id', 'N/A')}",
                    "status": "active",
                    "custom_data": {
                        "source_database": "dog",
                        "source_table": "pto_obj_adr",
                        "source_id": cleaned_row.get('id'),
                        "p2": cleaned_row.get('p2'),
                        "p3": cleaned_row.get('p3'),
                        "street": cleaned_row.get('street'),
                        "dom": cleaned_row.get('dom'),
                        "korpus": cleaned_row.get('korpus')
                    }
                },
                "relationships": {
                    "address": {
                        "data": {
                            "type": "address",
                            "attributes": {
                                "country": "Россия",
                                "region": region_name,
                                "city": p2_name,
                                "settlement": p3_name,
                                "area": area,
                                "zip_code": str(zip_code),
                                "street": street_full,
                                "house": str(house_number) if house_number else "2",
                                "block": str(block) if block else None,
                                "flat": 123,
                                "room": 3,
                                "region_fias_id": f"region_{cleaned_row.get('region_code', '52')}",
                                "city_fias_id": p2_fias_id,
                                "settlement_fias_id": p3_fias_id,
                                "area_fias_id": p2_fias_id,
                                "house_fias_id": p2_fias_id,
                                "street_fias_id": None,
                                "extra": f"Original: p2={cleaned_row.get('p2')}, p3={cleaned_row.get('p3')}, street={cleaned_row.get('street')}" or "новый дом",
                                "oktmo": 123456789,
                                "cadastral_number": "123421W",
                                "cadastral_home_number": "123421H",
                                "okato": "123421H",
                                "title": f"{region_name or 'МСК'}, ул {street_name} {house_number}",
                                "has_capital_construction": True,
                                "room_type": "apartment_building"
                            }
                        }
                    }
                }
            }
        }
    
    # Добавляем метаданные
    metadata = {
        'source': template_type,
        'source_id': cleaned_row.get('object_id') if template_type == 'pto_grp' else cleaned_row.get('id'),
        'created_at': datetime.now().isoformat(),
        'batch_processed': False
    }
    
    return template, metadata

# ========== ФУНКЦИИ ДЛЯ СОХРАНЕНИЯ ДАННЫХ ==========

class JSONEncoder(json.JSONEncoder):
    """Кастомный JSON encoder для обработки сложных объектов"""
    def default(self, obj):
        if isinstance(obj, (datetime, pymssql.Date)):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif isinstance(obj, (decimal.Decimal)):
            return float(obj)
        elif isinstance(obj, (uuid.UUID)):
            return str(obj)
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        else:
            return super().default(obj)

def ensure_directory(base_path, subfolder):
    """Создание и обеспечение существования директории"""
    directory = Path(base_path) / subfolder
    directory.mkdir(exist_ok=True, parents=True)
    return directory

def save_single_template(template, metadata, base_directory="gas_objects"):
    """Сохранение одного шаблона в отдельный JSON файл"""
    try:
        source = metadata['source']
        source_id = metadata['source_id']
        
        # Создаем папку для источника данных
        output_dir = ensure_directory(base_directory, source)
        
        # Создаем имя файла
        filename = f"gas_object_{source}_{source_id}.json"
        filepath = output_dir / filename
        
        # Подготавливаем данные для сохранения
        data_to_save = {
            "template": template,
            "metadata": metadata
        }
        
        # Сохраняем в файл
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data_to_save, file, ensure_ascii=False, indent=2, cls=JSONEncoder)
        
        return True
        
    except Exception as e:
        print(f"Ошибка при сохранении шаблона {source_id}: {e}")
        return False

def process_table_data(table_name, base_directory="gas_objects"):
    """Обработка всех данных из таблицы и сохранение в отдельные файлы"""
    print(f"\nОбработка таблицы: {table_name}")
    print("=" * 50)
    
    # Получаем общее количество записей
    total_count = get_total_count(table_name)
    print(f"Всего записей в таблице {table_name}: {total_count}")
    
    if total_count == 0:
        print(f"В таблице {table_name} нет данных для обработки")
        return 0
    
    processed_count = 0
    offset = 0
    
    # Создаем основную папку для таблицы
    table_dir = ensure_directory(base_directory, table_name)
    print(f"Папка для сохранения: {table_dir}")
    
    start_time = time.time()
    
    # Обрабатываем данные пачками
    while offset < total_count:
        print(f"Загрузка пачки {offset//BATCH_SIZE + 1}... ({offset}-{min(offset+BATCH_SIZE, total_count)})")
        
        # Получаем данные пачкой
        if table_name == "pto_grp":
            batch_data = get_complete_address_data_batch(offset, BATCH_SIZE)
        else:
            batch_data = get_pto_obj_adr_data_batch(offset, BATCH_SIZE)
        
        if not batch_data:
            print(f"Ошибка при загрузке пачки {offset}-{offset+BATCH_SIZE}")
            break
        
        # Обрабатываем каждую запись в пачке
        batch_processed = 0
        for row in batch_data:
            try:
                # Создаем шаблон
                template, metadata = prepare_gas_object_template(row, table_name)
                
                # Сохраняем в отдельный файл
                if save_single_template(template, metadata, base_directory):
                    batch_processed += 1
                    processed_count += 1
                
                # Выводим прогресс каждые 100 записей
                if processed_count % 100 == 0:
                    print(f"Обработано: {processed_count}/{total_count} записей")
                    
            except Exception as e:
                print(f"Ошибка при обработке записи: {e}")
                continue
        
        print(f"Пачка {offset//BATCH_SIZE + 1} обработана: {batch_processed} записей")
        
        # Увеличиваем смещение для следующей пачки
        offset += BATCH_SIZE
        
        # Небольшая пауза между пачками чтобы не нагружать БД
        time.sleep(0.1)
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    print(f"\nОбработка таблицы {table_name} завершена!")
    print(f"Успешно обработано: {processed_count}/{total_count} записей")
    print(f"Время обработки: {processing_time:.2f} секунд")
    print(f"Средняя скорость: {processed_count/processing_time:.2f} записей/секунду")
    
    return processed_count

def create_summary_file(base_directory="gas_objects", total_processed=0):
    """Создание файла с общей статистикой"""
    try:
        summary = {
            "processing_summary": {
                "total_objects_processed": total_processed,
                "processing_date": datetime.now().isoformat(),
                "base_directory": base_directory,
                "tables_processed": ["pto_grp", "pto_obj_adr"]
            },
            "file_structure": {
                "pto_grp": "Папка с объектами из таблицы pto_grp",
                "pto_obj_adr": "Папка с объектами из таблицы pto_obj_adr"
            },
            "notes": "Каждый объект сохранен в отдельном JSON файле"
        }
        
        summary_path = Path(base_directory) / "processing_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as file:
            json.dump(summary, file, ensure_ascii=False, indent=2)
        
        print(f"\nФайл статистики создан: {summary_path}")
        
    except Exception as e:
        print(f"Ошибка при создании файла статистики: {e}")

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========

def main():
    print("=" * 70)
    print("МАССОВЫЙ ГЕНЕРАТОР ШАБЛОНОВ API - СОХРАНЕНИЕ В ОТДЕЛЬНЫЕ ФАЙЛЫ")
    print("=" * 70)
    
    print(f"\nНастройки пагинации: размер пачки = {BATCH_SIZE} записей")
    print(f"Базовая директория: gas_objects/")
    
    print("\nНастройка прокси...")
    setup_proxy()
    
    print("\nТестируем прокси соединение...")
    if not test_proxy_connection():
        print("Прокси не работает. Продолжаем без прокси?")
    
    # Обрабатываем обе таблицы
    total_processed = 0
    
    # 1. Обрабатываем таблицу pto_grp
    processed_pto_grp = process_table_data("pto_grp")
    total_processed += processed_pto_grp
    
    # 2. Обрабатываем таблицу pto_obj_adr
    processed_pto_obj_adr = process_table_data("pto_obj_adr")
    total_processed += processed_pto_obj_adr
    
    # Создаем файл статистики
    create_summary_file("gas_objects", total_processed)
    
    # Выводим итоговую статистику
    print("\n" + "=" * 70)
    print("ИТОГОВАЯ СТАТИСТИКА")
    print("=" * 70)
    print(f"Всего обработано объектов: {total_processed}")
    print(f"Из таблицы pto_grp: {processed_pto_grp}")
    print(f"Из таблицы pto_obj_adr: {processed_pto_obj_adr}")
    print(f"\nВсе файлы сохранены в папке: gas_objects/")
    print("\n" + "=" * 70)
    print("ВЫПОЛНЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 70)

if __name__ == "__main__":
    main()