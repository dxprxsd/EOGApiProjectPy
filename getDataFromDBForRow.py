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
        print("Успешное подключение к SQL Server!")
        return conn
        
    except pymssql.Error as e:
        print(f"Ошибка подключения к SQL Server: {e}")
        return None

def get_complete_address_data(limit=100):
    """Получение полных данных об адресах из связанных таблиц - ИСПРАВЛЕННЫЙ ЗАПРОС"""
    conn = get_sql_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # ИСПРАВЛЕННЫЙ ЗАПРОС - конвертируем UUID в строку
        query = f"""
        SELECT TOP ({limit})
            pg.id AS object_id,
            pg.descr AS object_name,
            pg.a_dom AS house_number,
            pg.a_korp AS block,
            pg.adr AS full_address,
            p2.NAME AS settlement_name,
            p2.[INDEX] AS zip_code,
            CAST(p2.aoguid AS VARCHAR(36)) AS settlement_fias_id,  -- Конвертируем UUID в строку
            p3.NAME AS street_name,
            p3.SOCR AS street_type,
            CAST(p3.aoguid AS VARCHAR(36)) AS street_fias_id,  -- Конвертируем UUID в строку
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

def get_pto_obj_adr_data(limit=100):
    """Получение данных из таблицы pto_obj_adr с полной адресной информацией - ИСПРАВЛЕННЫЙ ЗАПРОС"""
    conn = get_sql_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # ИСПРАВЛЕННЫЙ ЗАПРОС - конвертируем UUID в строку
        query = f"""
        SELECT TOP ({limit})
            poa.*,
            p2.NAME AS p2_name,
            p2.SOCR AS p2_socr,
            p2.[INDEX] AS p2_index,
            CAST(p2.aoguid AS VARCHAR(36)) AS p2_fias_id,  -- Конвертируем UUID в строку
            p3.NAME AS p3_name,
            p3.SOCR AS p3_socr,
            p3.[INDEX] AS p3_index,
            CAST(p3.aoguid AS VARCHAR(36)) AS p3_fias_id,  -- Конвертируем UUID в строку
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
        """
        
        print("Выполняем запрос для получения данных pto_obj_adr...")
        cursor.execute(query)
        
        results = cursor.fetchall()
        print(f"Получено {len(results)} записей из pto_obj_adr")
        return results
            
    except Exception as e:
        print(f"Ошибка при выполнении запроса pto_obj_adr: {e}")
        return None
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
    """Подготовка шаблона данных для API на основе данных из БД - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
    
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
    
    # Добавляем метаданные ОТДЕЛЬНО
    metadata = {
        'source': template_type,
        'source_id': cleaned_row.get('object_id') if template_type == 'pto_grp' else cleaned_row.get('id'),
        'template_number': 0,
        'created_at': datetime.now().isoformat()
    }
    
    return template, metadata

def create_api_templates_from_db(limit=10):
    """Создание шаблонов API из данных базы данных"""
    
    print("\n" + "=" * 60)
    print("СОЗДАНИЕ ШАБЛОНОВ API ИЗ БАЗЫ ДАННЫХ")
    print("=" * 60)
    
    all_templates = []
    all_metadata = []
    
    # 1. Получаем данные из pto_grp
    print("\n1. Получаем данные из pto_grp...")
    pto_grp_data = get_complete_address_data(limit=limit)
    
    if pto_grp_data:
        print(f"Создаем шаблоны для {len(pto_grp_data)} объектов из pto_grp...")
        
        for i, row in enumerate(pto_grp_data, 1):
            print(f"  Создание шаблона {i}/{len(pto_grp_data)}...")
            
            template, metadata = prepare_gas_object_template(row, "pto_grp")
            metadata['template_number'] = i
            
            all_templates.append(template)
            all_metadata.append(metadata)
    else:
        print("  Не удалось получить данные из pto_grp")
    
    # 2. Получаем данные из pto_obj_adr
    print("\n2. Получаем данные из pto_obj_adr...")
    pto_obj_adr_data = get_pto_obj_adr_data(limit=limit)
    
    if pto_obj_adr_data:
        print(f"Создаем шаблоны для {len(pto_obj_adr_data)} объектов из pto_obj_adr...")
        
        start_number = len(all_templates) + 1
        
        for i, row in enumerate(pto_obj_adr_data, start_number):
            print(f"  Создание шаблона {i}/{len(pto_obj_adr_data) + start_number - 1}...")
            
            template, metadata = prepare_gas_object_template(row, "pto_obj_adr")
            metadata['template_number'] = i
            
            all_templates.append(template)
            all_metadata.append(metadata)
    else:
        print("  Не удалось получить данные из pto_obj_adr")
    
    return all_templates, all_metadata

# ========== ФУНКЦИИ ДЛЯ СОХРАНЕНИЯ ДАННЫХ ==========

class JSONEncoder(json.JSONEncoder):
    """Кастомный JSON encoder для обработки сложных объектов - ИСПРАВЛЕННЫЙ"""
    def default(self, obj):
        if isinstance(obj, (datetime, pymssql.Date)):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif isinstance(obj, (decimal.Decimal)):
            return float(obj)
        elif isinstance(obj, (uuid.UUID)):  # Добавляем обработку UUID
            return str(obj)
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        else:
            return super().default(obj)

def save_api_templates_to_file(templates, metadata, filename="api_templates.json"):
    """Сохранение шаблонов API в JSON файл"""
    try:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        # Сохраняем шаблоны и метаданные отдельно
        data_to_save = {
            "templates": templates,
            "metadata": metadata,
            "summary": {
                "total_templates": len(templates),
                "created_at": datetime.now().isoformat()
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data_to_save, file, ensure_ascii=False, indent=2, cls=JSONEncoder)
        
        print(f"Успешно: Шаблоны API сохранены в файл: {filepath}")
        print(f"Успешно: Всего шаблонов: {len(templates)}")
        
        return True
        
    except Exception as e:
        print(f"Ошибка: Ошибка при сохранении шаблонов: {e}")
        return False

def save_individual_templates(templates, metadata):
    """Сохранение каждого шаблона в отдельный файл"""
    try:
        output_dir = Path("output") / "individual_templates"
        output_dir.mkdir(exist_ok=True, parents=True)
        
        for i, (template, meta) in enumerate(zip(templates, metadata), 1):
            source = meta['source']
            source_id = meta['source_id']
            
            filename = f"template_{i:03d}_{source}_{source_id}.json"
            filepath = output_dir / filename
            
            data_to_save = {
                "template": template,
                "metadata": meta
            }
            
            with open(filepath, 'w', encoding='utf-8') as file:
                json.dump(data_to_save, file, ensure_ascii=False, indent=2, cls=JSONEncoder)
        
        print(f"Успешно: Индивидуальные шаблоны сохранены в папку: {output_dir}")
        return True
        
    except Exception as e:
        print(f"Ошибка: Ошибка при сохранении индивидуальных шаблонов: {e}")
        return False

def save_template_samples(templates, metadata, sample_size=5):
    """Сохранение примеров шаблонов для ознакомления"""
    try:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        samples = list(zip(templates[:min(sample_size, len(templates))], 
                          metadata[:min(sample_size, len(metadata))]))
        
        data_to_save = {
            "samples": [
                {"template": template, "metadata": meta} 
                for template, meta in samples
            ],
            "sample_count": len(samples),
            "created_at": datetime.now().isoformat()
        }
        
        filepath = output_dir / "api_templates_samples.json"
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data_to_save, file, ensure_ascii=False, indent=2, cls=JSONEncoder)
        
        print(f"Успешно: Примеры шаблонов сохранены в: {filepath}")
        print(f"Успешно: Сохранено примеров: {len(samples)}")
        
        return True
        
    except Exception as e:
        print(f"Ошибка: Ошибка при сохранении примеров: {e}")
        return False

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========

def main():
    print("=" * 60)
    print("ГЕНЕРАТОР ШАБЛОНОВ API ИЗ БАЗЫ ДАННЫХ")
    print("=" * 60)
    
    print("\nНастройка прокси...")
    setup_proxy()
    
    print("\nТестируем прокси соединение...")
    if not test_proxy_connection():
        print("Прокси не работает. Продолжаем без прокси?")
    
    # Создаем шаблоны API из данных базы данных
    templates, metadata = create_api_templates_from_db(limit=20)
    
    if templates:
        print(f"\nУспешно: Успешно создано {len(templates)} шаблонов API!")
        
        # Сохраняем все шаблоны в один файл
        save_api_templates_to_file(templates, metadata, "api_templates_complete.json")
        
        # Сохраняем примеры для ознакомления
        save_template_samples(templates, metadata, 5)
        
        # Сохраняем каждый шаблон отдельно
        save_individual_templates(templates, metadata)
        
        # Выводим статистику
        print("\n" + "=" * 50)
        print("СТАТИСТИКА СОЗДАННЫХ ШАБЛОНОВ")
        print("=" * 50)
        
        sources = {}
        for meta in metadata:
            source = meta['source']
            if source in sources:
                sources[source] += 1
            else:
                sources[source] = 1
        
        for source, count in sources.items():
            print(f"{source}: {count} шаблонов")
        
        print(f"Всего: {len(templates)} шаблонов")
        
    else:
        print("\nОшибка: Не удалось создать шаблоны API. Проверьте подключение к базе данных.")
    
    print("\n" + "=" * 60)
    print("ВЫПОЛНЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 60)

if __name__ == "__main__":
    main()