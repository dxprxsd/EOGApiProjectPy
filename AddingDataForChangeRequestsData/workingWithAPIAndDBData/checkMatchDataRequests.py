import json
import os
import urllib3
import pymssql
import decimal
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional

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
    """Находим базу данных, где есть таблица one_demand"""
    databases = get_available_databases()
    if not databases:
        print("Не удалось получить список баз данных")
        return None
    
    print("Поиск базы данных с таблицей one_demand...")
    
    for db_name in databases:
        print(f"Проверяем базу данных: {db_name}...")
        conn = get_sql_connection(db_name)
        if not conn:
            continue
            
        try:
            cursor = conn.cursor()
            # Проверка наличия таблицы one_demand
            cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'one_demand'
            """)
            
            if cursor.fetchone():
                print(f"✓ Найдена подходящая БД: {db_name}")
                return db_name
                
        except pymssql.Error as e:
            print(f"  Ошибка проверки БД {db_name}: {e}")
        finally:
            conn.close()
    
    print("Не найдена БД с таблицей one_demand")
    return None

class JSONDBComparator:
    def __init__(self, json_folder: str, output_folder: str):
        self.json_folder = json_folder
        self.output_folder = output_folder
        self.connection = None
        self.database_name = None
        self.start_time = None
        
        # Создаем папку для результатов, если она не существует
        os.makedirs(output_folder, exist_ok=True)
    
    def log_progress(self, message: str, current: int = None, total: int = None):
        """Логирование прогресса с временными метками"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        if current is not None and total is not None:
            progress = f" [{current}/{total}]"
            percentage = f" ({current/total*100:.1f}%)"
            print(f"[{timestamp}] {message}{progress}{percentage}")
        else:
            print(f"[{timestamp}] {message}")
    
    def log_time_elapsed(self):
        """Логирование прошедшего времени"""
        if self.start_time:
            elapsed = datetime.now() - self.start_time
            hours, remainder = divmod(elapsed.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            self.log_progress(f"Прошло времени: {int(hours)}ч {int(minutes)}м {int(seconds)}с")
    
    def connect_db(self):
        """Подключение к базе данных с автоматическим поиском правильной БД"""
        setup_proxy()
        
        self.log_progress("Поиск правильной базы данных...")
        self.database_name = find_correct_database()
        
        if not self.database_name:
            self.log_progress("Автоматический поиск не удался.")
            print("Пожалуйста, введите имя базы данных вручную:")
            self.database_name = input("Имя базы данных: ").strip()
        
        if not self.database_name:
            self.log_progress("Ошибка: Не указана база данных")
            return False
        
        try:
            self.connection = get_sql_connection(self.database_name)
            if self.connection:
                self.log_progress(f"Успешное подключение к БД: {self.database_name}")
                return True
            else:
                self.log_progress(f"Ошибка подключения к БД: {self.database_name}")
                return False
        except Exception as e:
            self.log_progress(f"Ошибка подключения к БД: {e}")
            return False
    
    def disconnect_db(self):
        """Отключение от базы данных"""
        if self.connection:
            self.connection.close()
            self.log_progress("Отключение от БД")
    
    def get_all_db_records(self) -> List[Dict]:
        """Получение всех записей из таблицы one_demand"""
        try:
            cursor = self.connection.cursor()
            query = """
            SELECT id, n1, n2, ogs, uf, fl_id, datesost, fil_add, fil_exec, type_comm, 
                   num_dem, ndog, datedog, nz, paytype, dem_type, addrob, prim, pay_client, 
                   price, no_delete, createdby, createtime, modifiedby, modifytime, source, 
                   id_currant_d, eog_num, notification_method, id_object, branch
            FROM one_demand
            """
            
            self.log_progress("Выполнение запроса к БД...")
            cursor.execute(query)
            
            records = []
            total_records = 0
            
            # Получаем все записи с прогрессом
            self.log_progress("Чтение данных из БД...")
            rows = cursor.fetchall()
            total_records = len(rows)
            
            for i, row in enumerate(rows, 1):
                if i % 1000 == 0 or i == total_records:  # Логируем каждые 1000 записей
                    self.log_progress(f"Обработано записей из БД", i, total_records)
                
                record = dict(row)
                # Конвертируем специальные типы данных
                for key, value in record.items():
                    if isinstance(value, datetime):
                        record[key] = value.isoformat()
                    elif isinstance(value, decimal.Decimal):
                        record[key] = float(value)
                    elif isinstance(value, uuid.UUID):
                        record[key] = str(value)
                records.append(record)
            
            self.log_progress(f"Получено {len(records)} записей из БД {self.database_name}")
            return records
            
        except Exception as e:
            self.log_progress(f"Ошибка получения данных из БД: {e}")
            return []
    
    def load_json_files(self) -> List[Dict]:
        """Загрузка всех JSON файлов из папки"""
        json_files = []
        
        if not os.path.exists(self.json_folder):
            self.log_progress(f"Папка {self.json_folder} не существует")
            return []
        
        files = [f for f in os.listdir(self.json_folder) if f.endswith('.json')]
        total_files = len(files)
        
        self.log_progress(f"Найдено {total_files} JSON файлов для обработки")
        
        for i, filename in enumerate(files, 1):
            if i % 10 == 0 or i == total_files:  # Логируем каждые 10 файлов
                self.log_progress(f"Загрузка JSON файлов", i, total_files)
                
            file_path = os.path.join(self.json_folder, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    data['_filename'] = filename  # Добавляем имя файла для идентификации
                    json_files.append(data)
            except Exception as e:
                self.log_progress(f"Ошибка загрузки файла {filename}: {e}")
        
        self.log_progress(f"Загружено {len(json_files)} JSON файлов")
        return json_files
    
    def safe_string_convert(self, value) -> str:
        """Безопасное преобразование значения в строку"""
        if value is None:
            return ""
        return str(value)
    
    def safe_lower(self, value) -> str:
        """Безопасное преобразование в нижний регистр"""
        if value is None:
            return ""
        return str(value).lower()
    
    def normalize_date(self, date_value) -> Optional[str]:
        """Нормализация даты для сравнения"""
        if not date_value:
            return None
        
        try:
            # Если это уже строка
            if isinstance(date_value, str):
                # Убираем временную зону для упрощения сравнения
                if 'T' in date_value:
                    # Простой парсинг даты
                    date_part = date_value.split('T')[0]
                    time_part = date_value.split('T')[1].split('+')[0].split('-')[0]
                    return f"{date_part} {time_part}"
                return date_value
            # Если это datetime объект
            elif isinstance(date_value, datetime):
                return date_value.strftime('%Y-%m-%d %H:%M:%S')
            return str(date_value)
        except:
            return str(date_value) if date_value else None
    
    def extract_json_data(self, json_data: Dict) -> Dict:
        """Извлечение данных из JSON для сравнения"""
        attributes = json_data.get('attributes', {})
        relationships = json_data.get('relationships', {})
        
        extracted = {
            'id': self.safe_string_convert(json_data.get('id')),
            'created_at': self.normalize_date(attributes.get('created_at')),
            'updated_at': self.normalize_date(attributes.get('updated_at')),
            'uid': self.safe_string_convert(attributes.get('uid')),
            'user_id': attributes.get('user_id'),
            'user_phone': self.safe_string_convert(attributes.get('user_phone')),
            'user_email': self.safe_string_convert(attributes.get('user_email')),
            'object_name': self.safe_string_convert(attributes.get('object_name')),
            'organization_name': self.safe_string_convert(attributes.get('organization_name')),
            'branch_id': relationships.get('branch', {}).get('data', {}).get('id'),
            'service_id': relationships.get('service', {}).get('data', {}).get('id'),
            'source_id': relationships.get('source', {}).get('data', {}).get('id'),
            'application_datetime': self.normalize_date(attributes.get('application_datetime')),
            'status': self.safe_string_convert(attributes.get('status')),
            'user_full_name': self.safe_string_convert(attributes.get('user_full_name')),
            'send_date': self.normalize_date(attributes.get('send_date'))
        }
        
        return extracted
    
    def calculate_match_score(self, matches: Dict) -> Dict:
        """Вычисление оценки совпадения и определение уровня"""
        score_details = {
            'total_matches': len(matches),
            'strong_matches': 0,
            'weak_matches': 0,
            'match_level': 'low'
        }
        
        # Сильные совпадения (уникальные идентификаторы)
        strong_match_types = ['id', 'uid_eog']
        # Слабые совпадения (могут повторяться)
        weak_match_types = ['dates', 'update_dates', 'email', 'phone', 'object_name', 'organization', 'branch', 'user_id']
        
        for match_type in matches:
            if match_type in strong_match_types:
                score_details['strong_matches'] += 1
            elif match_type in weak_match_types:
                score_details['weak_matches'] += 1
        
        # Определяем уровень совпадения
        total_score = score_details['strong_matches'] * 2 + score_details['weak_matches']
        
        if score_details['strong_matches'] >= 2 or total_score >= 4:
            score_details['match_level'] = 'high'
        elif score_details['strong_matches'] >= 1 or total_score >= 2:
            score_details['match_level'] = 'medium'
        else:
            score_details['match_level'] = 'low'
        
        score_details['total_score'] = total_score
        return score_details
    
    def compare_records(self, json_record: Dict, db_record: Dict) -> Dict:
        """Сравнение одной JSON записи с одной записью из БД"""
        json_data = self.extract_json_data(json_record)
        matches = {}
        
        # Сравнение дат создания
        json_created = json_data.get('created_at')
        db_created = db_record.get('createtime')
        
        if json_created and db_created and self.dates_are_similar(json_created, db_created):
            matches['dates'] = {
                'json_created': json_created,
                'db_created': db_created,
                'match_type': 'Даты создания'
            }
            
            # Если даты создания похожи, сравниваем остальные поля
            additional_matches = self.compare_additional_fields(json_data, db_record)
            if additional_matches:
                matches.update(additional_matches)
        
        # Сравнение дат обновления
        json_updated = json_data.get('updated_at')
        db_updated = db_record.get('modifytime')
        
        if json_updated and db_updated and self.dates_are_similar(json_updated, db_updated):
            matches['update_dates'] = {
                'json_updated': json_updated,
                'db_updated': db_updated,
                'match_type': 'Даты обновления'
            }
        
        # Сравнение ID
        json_id = json_data.get('id')
        db_id = self.safe_string_convert(db_record.get('id'))
        
        if json_id and db_id and json_id == db_id:
            matches['id'] = {
                'json_id': json_id,
                'db_id': db_id,
                'match_type': 'ID'
            }
        
        # Сравнение UID/EOG номера
        json_uid = json_data.get('uid')
        db_eog = self.safe_string_convert(db_record.get('eog_num'))
        
        if json_uid and db_eog and json_uid == db_eog:
            matches['uid_eog'] = {
                'json_uid': json_uid,
                'db_eog_num': db_eog,
                'match_type': 'UID/EOG номер'
            }
        
        # Если нет совпадений по датам, но есть другие важные совпадения
        if not matches.get('dates') and not matches.get('update_dates'):
            additional_matches = self.compare_additional_fields(json_data, db_record)
            if additional_matches:
                matches.update(additional_matches)
        
        # Вычисляем оценку совпадения
        if matches:
            match_score = self.calculate_match_score(matches)
            matches['_match_score'] = match_score
        
        return matches
    
    def dates_are_similar(self, date1: str, date2: str) -> bool:
        """Проверка, похожи ли даты (сравниваем только дату, без времени)"""
        try:
            def extract_date_part(date_str):
                """Извлекает часть с датой из строки"""
                if not date_str:
                    return None
                
                date_str = str(date_str).strip()
                
                # Формат: YYYY-MM-DD HH:MM:SS
                if ' ' in date_str:
                    return date_str.split(' ')[0]
                
                # Формат: YYYY-MM-DD
                elif '-' in date_str and 'T' not in date_str:
                    return date_str.split(' ')[0] if ' ' in date_str else date_str
                
                # Формат с T: YYYY-MM-DDTHH:MM:SS
                elif 'T' in date_str:
                    return date_str.split('T')[0]
                
                return date_str
            
            date1_part = extract_date_part(date1)
            date2_part = extract_date_part(date2)
            
            if not date1_part or not date2_part:
                return False
            
            # Сравниваем только даты (без времени)
            return date1_part == date2_part
            
        except:
            return False
    
    def compare_additional_fields(self, json_data: Dict, db_record: Dict) -> Dict:
        """Сравнение дополнительных полей после совпадения дат"""
        matches = {}
        
        # Сравнение email
        json_email = self.safe_lower(json_data.get('user_email'))
        db_prim = self.safe_lower(db_record.get('prim'))
        
        if json_email and db_prim and json_email and json_email in db_prim:
            matches['email'] = {
                'json_email': json_data['user_email'],
                'db_prim': db_record['prim'],
                'match_type': 'Email в примечаниях'
            }
        
        # Сравнение телефона
        json_phone = self.safe_string_convert(json_data.get('user_phone'))
        db_prim_str = self.safe_string_convert(db_record.get('prim'))
        
        if json_phone and db_prim_str and json_phone in db_prim_str:
            matches['phone'] = {
                'json_phone': json_phone,
                'db_prim': db_record['prim'],
                'match_type': 'Телефон в примечаниях'
            }
        
        # Сравнение имени объекта
        json_object = self.safe_string_convert(json_data.get('object_name'))
        db_addrob = self.safe_string_convert(db_record.get('addrob'))
        
        if json_object and db_addrob and json_object in db_addrob:
            matches['object_name'] = {
                'json_object_name': json_data['object_name'],
                'db_addrob': db_record['addrob'],
                'match_type': 'Название объекта в адресе'
            }
        
        # Сравнение названия организации
        json_org = self.safe_string_convert(json_data.get('organization_name'))
        db_prim_str = self.safe_string_convert(db_record.get('prim'))
        
        if json_org and db_prim_str and json_org in db_prim_str:
            matches['organization'] = {
                'json_organization': json_data['organization_name'],
                'db_prim': db_record['prim'],
                'match_type': 'Организация в примечаниях'
            }
        
        # Сравнение branch ID
        json_branch = self.safe_string_convert(json_data.get('branch_id'))
        db_branch = self.safe_string_convert(db_record.get('branch'))
        
        if json_branch and db_branch and json_branch == db_branch:
            matches['branch'] = {
                'json_branch_id': json_data['branch_id'],
                'db_branch': db_record['branch'],
                'match_type': 'ID ветки'
            }
        
        # Сравнение user_id с fl_id
        json_user_id = self.safe_string_convert(json_data.get('user_id'))
        db_fl_id = self.safe_string_convert(db_record.get('fl_id'))
        
        if json_user_id and db_fl_id and json_user_id == db_fl_id:
            matches['user_id'] = {
                'json_user_id': json_data['user_id'],
                'db_fl_id': db_record['fl_id'],
                'match_type': 'User ID'
            }
        
        return matches
    
    def save_high_match_file(self, json_file: Dict, db_record: Dict, matches: Dict, match_score: Dict):
        """Сохранение файла с высоким уровнем совпадения"""
        try:
            # Используем указанную папку для сохранения
            matches_folder = "/home/kuzminiv/EOGProjPyApi/AddingDataForChangeRequestsData/getDataFromAPI/outputData/matchs"
            os.makedirs(matches_folder, exist_ok=True)
            
            # Имя файла на основе JSON ID и UID
            json_id = json_file.get('id', 'unknown')
            json_uid = json_file.get('attributes', {}).get('uid', 'unknown')
            db_id = db_record.get('id', 'unknown')
            
            # Создаем безопасное имя файла
            safe_json_id = "".join(c for c in str(json_id) if c.isalnum() or c in ('-', '_'))
            safe_db_id = "".join(c for c in str(db_id) if c.isalnum() or c in ('-', '_'))
            
            filename = f"match_{safe_json_id}_{safe_db_id}.json"
            filepath = os.path.join(matches_folder, filename)
            
            # Создаем структуру данных для сохранения
            result_data = {
                "match_info": {
                    "match_level": match_score['match_level'],
                    "total_score": match_score['total_score'],
                    "strong_matches": match_score['strong_matches'],
                    "weak_matches": match_score['weak_matches'],
                    "total_matches": match_score['total_matches'],
                    "comparison_date": datetime.now().isoformat(),
                    "database": self.database_name,
                    "json_filename": json_file.get('_filename')
                },
                "json_data": {
                    "id": json_file.get('id'),
                    "uid": json_file.get('attributes', {}).get('uid'),
                    "user_full_name": json_file.get('attributes', {}).get('user_full_name'),
                    "user_phone": json_file.get('attributes', {}).get('user_phone'),
                    "user_email": json_file.get('attributes', {}).get('user_email'),
                    "object_name": json_file.get('attributes', {}).get('object_name'),
                    "organization_name": json_file.get('attributes', {}).get('organization_name'),
                    "created_at": json_file.get('attributes', {}).get('created_at'),
                    "updated_at": json_file.get('attributes', {}).get('updated_at'),
                    "status": json_file.get('attributes', {}).get('status'),
                    "application_datetime": json_file.get('attributes', {}).get('application_datetime'),
                    "send_date": json_file.get('attributes', {}).get('send_date')
                },
                "db_data": {
                    "id": db_record.get('id'),
                    "eog_num": db_record.get('eog_num'),
                    "fl_id": db_record.get('fl_id'),
                    "addrob": db_record.get('addrob'),
                    "prim": db_record.get('prim'),
                    "branch": db_record.get('branch'),
                    "createtime": db_record.get('createtime'),
                    "modifytime": db_record.get('modifytime'),
                    "n1": db_record.get('n1'),
                    "n2": db_record.get('n2'),
                    "ogs": db_record.get('ogs'),
                    "uf": db_record.get('uf'),
                    "datesost": db_record.get('datesost'),
                    "fil_add": db_record.get('fil_add'),
                    "fil_exec": db_record.get('fil_exec'),
                    "type_comm": db_record.get('type_comm'),
                    "num_dem": db_record.get('num_dem'),
                    "ndog": db_record.get('ndog'),
                    "datedog": db_record.get('datedog'),
                    "nz": db_record.get('nz'),
                    "paytype": db_record.get('paytype'),
                    "dem_type": db_record.get('dem_type'),
                    "pay_client": db_record.get('pay_client'),
                    "price": db_record.get('price'),
                    "no_delete": db_record.get('no_delete'),
                    "createdby": db_record.get('createdby'),
                    "modifiedby": db_record.get('modifiedby'),
                    "source": db_record.get('source'),
                    "id_currant_d": db_record.get('id_currant_d'),
                    "notification_method": db_record.get('notification_method'),
                    "id_object": db_record.get('id_object')
                },
                "common_fields": self.format_common_fields(matches)
            }
            
            # Сохраняем файл
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2, default=str)
            
            self.log_progress(f"Сохранено совпадение высокого уровня: {filename}")
            return filepath
            
        except Exception as e:
            self.log_progress(f"Ошибка сохранения файла совпадения: {e}")
            return None
    
    def format_common_fields(self, matches: Dict) -> Dict:
        """Форматирование общих полей для отчета"""
        common_fields = {
            "summary": "Общие поля между JSON и записью БД:",
            "matches": []
        }
        
        for match_type, match_data in matches.items():
            if match_type == '_match_score':
                continue
                
            match_info = {
                'match_type': match_data.get('match_type', match_type),
                'description': self.get_match_description(match_type),
                'json_value': match_data.get(f'json_{match_type}', 
                                           match_data.get('json_id', 
                                                        match_data.get('json_uid', 'N/A'))),
                'db_value': match_data.get(f'db_{match_type}', 
                                         match_data.get('db_id', 
                                                      match_data.get('db_eog_num', 'N/A')))
            }
            common_fields["matches"].append(match_info)
        
        return common_fields
    
    def get_match_description(self, match_type: str) -> str:
        """Получение описания типа совпадения"""
        descriptions = {
            'id': 'Прямое совпадение идентификаторов',
            'uid_eog': 'Совпадение UID из JSON с EOG номером из БД',
            'dates': 'Совпадение дат создания',
            'update_dates': 'Совпадение дат обновления',
            'email': 'Email найден в примечаниях БД',
            'phone': 'Телефон найден в примечаниях БД',
            'object_name': 'Название объекта найдено в адресе БД',
            'organization': 'Название организации найдено в примечаниях БД',
            'branch': 'Совпадение идентификаторов веток',
            'user_id': 'Совпадение User ID с fl_id из БД'
        }
        return descriptions.get(match_type, 'Неизвестный тип совпадения')
    
    def compare_all(self) -> Dict:
        """Основная функция сравнения всех данных"""
        self.start_time = datetime.now()
        self.log_progress("Начало сравнения JSON файлов с данными БД")
        
        if not self.connect_db():
            return {"error": "Не удалось подключиться к БД"}
        
        try:
            # Получаем данные
            self.log_progress("Загрузка данных из БД...")
            db_records = self.get_all_db_records()
            
            self.log_progress("Загрузка JSON файлов...")
            json_files = self.load_json_files()
            
            if not db_records or not json_files:
                return {"error": "Нет данных для сравнения"}
            
            self.log_progress(f"Начинаем сравнение: {len(json_files)} файлов vs {len(db_records)} записей БД")
            self.log_time_elapsed()
            
            results = {
                "comparison_date": datetime.now().isoformat(),
                "database_used": self.database_name,
                "total_json_files": len(json_files),
                "total_db_records": len(db_records),
                "high_matches": 0,
                "medium_matches": 0,
                "low_matches": 0,
                "saved_files": []
            }
            
            total_comparisons = len(json_files) * len(db_records)
            comparisons_done = 0
            last_log_time = datetime.now()
            
            # Сравниваем каждый JSON файл с каждой записью БД
            for file_index, json_file in enumerate(json_files, 1):
                filename = json_file.get('_filename', 'unknown')
                file_saved_matches = []
                
                # Логируем прогресс по файлам
                if file_index % 5 == 0 or file_index == len(json_files):
                    self.log_progress(f"Обработка файлов", file_index, len(json_files))
                    self.log_time_elapsed()
                
                for db_index, db_record in enumerate(db_records, 1):
                    comparisons_done += 1
                    
                    # Логируем прогресс каждые 10000 сравнений или каждую минуту
                    current_time = datetime.now()
                    if (comparisons_done % 10000 == 0 or 
                        (current_time - last_log_time).total_seconds() > 60):
                        progress_percent = (comparisons_done / total_comparisons) * 100
                        self.log_progress(
                            f"Выполнено сравнений: {comparisons_done}/{total_comparisons} ({progress_percent:.1f}%)"
                        )
                        self.log_time_elapsed()
                        last_log_time = current_time
                    
                    try:
                        matches = self.compare_records(json_file, db_record)
                        
                        if matches and '_match_score' in matches:
                            match_score = matches['_match_score']
                            
                            # Сохраняем только высокие и средние совпадения
                            if match_score['match_level'] in ['high', 'medium']:
                                # Сохраняем файл для высоких совпадений
                                if match_score['match_level'] == 'high':
                                    saved_file = self.save_high_match_file(json_file, db_record, matches, match_score)
                                    if saved_file:
                                        results["high_matches"] += 1
                                        file_saved_matches.append({
                                            "db_record_id": db_record.get('id'),
                                            "saved_file": os.path.basename(saved_file),
                                            "match_score": match_score
                                        })
                                else:
                                    results["medium_matches"] += 1
                                
                                # Логируем найденные совпадения
                                if match_score['match_level'] == 'high':
                                    self.log_progress(f"ВЫСОКОЕ СОВПАДЕНИЕ: {filename} с записью БД {db_record.get('id')} (оценка: {match_score['total_score']})")
                            
                    except Exception as e:
                        if comparisons_done % 1000 == 0:  # Логируем ошибки не слишком часто
                            self.log_progress(f"Ошибка при сравнении файла {filename} с записью БД {db_record.get('id')}: {e}")
                        continue
                
                # Добавляем информацию о файлах с сохраненными совпадениями
                if file_saved_matches:
                    results["saved_files"].append({
                        "json_file": filename,
                        "json_id": json_file.get('id'),
                        "saved_matches_count": len(file_saved_matches),
                        "matches": file_saved_matches
                    })
            
            self.log_progress("Сравнение завершено!")
            self.log_time_elapsed()
            
            # Сохраняем общие результаты
            self.save_summary_results(results)
            return results
            
        except Exception as e:
            self.log_progress(f"Критическая ошибка при сравнении: {e}")
            return {"error": str(e)}
        finally:
            self.disconnect_db()
    
    def save_summary_results(self, results: Dict):
        """Сохранение сводных результатов"""
        self.log_progress("Сохранение сводных результатов...")
        
        # Сохраняем сводку в папку matches
        matches_folder = "/home/kuzminiv/EOGProjPyApi/AddingDataForChangeRequestsData/getDataFromAPI/outputData/matchs"
        os.makedirs(matches_folder, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(matches_folder, f"comparison_summary_{timestamp}.json")
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=str)
            
            self.log_progress(f"Сводные результаты сохранены в файл: {output_file}")
            
            # Создаем читабельный отчет
            self.create_readable_summary(results, timestamp, matches_folder)
            
        except Exception as e:
            self.log_progress(f"Ошибка сохранения сводных результатов: {e}")
    
    def create_readable_summary(self, results: Dict, timestamp: str, matches_folder: str):
        """Создание читабельного сводного отчета"""
        self.log_progress("Создание читабельного отчета...")
        
        report_file = os.path.join(matches_folder, f"comparison_summary_{timestamp}.txt")
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("СВОДНЫЙ ОТЧЕТ СРАВНЕНИЯ JSON ФАЙЛОВ С ДАННЫМИ БД\n")
                f.write("=" * 70 + "\n\n")
                
                f.write(f"Дата сравнения: {results.get('comparison_date')}\n")
                f.write(f"База данных: {results.get('database_used')}\n")
                f.write(f"Всего JSON файлов: {results.get('total_json_files')}\n")
                f.write(f"Всего записей в БД: {results.get('total_db_records')}\n\n")
                
                f.write("РЕЗУЛЬТАТЫ СОВПАДЕНИЙ:\n")
                f.write("-" * 30 + "\n")
                f.write(f"Высокие совпадения: {results.get('high_matches', 0)}\n")
                f.write(f"Средние совпадения: {results.get('medium_matches', 0)}\n")
                f.write(f"Слабые совпадения: {results.get('low_matches', 0)}\n\n")
                
                f.write("СОХРАНЕННЫЕ ФАЙЛЫ С ВЫСОКИМИ СОВПАДЕНИЯМИ:\n")
                f.write("-" * 50 + "\n")
                
                saved_files = results.get('saved_files', [])
                if saved_files:
                    total_saved = sum(file_info['saved_matches_count'] for file_info in saved_files)
                    f.write(f"Всего сохранено файлов: {total_saved}\n\n")
                    
                    for file_info in saved_files:
                        f.write(f"\nФайл: {file_info['json_file']}\n")
                        f.write(f"JSON ID: {file_info['json_id']}\n")
                        f.write(f"Количество сохраненных совпадений: {file_info['saved_matches_count']}\n")
                        
                        for match in file_info['matches']:
                            score = match['match_score']
                            f.write(f"  - Файл: {match['saved_file']}\n")
                            f.write(f"    Запись БД ID: {match['db_record_id']}\n")
                            f.write(f"    Оценка: {score['total_score']} (сильные: {score['strong_matches']}, слабые: {score['weak_matches']})\n")
                else:
                    f.write("Файлов с высокими совпадениями не найдено\n")
                
                f.write(f"\nВсе файлы сохранены в папке: {matches_folder}\n")
                f.write("\nКОНЕЦ ОТЧЕТА\n")
            
            self.log_progress(f"Читабельный отчет сохранен в файл: {report_file}")
            
        except Exception as e:
            self.log_progress(f"Ошибка создания читабельного отчета: {e}")


def main():
    """Основная функция для запуска сравнения"""
    # Папки для работы
    json_folder = "/home/kuzminiv/EOGProjPyApi/AddingDataForChangeRequestsData/getDataFromAPI/outputData/leads"
    output_folder = "/home/kuzminiv/EOGProjPyApi/AddingDataForChangeRequestsData/getDataFromAPI/outputData/matchs"
    
    print("=" * 70)
    print("ЗАПУСК СРАВНЕНИЯ JSON ФАЙЛОВ С ДАННЫМИ БД")
    print("=" * 70)
    print("Режим: сохранение только полных и почти полных совпадений")
    print(f"Файлы будут сохранены в: {output_folder}")
    print("=" * 70)
    
    # Создаем компаратор и запускаем сравнение
    comparator = JSONDBComparator(json_folder, output_folder)
    results = comparator.compare_all()
    
    # Выводим краткие результаты в консоль
    if "error" in results:
        print(f"ОШИБКА: {results['error']}")
        return
    
    matches_folder = "/home/kuzminiv/EOGProjPyApi/AddingDataForChangeRequestsData/getDataFromAPI/outputData/matchs"
    
    print("\n" + "="*70)
    print("ФИНАЛЬНЫЕ РЕЗУЛЬТАТЫ СРАВНЕНИЯ")
    print("="*70)
    print(f"База данных: {results.get('database_used')}")
    print(f"JSON файлов обработано: {results.get('total_json_files', 0)}")
    print(f"Записей из БД обработано: {results.get('total_db_records', 0)}")
    print(f"Высоких совпадений: {results.get('high_matches', 0)}")
    print(f"Средних совпадений: {results.get('medium_matches', 0)}")
    print(f"Сохранено файлов с высокими совпадениями: {len(results.get('saved_files', []))}")
    print(f"Все файлы сохранены в папке: {matches_folder}")
    print("="*70)


if __name__ == "__main__":
    main()