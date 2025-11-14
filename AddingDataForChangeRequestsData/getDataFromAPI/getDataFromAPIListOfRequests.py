import os
import json
import requests
import urllib3
import time
from datetime import datetime

# Программа для получения данных с функции апи "GET /v1/admin/leads Получение списка заявок"

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

# Путь для сохранения данных
OUTPUT_BASE_DIR = "/home/kuzminiv/EOGProjPyApi/AddingDataForChangeRequestsData/getDataFromAPI/outputData/leads"

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

def get_user_input_yes_no(question):
    """
    Запрашивает у пользователя ответ да/нет
    Args:
        question (str): Вопрос для пользователя
    Returns:
        bool: True если да, False если нет
    """
    while True:
        response = input(f"{question} (y/n): ").strip().lower()
        if response in ['y', 'yes', 'да', 'д']:
            return True
        elif response in ['n', 'no', 'нет', 'н']:
            return False
        else:
            print("Пожалуйста, введите 'y' (да) или 'n' (нет)")

def get_user_input_numbers(question):
    """
    Запрашивает у пользователя список чисел
    Args:
        question (str): Вопрос для пользователя
    Returns:
        list: Список чисел или None если пользователь не ввел данные
    """
    while True:
        response = input(f"{question} (через запятую или пробел): ").strip()
        
        if not response:
            return None
        
        try:
            # Разделяем ввод по запятым или пробелам
            numbers = []
            for item in response.replace(',', ' ').split():
                if item.strip():
                    numbers.append(int(item.strip()))
            return numbers
        except ValueError:
            print("Ошибка! Пожалуйста, введите только числа, разделенные запятыми или пробелами.")

def get_user_input_int(question, default_value=None):
    """
    Запрашивает у пользователя целое число
    Args:
        question (str): Вопрос для пользователя
        default_value (int): Значение по умолчанию
    Returns:
        int: Введенное число или значение по умолчанию
    """
    while True:
        if default_value is not None:
            prompt = f"{question} [по умолчанию: {default_value}]: "
        else:
            prompt = f"{question}: "
        
        response = input(prompt).strip()
        
        if not response and default_value is not None:
            return default_value
        
        try:
            return int(response)
        except ValueError:
            print("Ошибка! Пожалуйста, введите целое число.")

def get_user_input_string(question, default_value=None):
    """
    Запрашивает у пользователя строку
    Args:
        question (str): Вопрос для пользователя
        default_value (str): Значение по умолчанию
    Returns:
        str: Введенная строка или значение по умолчанию
    """
    if default_value is not None:
        prompt = f"{question} [по умолчанию: {default_value}]: "
    else:
        prompt = f"{question}: "
    
    response = input(prompt).strip()
    
    if not response and default_value is not None:
        return default_value
    
    return response if response else None

def get_user_input_string_list(question):
    """
    Запрашивает у пользователя список строк
    Args:
        question (str): Вопрос для пользователя
    Returns:
        list: Список строк или None если пользователь не ввел данные
    """
    response = input(f"{question} (через запятую): ").strip()
    
    if not response:
        return None
    
    # Разделяем строки по запятым
    items = [item.strip() for item in response.split(',') if item.strip()]
    return items if items else None

def get_user_input_boolean(question):
    """
    Запрашивает у пользователя булево значение
    Args:
        question (str): Вопрос для пользователя
    Returns:
        bool: True/False или None если не выбрано
    """
    while True:
        response = input(f"{question} (y/n/пусто - пропустить): ").strip().lower()
        
        if not response:
            return None
        
        if response in ['y', 'yes', 'да', 'д']:
            return True
        elif response in ['n', 'no', 'нет', 'н']:
            return False
        else:
            print("Пожалуйста, введите 'y' (да), 'n' (нет) или оставьте пустым для пропуска")

def configure_filters():
    """
    Настраивает фильтры через интерактивный диалог с пользователем
    Returns:
        dict: Словарь с настройками фильтров
    """
    print("\n" + "="*50)
    print("НАСТРОЙКА ФИЛЬТРОВ ДЛЯ СПИСКА ЗАЯВОК")
    print("="*50)
    
    filters = {
        # Сортировка
        'order_key': 'id',
        'order_type': 'desc',
        
        # Основные фильтры
        'status': None,
        'service': None,
        'user_id': None,
        'admin_id': None,
        'confidant_user_id': None,
        'has_confidant': None,
        'expired': None,
        'without_duplicates': None,
        
        # Поиск
        'query': None,
        'query_rog_eog': None,
        'uid': None,
        'epgu_order_id': None,
        'mfc_order_id': None,
        
        # Фильтры по организациям и регионам
        'organization_ids': None,
        'branch_ids': None,
        'region_ids': None,
        'source_ids': None,
        
        # Фильтры по датам
        'date_from': '21.01.2010',
        'date_to': '21.01.2030',
        'updated_at_from': '21.01.2010',
        'updated_at_to': '21.01.2030',
        'send_date_from': '21.01.2010',
        'send_date_to': '21.01.2030',
        
        # Дополнительные фильтры
        'contract_status': None,
        'contract_agreement_status': None,
        'rejection_reason_ids': None,
        'protocol_mismatch_ids': None,
        
        # Географические фильтры
        'gas_object_region': None,
        'gas_object_area': None,
        'gas_object_city': None,
        'gas_object_settlement': None,
        
        # Настройки пагинации
        'per_page': 100,
        'get_detailed_info': True,
        'short_format': False
    }
    
    print("\n--- НАСТРОЙКИ СОРТИРОВКИ ---")
    filters['order_key'] = get_user_input_string(
        "Ключ сортировки (например: id, created_at, updated_at)",
        default_value='id'
    )
    filters['order_type'] = get_user_input_string(
        "Тип сортировки (asc/desc)",
        default_value='desc'
    )
    
    print("\n--- ОСНОВНЫЕ ФИЛЬТРЫ ---")
    if get_user_input_yes_no("Хотите указать статусы заявок?"):
        filters['status'] = get_user_input_string_list(
            "Введите статусы через запятую (например: registered,in_progress,completed):"
        )
    
    if get_user_input_yes_no("Хотите указать ID услуги?"):
        filters['service'] = get_user_input_int("Введите ID услуги:")
    
    if get_user_input_yes_no("Хотите указать ID заявителя?"):
        filters['user_id'] = get_user_input_int("Введите ID заявителя:")
    
    if get_user_input_yes_no("Хотите указать ID исполнителя?"):
        filters['admin_id'] = get_user_input_string("Введите ID исполнителя:")
    
    if get_user_input_yes_no("Хотите указать ID уполномоченного представителя?"):
        filters['confidant_user_id'] = get_user_input_int("Введите ID уполномоченного представителя:")
    
    print("\n--- ФИЛЬТРЫ ПО ОРГАНИЗАЦИЯМ И РЕГИОНАМ ---")
    if get_user_input_yes_no("Хотите указать фильтр по организациям?"):
        filters['organization_ids'] = get_user_input_numbers(
            "Введите ID организаций (через запятую или пробел):"
        )
    
    if get_user_input_yes_no("Хотите указать фильтр по филиалам?"):
        filters['branch_ids'] = get_user_input_numbers(
            "Введите ID филиалов (через запятую или пробел):"
        )
    
    if get_user_input_yes_no("Хотите указать фильтр по регионам?"):
        filters['region_ids'] = get_user_input_numbers(
            "Введите ID регионов (через запятую или пробел):"
        )
    
    print("\n--- ПОИСК И ДОПОЛНИТЕЛЬНЫЕ ФИЛЬТРЫ ---")
    if get_user_input_yes_no("Хотите выполнить поиск по тексту?"):
        filters['query'] = get_user_input_string("Введите текст для поиска:")
    
    if get_user_input_yes_no("Хотите указать UID услуги?"):
        filters['uid'] = get_user_input_string("Введите UID услуги:")
    
    if get_user_input_yes_no("Хотите указать номер заявления на ЕПГУ?"):
        filters['epgu_order_id'] = get_user_input_string("Введите номер заявления на ЕПГУ:")
    
    if get_user_input_yes_no("Хотите указать номер заявления в МФЦ?"):
        filters['mfc_order_id'] = get_user_input_string("Введите номер заявления в МФЦ:")
    
    print("\n--- ФИЛЬТРЫ ПО ДАТАМ ---")
    print("Даты в формате ДД.ММ.ГГГГ")
    filters['date_from'] = get_user_input_string(
        "Дата создания ОТ",
        default_value='21.01.2010'
    )
    filters['date_to'] = get_user_input_string(
        "Дата создания ДО", 
        default_value='21.01.2030'
    )
    
    print("\n--- ДОПОЛНИТЕЛЬНЫЕ НАСТРОЙКИ ---")
    filters['per_page'] = get_user_input_int(
        "Количество элементов на странице (рекомендуется 50-100):",
        default_value=100
    )
    
    filters['get_detailed_info'] = get_user_input_yes_no(
        "Получать детальную информацию по каждой заявке?"
    )
    
    print("\nНастройки завершены!")
    return filters

def get_leads(filters, page=1):
    """
    Получает список заявок с учетом фильтров
    Args:
        filters (dict): Словарь с фильтрами
        page (int): Номер страницы
    Returns:
        dict: Ответ от API или None в случае ошибки
    """
    global API_AUTH_TOKEN
    
    if not API_AUTH_TOKEN:
        print("Токен авторизации не получен. Сначала выполните авторизацию.")
        return None
        
    try:
        # Формируем URL запроса
        url = f"{API_BASE_URL}/admin/leads"
        
        # Базовые параметры
        params = {
            "order_key": filters['order_key'],
            "order_type": filters['order_type'],
            "date_from": filters['date_from'],
            "date_to": filters['date_to'],
            "updated_at_from": filters['updated_at_from'],
            "updated_at_to": filters['updated_at_to'],
            "send_date_from": filters['send_date_from'],
            "send_date_to": filters['send_date_to'],
            "page": page,
            "per": filters['per_page'],
            "short_format": str(filters['short_format']).lower()
        }
        
        # Добавляем опциональные параметры если они указаны
        optional_params = [
            'status', 'service', 'user_id', 'admin_id', 'confidant_user_id',
            'query', 'query_rog_eog', 'uid', 'epgu_order_id', 'mfc_order_id',
            'organization_ids', 'branch_ids', 'region_ids', 'source_ids',
            'rejection_reason_ids', 'protocol_mismatch_ids',
            'contract_status', 'contract_agreement_status',
            'gas_object_region', 'gas_object_area', 'gas_object_city', 'gas_object_settlement',
            'sla_days_remaining', 'contract_agreement_term_id'
        ]
        
        for param in optional_params:
            if filters.get(param) is not None:
                if isinstance(filters[param], list):
                    for item in filters[param]:
                        params[f"{param}[]"] = item
                else:
                    params[param] = filters[param]
        
        # Булевы параметры
        bool_params = ['has_confidant', 'expired', 'without_duplicates', 
                      'home_in_schedule', 'is_writing', 'is_complex',
                      'social_assistance_approved', 'contract_or_agreement_coordination',
                      'preferential_category', 'contract_agreement_is_writing']
        
        for param in bool_params:
            if filters.get(param) is not None:
                params[param] = str(filters[param]).lower()
        
        # Заголовки запроса
        headers = {
            "Accept": "application/json",
            "Authorization": API_AUTH_TOKEN,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        print(f"Запрос списка заявок (страница {page})...")
        print(f"Фильтры: { {k: v for k, v in filters.items() if v is not None and k not in ['get_detailed_info']} }")
        
        # Отправляем запрос через прокси
        response = requests.get(
            url,
            params=params,
            headers=headers,
            proxies={
                "http": PROXY_URL,
                "https": PROXY_URL
            },
            timeout=30,
            verify=False
        )
        
        print(f"Статус ответа для страницы {page}: {response.status_code}")
        
        # Пытаемся получить JSON ответ
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON для страницы {page}: {e}")
            print(f"Текст ответа: {response.text[:200]}...")
            return None
        
        # Проверяем статус ответа
        if response.status_code == 200:
            leads_count = len(result.get('data', []))
            total_pages = result.get('meta', {}).get('total_pages', 1)
            total_count = result.get('meta', {}).get('total_count', 0)
            print(f"Успешно! Найдено заявок: {leads_count} (всего: {total_count}, страница {page} из {total_pages})")
            return result
        elif response.status_code == 401:
            print("Ошибка авторизации при запросе списка заявок")
            return None
        else:
            print(f"Ошибка API при запросе списка заявок: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сетевого запроса для страницы {page}: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка для страницы {page}: {e}")
        return None

def get_all_leads(filters):
    """
    Получает все заявки с постраничным запросом
    Args:
        filters (dict): Словарь с фильтрами
    Returns:
        list: Список всех заявок
    """
    all_leads = []
    page = 1
    
    print("Начинаем сбор всех заявок...")
    
    while True:
        print(f"\n--- Получение страницы {page} ---")
        
        # Получаем заявки для текущей страницы
        result = get_leads(filters, page)
        
        if not result:
            print(f"Ошибка при получении страницы {page}. Прерывание.")
            break
        
        # Добавляем заявки в общий список
        page_leads = result.get('data', [])
        all_leads.extend(page_leads)
        
        # Проверяем есть ли следующая страница
        total_pages = result.get('meta', {}).get('total_pages', 1)
        
        if page >= total_pages:
            print(f"Достигнута последняя страница ({page}/{total_pages})")
            break
        
        page += 1
        
        # Задержка между запросами
        time.sleep(0.3)
    
    print(f"\nВсего собрано заявок: {len(all_leads)}")
    return all_leads

def ensure_output_directory():
    """
    Создает выходную директорию если она не существует
    Returns:
        str: Путь к выходной директории
    """
    global OUTPUT_BASE_DIR
    
    if not os.path.exists(OUTPUT_BASE_DIR):
        try:
            os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)
            print(f"Создана директория для сохранения: {OUTPUT_BASE_DIR}")
        except Exception as e:
            print(f"Ошибка создания директории {OUTPUT_BASE_DIR}: {e}")
            # Используем текущую директорию как запасной вариант
            OUTPUT_BASE_DIR = "output_leads"
            os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)
            print(f"Используем альтернативную директорию: {OUTPUT_BASE_DIR}")
    
    return OUTPUT_BASE_DIR

def save_leads_to_file(leads_data, filters, filename_prefix="leads"):
    """
    Сохраняет данные заявок в JSON файлы
    Args:
        leads_data (list): Список данных заявок
        filters (dict): Использованные фильтры
        filename_prefix (str): Префикс имени файла
    """
    try:
        # Создаем папку если ее нет
        output_dir = ensure_output_directory()
        
        # Сохраняем все заявки в один файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        all_leads_filename = f"{filename_prefix}_all_{timestamp}.json"
        all_leads_filepath = os.path.join(output_dir, all_leads_filename)
        
        # Создаем структуру с метаданными
        output_data = {
            "meta": {
                "total_count": len(leads_data),
                "exported_at": datetime.now().isoformat(),
                "filters": filters
            },
            "data": leads_data
        }
        
        with open(all_leads_filepath, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"Все заявки сохранены в: {all_leads_filepath}")
        
        # Также сохраняем каждую заявку в отдельный файл
        individual_count = 0
        for lead in leads_data:
            lead_id = lead.get('id', 'unknown')
            lead_filename = f"{filename_prefix}_{lead_id}.json"
            lead_filepath = os.path.join(output_dir, lead_filename)
            
            with open(lead_filepath, 'w', encoding='utf-8') as f:
                json.dump(lead, f, ensure_ascii=False, indent=2)
            
            individual_count += 1
        
        print(f"Отдельные файлы созданы для {individual_count} заявок")
        
        return True
        
    except Exception as e:
        print(f"Ошибка сохранения заявок: {e}")
        return False

def get_lead_by_id(lead_id):
    """
    Получает детальную информацию о заявке по ID
    Args:
        lead_id (int): ID заявки
    Returns:
        dict: Данные заявки или None в случае ошибки
    """
    global API_AUTH_TOKEN
    
    if not API_AUTH_TOKEN:
        return None
        
    try:
        # Формируем URL запроса для получения заявки
        url = f"{API_BASE_URL}/admin/leads/{lead_id}"
        
        # Параметры для включения связанных моделей
        params = {
            "included[]": [
                "user", "admin", "service", "region", "organization", 
                "source", "gas_object_address", "attachments", "contract", 
                "cancellation_reason", "correspondence_address", "rejection_reason",
                "protocol_mismatches", "parent", "children", "mfc_manager", 
                "mfc", "additional_activities"
            ]
        }
        
        # Заголовки запроса
        headers = {
            "Accept": "application/json",
            "Authorization": API_AUTH_TOKEN,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        # Отправляем запрос через прокси
        response = requests.get(
            url,
            params=params,
            headers=headers,
            proxies={
                "http": PROXY_URL,
                "https": PROXY_URL
            },
            timeout=30,
            verify=False
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            return None
            
    except Exception as e:
        print(f"Ошибка получения деталей заявки {lead_id}: {e}")
        return None

def save_lead_details(leads_data):
    """
    Сохраняет детальную информацию по каждой заявке
    Args:
        leads_data (list): Список заявок
    Returns:
        list: Список детальных данных
    """
    print("\nСбор детальной информации по заявкам...")
    
    detailed_leads = []
    success_count = 0
    error_count = 0
    
    for i, lead in enumerate(leads_data, 1):
        lead_id = lead.get('id')
        if not lead_id:
            continue
            
        print(f"Обработка заявки {i}/{len(leads_data)}: ID {lead_id}")
        
        # Получаем детальную информацию по заявке
        detailed_lead = get_lead_by_id(lead_id)
        
        if detailed_lead:
            detailed_leads.append(detailed_lead)
            success_count += 1
            print(f"Детальная информация получена для ID {lead_id}")
        else:
            error_count += 1
            print(f"Ошибка получения детальной информации для ID {lead_id}")
        
        # Задержка между запросами
        if i < len(leads_data):
            time.sleep(0.3)
    
    # Сохраняем детальную информацию
    if detailed_leads:
        output_dir = ensure_output_directory()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        detailed_filename = f"leads_detailed_{timestamp}.json"
        detailed_filepath = os.path.join(output_dir, detailed_filename)
        
        with open(detailed_filepath, 'w', encoding='utf-8') as f:
            json.dump(detailed_leads, f, ensure_ascii=False, indent=2)
        
        print(f"Детальная информация сохранена в: {detailed_filepath}")
        print(f"Успешно: {success_count}, Ошибок: {error_count}")
    
    return detailed_leads

def collect_leads(filters):
    """
    Основная функция сбора заявок
    Args:
        filters (dict): Словарь с настройками фильтров
    """
    print("\n" + "="*50)
    print("ЗАПУСК СБОРА ДАННЫХ")
    print("="*50)
    print(f"Данные будут сохранены в: {OUTPUT_BASE_DIR}")
    
    # Статистика
    stats = {
        'total_leads': 0,
        'success': 0,
        'errors': 0,
        'start_time': datetime.now(),
        'filters': filters,
        'output_directory': OUTPUT_BASE_DIR
    }
    
    # Создаем папку для результатов
    ensure_output_directory()
    
    # Получаем все заявки
    all_leads = get_all_leads(filters)
    
    if not all_leads:
        print("Не удалось получить заявки")
        return
    
    stats['total_leads'] = len(all_leads)
    
    # Сохраняем основные данные
    if save_leads_to_file(all_leads, filters):
        stats['success'] = len(all_leads)
        print(f"Основные данные сохранены для {len(all_leads)} заявок")
    else:
        stats['errors'] = len(all_leads)
        print("Ошибка сохранения основных данных")
    
    # Получаем детальную информацию если требуется
    if filters['get_detailed_info']:
        detailed_leads = save_lead_details(all_leads)
        stats['detailed_leads'] = len(detailed_leads)
    
    # Выводим статистику
    stats['end_time'] = datetime.now()
    stats['duration'] = stats['end_time'] - stats['start_time']
    
    print(f"\n{'='*50}")
    print("СТАТИСТИКА СБОРА ЗАЯВОК:")
    print(f"{'='*50}")
    print(f"Всего заявок: {stats['total_leads']}")
    print(f"Успешно обработано: {stats['success']}")
    print(f"Ошибок: {stats['errors']}")
    if filters['get_detailed_info']:
        print(f"Детальная информация: {stats.get('detailed_leads', 0)}")
    print(f"Папка сохранения: {OUTPUT_BASE_DIR}")
    print(f"Время выполнения: {stats['duration']}")
    print(f"{'='*50}")
    
    # Сохраняем статистику
    stats_file = os.path.join(OUTPUT_BASE_DIR, "leads_stats.json")
    with open(stats_file, 'w', encoding='utf-8') as f:
        stats_json = stats.copy()
        stats_json['start_time'] = stats_json['start_time'].isoformat()
        stats_json['end_time'] = stats_json['end_time'].isoformat()
        stats_json['duration'] = str(stats_json['duration'])
        json.dump(stats_json, f, ensure_ascii=False, indent=2)
    
    print(f"Статистика сохранена в: {stats_file}")

def main():
    """
    Основная функция выполнения скрипта
    """
    print("=== СКРИПТ СБОРА СПИСКА ЗАЯВОК ===")
    print(f"Выходная директория: {OUTPUT_BASE_DIR}")
    
    # Настраиваем прокси
    setup_proxy()
    
    # Тестируем подключение
    if not test_proxy_connection():
        print("Проверка прокси не удалась. Продолжаем с текущими настройками...")
    
    # Получаем токен авторизации
    token = get_auth_token()
    if not token:
        print("Не удалось получить токен авторизации. Завершение работы.")
        return
    
    # Настраиваем фильтры через интерактивный диалог
    filters = configure_filters()
    
    # Подтверждение запуска
    print("\n" + "="*50)
    print("ПОДТВЕРЖДЕНИЕ ЗАПУСКА")
    print("="*50)
    print("Настройки сбора данных:")
    print(f"Сортировка: {filters['order_key']} ({filters['order_type']})")
    print(f"Статусы: {filters['status'] or 'все'}")
    print(f"ID услуги: {filters['service'] or 'не указан'}")
    print(f"Организации: {filters['organization_ids'] or 'все'}")
    print(f"Регионы: {filters['region_ids'] or 'все'}")
    print(f"Дата создания: {filters['date_from']} - {filters['date_to']}")
    print(f"Детальная информация: {'да' if filters['get_detailed_info'] else 'нет'}")
    print(f"Элементов на странице: {filters['per_page']}")
    print(f"Папка сохранения: {OUTPUT_BASE_DIR}")
    print("="*50)
    
    if get_user_input_yes_no("Запустить сбор данных с указанными настройками?"):
        # Запускаем сбор данных
        try:
            collect_leads(filters)
        except KeyboardInterrupt:
            print("\nСбор данных прерван пользователем.")
        except Exception as e:
            print(f"\nКритическая ошибка при сборе данных: {e}")
    else:
        print("Сбор данных отменен пользователем.")

if __name__ == "__main__":
    main()