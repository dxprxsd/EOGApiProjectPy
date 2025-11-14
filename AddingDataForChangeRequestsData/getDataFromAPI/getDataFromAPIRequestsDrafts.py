import os
import json
import requests
import urllib3
import time
from datetime import datetime

# Программа для получения данных с функции апи "GET /v1/admin/draft/leads Черновики заявок"

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
OUTPUT_BASE_DIR = "/home/kuzminiv/EOGProjPyApi/AddingDataForChangeRequestsData/getDataFromAPI/outputData/drafts"

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

def configure_filters():
    """
    Настраивает фильтры через интерактивный диалог с пользователем
    Returns:
        dict: Словарь с настройками фильтров
    """
    print("\n" + "="*50)
    print("НАСТРОЙКА ФИЛЬТРОВ")
    print("="*50)
    
    filters = {
        'organization_ids': None,
        'branch_ids': None,
        'get_detailed_info': True,
        'per_page': 100
    }
    
    # Фильтр по организациям
    if get_user_input_yes_no("Хотите указать фильтр по организациям?"):
        filters['organization_ids'] = get_user_input_numbers(
            "Введите ID организаций (через запятую или пробел):"
        )
        if filters['organization_ids']:
            print(f"Будет использован фильтр по организациям: {filters['organization_ids']}")
        else:
            print("Фильтр по организациям отключен")
    
    # Фильтр по филиалам
    if get_user_input_yes_no("Хотите указать фильтр по филиалам?"):
        filters['branch_ids'] = get_user_input_numbers(
            "Введите ID филиалов (через запятую или пробел):"
        )
        if filters['branch_ids']:
            print(f"Будет использован фильтр по филиалам: {filters['branch_ids']}")
        else:
            print("Фильтр по филиалам отключен")
    
    # Детальная информация
    filters['get_detailed_info'] = get_user_input_yes_no(
        "Получать детальную информацию по каждой заявке?"
    )
    
    # Количество элементов на странице
    filters['per_page'] = get_user_input_int(
        "Количество элементов на странице (рекомендуется 50-100):",
        default_value=100
    )
    
    print("\nНастройки завершены!")
    return filters

def get_draft_leads(organization_ids=None, branch_ids=None, page=1, per_page=100):
    """
    Получает список черновиков заявок
    Args:
        organization_ids (list): Список ID организаций
        branch_ids (list): Список ID филиалов
        page (int): Номер страницы
        per_page (int): Количество элементов на странице
    Returns:
        dict: Ответ от API или None в случае ошибки
    """
    global API_AUTH_TOKEN
    
    if not API_AUTH_TOKEN:
        print("Токен авторизации не получен. Сначала выполните авторизацию.")
        return None
        
    try:
        # Формируем URL запроса
        url = f"{API_BASE_URL}/admin/draft/leads"
        
        # Параметры запроса
        params = {
            "page": page,
            "per_page": per_page
        }
        
        # Добавляем фильтры если указаны
        if organization_ids:
            params["organization_ids[]"] = organization_ids
        
        if branch_ids:
            params["branch_ids[]"] = branch_ids
        
        # Заголовки запроса
        headers = {
            "Accept": "application/json",
            "Authorization": API_AUTH_TOKEN,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        print(f"Запрос черновиков заявок (страница {page})...")
        if organization_ids:
            print(f"Фильтр по организациям: {organization_ids}")
        if branch_ids:
            print(f"Фильтр по филиалам: {branch_ids}")
        
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
            verify=False  # Игнорируем SSL проверки
        )
        
        print(f"Статус ответа для черновиков: {response.status_code}")
        
        # Пытаемся получить JSON ответ
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON для черновиков: {e}")
            print(f"Текст ответа: {response.text[:200]}...")
            return None
        
        # Проверяем статус ответа
        if response.status_code == 200:
            leads_count = len(result.get('data', []))
            total_pages = result.get('meta', {}).get('total_pages', 1)
            print(f"Успешно! Найдено черновиков: {leads_count} (страница {page} из {total_pages})")
            return result
        elif response.status_code == 401:
            print("Ошибка авторизации при запросе черновиков")
            return None
        else:
            print(f"Ошибка API при запросе черновиков: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сетевого запроса для черновиков: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка для черновиков: {e}")
        return None

def get_all_draft_leads(organization_ids=None, branch_ids=None, per_page=100):
    """
    Получает все черновики заявок с постраничным запросом
    Args:
        organization_ids (list): Список ID организаций
        branch_ids (list): Список ID филиалов
        per_page (int): Количество элементов на странице
    Returns:
        list: Список всех черновиков заявок
    """
    all_leads = []
    page = 1
    
    print("Начинаем сбор всех черновиков заявок...")
    
    while True:
        print(f"\n--- Получение страницы {page} ---")
        
        # Получаем черновики для текущей страницы
        result = get_draft_leads(organization_ids, branch_ids, page, per_page)
        
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
    
    print(f"\nВсего собрано черновиков: {len(all_leads)}")
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
            OUTPUT_BASE_DIR = "output_drafts"
            os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)
            print(f"Используем альтернативную директорию: {OUTPUT_BASE_DIR}")
    
    return OUTPUT_BASE_DIR

def save_draft_leads_to_file(leads_data, filename_prefix="draft_leads"):
    """
    Сохраняет данные черновиков заявок в JSON файлы
    Args:
        leads_data (list): Список данных черновиков
        filename_prefix (str): Префикс имени файла
    """
    try:
        # Создаем папку если ее нет
        output_dir = ensure_output_directory()
        
        # Сохраняем все черновики в один файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        all_leads_filename = f"{filename_prefix}_all_{timestamp}.json"
        all_leads_filepath = os.path.join(output_dir, all_leads_filename)
        
        with open(all_leads_filepath, 'w', encoding='utf-8') as f:
            json.dump(leads_data, f, ensure_ascii=False, indent=2)
        
        print(f"Все черновики сохранены в: {all_leads_filepath}")
        
        # Также сохраняем каждый черновик в отдельный файл
        individual_count = 0
        for lead in leads_data:
            lead_id = lead.get('id', 'unknown')
            lead_filename = f"{filename_prefix}_{lead_id}.json"
            lead_filepath = os.path.join(output_dir, lead_filename)
            
            with open(lead_filepath, 'w', encoding='utf-8') as f:
                json.dump(lead, f, ensure_ascii=False, indent=2)
            
            individual_count += 1
        
        print(f"Отдельные файлы созданы для {individual_count} черновиков")
        
        return True
        
    except Exception as e:
        print(f"Ошибка сохранения черновиков: {e}")
        return False

def save_lead_details(leads_data):
    """
    Сохраняет детальную информацию по каждому черновику
    Args:
        leads_data (list): Список черновиков
    Returns:
        list: Список детальных данных
    """
    print("\nСбор детальной информации по черновикам...")
    
    detailed_leads = []
    success_count = 0
    error_count = 0
    
    for i, lead in enumerate(leads_data, 1):
        lead_id = lead.get('id')
        if not lead_id:
            continue
            
        print(f"Обработка черновика {i}/{len(leads_data)}: ID {lead_id}")
        
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
        detailed_filename = f"draft_leads_detailed_{timestamp}.json"
        detailed_filepath = os.path.join(output_dir, detailed_filename)
        
        with open(detailed_filepath, 'w', encoding='utf-8') as f:
            json.dump(detailed_leads, f, ensure_ascii=False, indent=2)
        
        print(f"Детальная информация сохранена в: {detailed_filepath}")
        print(f"Успешно: {success_count}, Ошибок: {error_count}")
    
    return detailed_leads

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
        # Формируем URL запроса для получения заявке
        url = f"{API_BASE_URL}/admin/leads/{lead_id}"
        
        # Заголовки запроса
        headers = {
            "Accept": "application/json",
            "Authorization": API_AUTH_TOKEN,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        # Отправляем запрос через прокси
        response = requests.get(
            url,
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

def collect_draft_leads(filters):
    """
    Основная функция сбора черновиков заявок
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
    
    # Получаем все черновики
    all_draft_leads = get_all_draft_leads(
        organization_ids=filters['organization_ids'],
        branch_ids=filters['branch_ids'],
        per_page=filters['per_page']
    )
    
    if not all_draft_leads:
        print("Не удалось получить черновики заявок")
        return
    
    stats['total_leads'] = len(all_draft_leads)
    
    # Сохраняем основные данные
    if save_draft_leads_to_file(all_draft_leads):
        stats['success'] = len(all_draft_leads)
        print(f"Основные данные сохранены для {len(all_draft_leads)} черновиков")
    else:
        stats['errors'] = len(all_draft_leads)
        print("Ошибка сохранения основных данных")
    
    # Получаем детальную информацию если требуется
    if filters['get_detailed_info']:
        detailed_leads = save_lead_details(all_draft_leads)
        stats['detailed_leads'] = len(detailed_leads)
    
    # Выводим статистику
    stats['end_time'] = datetime.now()
    stats['duration'] = stats['end_time'] - stats['start_time']
    
    print(f"\n{'='*50}")
    print("СТАТИСТИКА СБОРА ЧЕРНОВИКОВ:")
    print(f"{'='*50}")
    print(f"Всего черновиков: {stats['total_leads']}")
    print(f"Успешно обработано: {stats['success']}")
    print(f"Ошибок: {stats['errors']}")
    if filters['get_detailed_info']:
        print(f"Детальная информация: {stats.get('detailed_leads', 0)}")
    print(f"Папка сохранения: {OUTPUT_BASE_DIR}")
    print(f"Время выполнения: {stats['duration']}")
    print(f"{'='*50}")
    
    # Сохраняем статистику
    stats_file = os.path.join(OUTPUT_BASE_DIR, "draft_leads_stats.json")
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
    print("=== СКРИПТ СБОРА ЧЕРНОВИКОВ ЗАЯВОК ===")
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
    print(f"Фильтр по организациям: {filters['organization_ids'] or 'нет'}")
    print(f"Фильтр по филиалам: {filters['branch_ids'] or 'нет'}")
    print(f"Детальная информация: {'да' if filters['get_detailed_info'] else 'нет'}")
    print(f"Элементов на странице: {filters['per_page']}")
    print(f"Папка сохранения: {OUTPUT_BASE_DIR}")
    print("="*50)
    
    if get_user_input_yes_no("Запустить сбор данных с указанными настройками?"):
        # Запускаем сбор данных
        try:
            collect_draft_leads(filters)
        except KeyboardInterrupt:
            print("\nСбор данных прерван пользователем.")
        except Exception as e:
            print(f"\nКритическая ошибка при сборе данных: {e}")
    else:
        print("Сбор данных отменен пользователем.")

if __name__ == "__main__":
    main()