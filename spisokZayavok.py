import json
import requests
import socket
from pathlib import Path
from datetime import datetime
import os
import urllib3
import pymssql
import time

# ОСНОВНАЯ ПРОГРАММА (функция получения списка заявок)

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

# Глобальная переменная для токена
API_AUTH_TOKEN = None

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

def get_leads_page(page=1, per_page=100, **filters):
    # Получает одну страницу заявок из API
    # Args: 
    #   page (int): номер страницы
    #   per_page (int): количество элементов на странице
    #   filters (dict): дополнительные параметры фильтрации
    # Returns: dict: Ответ от API или None в случае ошибки
    
    global API_AUTH_TOKEN
    
    if not API_AUTH_TOKEN:
        print("Токен авторизации не получен. Сначала выполните авторизацию.")
        return None
        
    try:
        # Формируем URL запроса
        url = f"{API_BASE_URL}/admin/leads"
        
        # Базовые параметры (как в примере из Swagger)
        params = {
            "order_key": "id",
            "order_type": "desc",
            "date_from": "21.01.2010",
            "date_to": "21.01.2030", 
            "updated_at_from": "21.01.2010",
            "updated_at_to": "21.01.2030",
            "send_date_from": "21.01.2010",
            "send_date_to": "21.01.2030",
            "page": page,
            "per": per_page,
            "short_format": "false"
        }
        
        # Добавляем пользовательские фильтры
        params.update(filters)
        
        # Заголовки запроса
        headers = {
            "Accept": "application/json",
            "Authorization": API_AUTH_TOKEN,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        print(f"Запрос страницы {page} (по {per_page} заявок)")
        
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
            
            print(f"Успешно! Страница {page}/{total_pages}")
            print(f"Заявок на странице: {leads_count}")
            print(f"Всего заявок: {total_count}")
            
            return result
        elif response.status_code == 401:
            print(f"Ошибка авторизации для страницы {page}")
            return None
        elif response.status_code == 403:
            print(f"Недостаточно прав для доступа к заявкам")
            return None
        else:
            print(f"Ошибка API для страницы {page}: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сетевого запроса для страницы {page}: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка для страницы {page}: {e}")
        return None

def display_leads_summary(data, page):
    # Красиво отображает сводку по заявкам на странице
    if not data or 'data' not in data:
        print(f"Нет данных для страницы {page}")
        return
    
    leads = data['data']
    meta = data.get('meta', {})
    
    print(f"\n{'='*80}")
    print(f"СТРАНИЦА {page} / {meta.get('total_pages', 'N/A')}")
    print(f"Всего заявок: {meta.get('total_count', 'N/A')}")
    print(f"Заявок на странице: {len(leads)}")
    print(f"{'='*80}")
    
    for i, lead in enumerate(leads, 1):
        attrs = lead.get('attributes', {})
        print(f"\n{i}. Заявка ID: {attrs.get('id', 'N/A')}")
        print(f"   UID: {attrs.get('uid', 'N/A')}")
        print(f"   Статус: {attrs.get('status', 'N/A')}")
        print(f"   Услуга ID: {attrs.get('service_id', 'N/A')}")
        print(f"   Пользователь: {attrs.get('user_full_name', 'N/A')}")
        print(f"   Организация: {attrs.get('organization_name', 'N/A')}")
        print(f"   Администратор: {attrs.get('admin_name', 'N/A')}")
        print(f"   Дата создания: {attrs.get('created_at', 'N/A')}")
        print(f"   Дата отправки: {attrs.get('send_date', 'N/A')}")
        print(f"   {'-'*50}")

def save_leads_page_to_file(data, page, folder="zayavki_spisok"):
    # Сохраняет данные страницы заявок в JSON файл
    try:
        # Создаем папку если не существует
        Path(folder).mkdir(exist_ok=True)
        
        filename = f"{folder}/leads_page_{page:03d}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Данные страницы {page} сохранены в файл: {filename}")
        return filename
    except Exception as e:
        print(f"Ошибка сохранения файла для страницы {page}: {e}")
        return None

def get_all_leads(per_page=100, max_pages=None, **filters):
    # Получает все заявки с пагинацией
    # Args:
    #   per_page (int): количество заявок на странице
    #   max_pages (int): максимальное количество страниц (None - все)
    #   filters (dict): дополнительные параметры фильтрации
    # Returns: tuple: (успешные страницы, общее количество заявок, общее количество страниц)
    
    print(f"\n{'='*80}")
    print(f"ПОЛУЧЕНИЕ ВСЕХ ЗАЯВОК")
    print(f"Элементов на странице: {per_page}")
    print(f"Максимум страниц: {max_pages or 'все'}")
    print(f"Фильтры: {filters}")
    print(f"{'='*80}")
    
    # Получаем первую страницу для определения общего количества
    first_page_data = get_leads_page(1, per_page, **filters)
    
    if not first_page_data:
        print("Не удалось получить первую страницу. Завершение.")
        return 0, 0, 0
    
    # Извлекаем мета-информацию
    meta = first_page_data.get('meta', {})
    total_pages = meta.get('total_pages', 1)
    total_count = meta.get('total_count', 0)
    
    print(f"\nОБЩАЯ ИНФОРМАЦИЯ:")
    print(f"• Всего заявок: {total_count}")
    print(f"• Всего страниц: {total_pages}")
    print(f"• Элементов на странице: {per_page}")
    
    # Ограничиваем количество страниц если указано
    if max_pages and max_pages < total_pages:
        total_pages = max_pages
        print(f"• Ограничение: будет получено {max_pages} страниц")
    
    # Сохраняем первую страницу
    save_leads_page_to_file(first_page_data, 1)
    display_leads_summary(first_page_data, 1)
    
    successful_pages = 1
    all_leads_count = len(first_page_data.get('data', []))
    
    # Получаем остальные страницы
    for page in range(2, total_pages + 1):
        print(f"\n--- Получение страницы {page} ---")
        
        page_data = get_leads_page(page, per_page, **filters)
        
        if page_data:
            save_leads_page_to_file(page_data, page)
            display_leads_summary(page_data, page)
            successful_pages += 1
            all_leads_count += len(page_data.get('data', []))
        else:
            print(f"Не удалось получить страницу {page}. Пропускаем.")
        
        # Небольшая задержка чтобы не перегружать API
        time.sleep(0.5)
    
    # Выводим итоговую статистику
    print(f"\n{'='*80}")
    print("ИТОГИ ПОЛУЧЕНИЯ ЗАЯВОК:")
    print(f"• Всего страниц в системе: {meta.get('total_pages', 'N/A')}")
    print(f"• Всего заявок в системе: {meta.get('total_count', 'N/A')}")
    print(f"• Успешно получено страниц: {successful_pages}")
    print(f"• Получено заявок: {all_leads_count}")
    print(f"• Данные сохранены в папку: zayavki_spisok/")
    print(f"{'='*80}")
    
    return successful_pages, all_leads_count, total_pages

def test_specific_filters():
    # Тестирует работу с конкретными фильтрами
    print(f"\n{'='*80}")
    print(f"ТЕСТИРОВАНИЕ ФИЛЬТРОВ")
    print(f"{'='*80}")
    
    # Примеры фильтров (можно добавлять любые из документации)
    filters = {
        "service": 25,  # ID услуги
        "status[]": ["in_progress", "implementation"],  # Статусы
        "query": "Абраменкова",  # Поисковый запрос
        # "date_from": "01.01.2025",  # Созданы от
        # "date_to": "31.03.2025",    # Созданы до
    }
    
    print(f"Применяемые фильтры: {filters}")
    
    successful, count, pages = get_all_leads(per_page=50, max_pages=3, **filters)
    
    print(f"\nРезультат тестирования фильтров:")
    print(f"Успешных страниц: {successful}")
    print(f"Получено заявок: {count}")
    print(f"Всего страниц: {pages}")

def export_leads_to_single_file(folder="zayavki_spisok", output_file="all_leads_combined.json"):
    # Объединяет все страницы в один файл
    try:
        all_leads = []
        included_data = {}
        page_files = []
        
        # Находим все файлы страниц
        folder_path = Path(folder)
        if not folder_path.exists():
            print(f"Папка {folder} не существует")
            return None
            
        for file_path in folder_path.glob("leads_page_*.json"):
            page_files.append(file_path)
        
        if not page_files:
            print("Файлы страниц не найдены")
            return None
            
        # Сортируем по номеру страницы
        page_files.sort()
        
        print(f"Объединение {len(page_files)} файлов...")
        
        for file_path in page_files:
            with open(file_path, 'r', encoding='utf-8') as f:
                page_data = json.load(f)
                
            # Добавляем заявки
            all_leads.extend(page_data.get('data', []))
            
            # Объединяем включенные данные (included)
            for included_item in page_data.get('included', []):
                item_id = included_item.get('id')
                item_type = included_item.get('type')
                key = f"{item_type}_{item_id}"
                if key not in included_data:
                    included_data[key] = included_item
        
        # Создаем объединенную структуру
        combined_data = {
            "data": all_leads,
            "included": list(included_data.values()),
            "meta": {
                "total_pages": len(page_files),
                "total_count": len(all_leads),
                "combined_at": datetime.now().isoformat()
            }
        }
        
        # Сохраняем объединенный файл
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=2)
        
        print(f"Объединенный файл создан: {output_file}")
        print(f"Всего заявок в объединенном файле: {len(all_leads)}")
        return output_file
        
    except Exception as e:
        print(f"Ошибка объединения файлов: {e}")
        return None

def test_sql_connection():
    # Тестирует подключение к SQL Server
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            port=SQL_PORT,
            user=SQL_USERNAME,
            password=SQL_PASSWORD,
            database=SQL_DATABASE
        )
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()
        print(f"Подключение к SQL Server успешно")
        print(f"Версия сервера: {version[0][:100]}...")
        conn.close()
        return True
    except Exception as e:
        print(f"Ошибка подключения к SQL Server: {e}")
        return False

def main_leads():
    # Основная функция программы для работы со списком заявок
    print("ЗАПУСК ПРОГРАММЫ ПОЛУЧЕНИЯ СПИСКА ЗАЯВОК ИЗ API")
    print("=" * 60)
    
    # Настраиваем прокси
    setup_proxy()
    
    # Тестируем прокси
    if not test_proxy_connection():
        print("Предупреждение: прокси не работает, продолжаем без него...")
    
    # АВТОРИЗАЦИЯ
    print("\n" + "="*50)
    print("АВТОРИЗАЦИЯ В API")
    print("="*50)
    
    token = get_auth_token()
    if not token:
        print("Не удалось выполнить авторизацию. Программа завершена.")
        return
    
    # Меню выбора режима работы
    print("\n" + "="*50)
    print("ВЫБЕРИТЕ РЕЖИМ РАБОТЫ СО СПИСКОМ ЗАЯВОК:")
    print("1. Получить все заявки (с пагинацией)")
    print("2. Тестирование с фильтрами")
    print("3. Объединить сохраненные страницы в один файл")
    print("4. Настроить параметры запроса")
    print("="*50)
    
    choice = input("Введите номер режима (1-4): ").strip()
    
    if choice == "1":
        # Получаем все заявки
        per_page = input("Элементов на странице (по умолчанию 100): ").strip()
        per_page = int(per_page) if per_page else 100
        
        max_pages = input("Максимум страниц (Enter - все): ").strip()
        max_pages = int(max_pages) if max_pages else None
        
        successful, count, pages = get_all_leads(per_page=per_page, max_pages=max_pages)
        
        print(f"\nИтоговый результат:")
        print(f"Успешно получено страниц: {successful}")
        print(f"Получено заявок: {count}")
        print(f"Всего страниц в системе: {pages}")
        
    elif choice == "2":
        # Тестируем фильтры
        test_specific_filters()
        
    elif choice == "3":
        # Объединяем файлы
        export_leads_to_single_file()
        
    elif choice == "4":
        # Настраиваем параметры вручную
        print("\nНастройка параметров запроса:")
        service_id = input("ID услуги (Enter - все): ").strip()
        status = input("Статус (in_progress, implementation и т.д., Enter - все): ").strip()
        date_from = input("Дата от (дд.мм.гггг, Enter - 21.01.2010): ").strip() or "21.01.2010"
        date_to = input("Дата до (дд.мм.гггг, Enter - 21.01.2030): ").strip() or "21.01.2030"
        
        filters = {}
        if service_id:
            filters["service"] = int(service_id)
        if status:
            filters["status[]"] = [status]
        if date_from != "21.01.2010":
            filters["date_from"] = date_from
        if date_to != "21.01.2030":
            filters["date_to"] = date_to
            
        successful, count, pages = get_all_leads(per_page=50, max_pages=5, **filters)
        
    else:
        print("Неверный выбор. Завершение программы.")
        return
        
    print(f"\nПрограмма завершена в {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Запуск программы
if __name__ == "__main__":
    main_leads()