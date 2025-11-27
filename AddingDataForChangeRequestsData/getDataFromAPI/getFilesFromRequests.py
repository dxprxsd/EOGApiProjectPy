import os
import json
import requests
import urllib3
import time
from datetime import datetime
from typing import Optional, Dict, Any

# Программа для получения файлов заявки 

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
REQUESTS_DATA_DIR = "/home/kuzminiv/EOGProjPyApi/AddingDataForChangeRequestsData/getDataFromAPI/outputData/requestsData"

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
            verify=False
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

def ensure_directory(directory_path: str) -> str:
    """
    Создает директорию если она не существует
    Args:
        directory_path (str): Путь к директории
    Returns:
        str: Путь к созданной директории
    """
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path, exist_ok=True)
            print(f"Создана директория: {directory_path}")
        except Exception as e:
            print(f"Ошибка создания директории {directory_path}: {e}")
            # Используем текущую директорию как запасной вариант
            directory_path = "output_data"
            os.makedirs(directory_path, exist_ok=True)
            print(f"Используем альтернативную директорию: {directory_path}")
    
    return directory_path

class LeadFilesDownloader:
    """Класс для скачивания файлов заявок"""
    
    def __init__(self):
        self.session = None
        self.setup_session()
    
    def setup_session(self):
        """Настройка сессии с прокси и заголовками"""
        setup_proxy()
        
        self.session = requests.Session()
        
        # Настраиваем прокси для сессии
        proxies = {
            'http': PROXY_URL,
            'https': PROXY_URL
        }
        self.session.proxies.update(proxies)
        
        # Устанавливаем заголовки
        self.session.headers.update({
            'Authorization': API_AUTH_TOKEN,
            'accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Отключаем проверку SSL
        self.session.verify = False
        
        print("Сессия для скачивания файлов настроена")
    
    def log_progress(self, message: str, current: int = None, total: int = None):
        """Логирование прогресса с временными метками"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        if current is not None and total is not None:
            progress = f" [{current}/{total}]"
            percentage = f" ({current/total*100:.1f}%)"
            print(f"[{timestamp}] {message}{progress}{percentage}")
        else:
            print(f"[{timestamp}] {message}")
    
    def download_lead_files(self, lead_id: int) -> Dict[str, Any]:
        """
        Скачивание архива документов для конкретной заявки
        Args:
            lead_id (int): ID заявки
        Returns:
            dict: Результат операции
        """
        if not self.session:
            self.setup_session()
        
        url = f"{API_BASE_URL}/admin/leads/{lead_id}.zip"
        
        self.log_progress(f"Скачивание файлов для заявки ID: {lead_id}")
        self.log_progress(f"URL: {url}")
        
        result = {
            'success': False,
            'lead_id': lead_id,
            'file_path': None,
            'file_size': 0,
            'error': None,
            'status_code': None
        }
        
        try:
            response = self.session.get(url, stream=True)
            result['status_code'] = response.status_code
            
            if response.status_code == 200:
                # Создаем папку для сохранения если не существует
                ensure_directory(REQUESTS_DATA_DIR)
                
                # Определяем имя файла для сохранения
                filename = f"lead_{lead_id}_documents.zip"
                filepath = os.path.join(REQUESTS_DATA_DIR, filename)
                
                # Сохраняем файл
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Проверяем размер файла
                file_size = os.path.getsize(filepath)
                result.update({
                    'success': True,
                    'file_path': filepath,
                    'file_size': file_size
                })
                
                self.log_progress(f"Файл успешно сохранен: {filename} ({file_size} байт)")
                
            elif response.status_code == 401:
                error_msg = f"Ошибка 401: Неавторизованный доступ для заявки {lead_id}"
                result['error'] = error_msg
                self.log_progress(error_msg)
                self.log_progress("Проверьте токен авторизации")
                
            elif response.status_code == 403:
                error_msg = f"Ошибка 403: Недостаточно прав для заявки {lead_id}"
                result['error'] = error_msg
                self.log_progress(error_msg)
                
            elif response.status_code == 404:
                error_msg = f"Ошибка 404: Заявка {lead_id} не найдена или архив отсутствует"
                result['error'] = error_msg
                self.log_progress(error_msg)
                
            else:
                error_msg = f"Ошибка {response.status_code} для заявки {lead_id}"
                result['error'] = error_msg
                self.log_progress(error_msg)
                try:
                    error_response = response.json()
                    self.log_progress(f"Ответ сервера: {error_response}")
                except:
                    self.log_progress(f"Текст ответа: {response.text[:200]}...")
                    
        except requests.exceptions.RequestException as e:
            error_msg = f"Ошибка сети при скачивании заявки {lead_id}: {e}"
            result['error'] = error_msg
            self.log_progress(error_msg)
        except Exception as e:
            error_msg = f"Неожиданная ошибка при скачивании заявки {lead_id}: {e}"
            result['error'] = error_msg
            self.log_progress(error_msg)
        
        return result
    
    def download_multiple_leads_files(self, lead_ids: list, delay: float = 0.5) -> Dict[str, Any]:
        """
        Скачивание архивов для нескольких заявок
        Args:
            lead_ids (list): Список ID заявок
            delay (float): Задержка между запросами в секундах
        Returns:
            dict: Статистика скачивания
        """
        if not lead_ids:
            return {'error': 'Список ID заявок пуст'}
        
        self.log_progress(f"Начало массового скачивания файлов для {len(lead_ids)} заявок")
        
        stats = {
            'total': len(lead_ids),
            'successful': 0,
            'failed': 0,
            'total_size': 0,
            'results': [],
            'start_time': datetime.now()
        }
        
        for i, lead_id in enumerate(lead_ids, 1):
            self.log_progress(f"Обработка заявки {i}/{len(lead_ids)}: ID {lead_id}")
            
            # Скачиваем файлы для заявки
            result = self.download_lead_files(lead_id)
            stats['results'].append(result)
            
            if result['success']:
                stats['successful'] += 1
                stats['total_size'] += result['file_size']
            else:
                stats['failed'] += 1
            
            # Задержка между запросами (если это не последний запрос)
            if i < len(lead_ids):
                time.sleep(delay)
        
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
        
        # Сохраняем статистику
        self.save_download_stats(stats)
        
        return stats
    
    def save_download_stats(self, stats: Dict[str, Any]):
        """Сохраняет статистику скачивания в файл"""
        try:
            stats_file = os.path.join(REQUESTS_DATA_DIR, "download_statistics.json")
            
            # Конвертируем несериализуемые объекты
            serializable_stats = stats.copy()
            serializable_stats['start_time'] = serializable_stats['start_time'].isoformat()
            serializable_stats['end_time'] = serializable_stats['end_time'].isoformat()
            serializable_stats['duration'] = str(serializable_stats['duration'])
            
            # Убираем подробные результаты если их много
            if len(serializable_stats['results']) > 100:
                serializable_stats['results'] = f"Подробные результаты скрыты (всего: {len(stats['results'])})"
            
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(serializable_stats, f, ensure_ascii=False, indent=2)
            
            self.log_progress(f"Статистика скачивания сохранена в: {stats_file}")
            
        except Exception as e:
            self.log_progress(f"Ошибка сохранения статистики: {e}")

def get_user_input_yes_no(question: str) -> bool:
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

def get_user_input_numbers(question: str) -> list:
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

def get_user_input_int(question: str, default_value: Optional[int] = None) -> int:
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

def download_single_lead_interactive():
    """Интерактивная функция для скачивания файлов одной заявки"""
    print("\n" + "="*50)
    print("СКАЧИВАНИЕ ФАЙЛОВ ОДНОЙ ЗАЯВКИ")
    print("="*50)
    
    lead_id = get_user_input_int("Введите ID заявки для скачивания файлов")
    
    if not API_AUTH_TOKEN:
        print("Токен авторизации не получен. Сначала выполните авторизацию.")
        return
    
    downloader = LeadFilesDownloader()
    result = downloader.download_lead_files(lead_id)
    
    print("\n" + "="*50)
    print("РЕЗУЛЬТАТ СКАЧИВАНИЯ:")
    print("="*50)
    if result['success']:
        print(f"✓ УСПЕХ: Файлы заявки {lead_id} сохранены")
        print(f"  Путь: {result['file_path']}")
        print(f"  Размер: {result['file_size']} байт")
    else:
        print(f"✗ ОШИБКА: Не удалось скачать файлы заявки {lead_id}")
        print(f"  Причина: {result['error']}")
        print(f"  Код статуса: {result['status_code']}")

def download_multiple_leads_interactive():
    """Интерактивная функция для скачивания файлов нескольких заявок"""
    print("\n" + "="*50)
    print("СКАЧИВАНИЕ ФАЙЛОВ НЕСКОЛЬКИХ ЗАЯВОК")
    print("="*50)
    
    lead_ids = get_user_input_numbers("Введите ID заявок через запятую или пробел")
    
    if not lead_ids:
        print("Список ID заявок пуст. Операция отменена.")
        return
    
    if not API_AUTH_TOKEN:
        print("Токен авторизации не получен. Сначала выполните авторизацию.")
        return
    
    delay = get_user_input_int("Задержка между запросами (секунды)", default_value=1)
    
    print(f"\nНачинаем скачивание файлов для {len(lead_ids)} заявок...")
    print(f"Файлы будут сохранены в: {REQUESTS_DATA_DIR}")
    
    downloader = LeadFilesDownloader()
    stats = downloader.download_multiple_leads_files(lead_ids, delay)
    
    print("\n" + "="*50)
    print("СТАТИСТИКА СКАЧИВАНИЯ:")
    print("="*50)
    print(f"Всего заявок: {stats['total']}")
    print(f"Успешно: {stats['successful']}")
    print(f"Ошибок: {stats['failed']}")
    print(f"Общий размер: {stats['total_size']} байт")
    print(f"Время выполнения: {stats['duration']}")
    print(f"Папка сохранения: {REQUESTS_DATA_DIR}")

def download_from_file_interactive():
    """Скачивание файлов для заявок из JSON файла"""
    print("\n" + "="*50)
    print("СКАЧИВАНИЕ ФАЙЛОВ ДЛЯ ЗАЯВОК ИЗ ФАЙЛА")
    print("="*50)
    
    filename = input("Введите имя JSON файла с данными заявок: ").strip()
    
    if not filename:
        print("Имя файла не указано. Операция отменена.")
        return
    
    filepath = os.path.join(OUTPUT_BASE_DIR, filename)
    if not os.path.exists(filepath):
        print(f"Файл {filepath} не найден.")
        return
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Извлекаем ID заявок из данных
        lead_ids = []
        if isinstance(data, list):
            # Если файл содержит простой список заявок
            for item in data:
                if isinstance(item, dict) and 'id' in item:
                    lead_ids.append(item['id'])
        elif isinstance(data, dict):
            # Если файл содержит структурированные данные
            if 'data' in data and isinstance(data['data'], list):
                for item in data['data']:
                    if isinstance(item, dict) and 'id' in item:
                        lead_ids.append(item['id'])
            elif 'leads' in data and isinstance(data['leads'], list):
                for item in data['leads']:
                    if isinstance(item, dict) and 'id' in item:
                        lead_ids.append(item['id'])
        
        if not lead_ids:
            print("В файле не найдены ID заявок.")
            return
        
        print(f"Найдено {len(lead_ids)} заявок в файле.")
        
        if get_user_input_yes_no("Начать скачивание файлов для этих заявок?"):
            delay = get_user_input_int("Задержка между запросами (секунды)", default_value=1)
            
            downloader = LeadFilesDownloader()
            stats = downloader.download_multiple_leads_files(lead_ids, delay)
            
            print("\n" + "="*50)
            print("СТАТИСТИКА СКАЧИВАНИЯ:")
            print("="*50)
            print(f"Всего заявок: {stats['total']}")
            print(f"Успешно: {stats['successful']}")
            print(f"Ошибок: {stats['failed']}")
            print(f"Общий размер: {stats['total_size']} байт")
            print(f"Время выполнения: {stats['duration']}")
    
    except Exception as e:
        print(f"Ошибка чтения файла: {e}")

def main_files_downloader():
    """Основная функция менеджера скачивания файлов"""
    print("=== СКАЧИВАНИЕ ФАЙЛОВ ЗАЯВОК ===")
    print(f"Папка сохранения: {REQUESTS_DATA_DIR}")
    
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
    
    # Создаем папку для сохранения
    ensure_directory(REQUESTS_DATA_DIR)
    
    while True:
        print("\n" + "="*50)
        print("МЕНЮ СКАЧИВАНИЯ ФАЙЛОВ ЗАЯВОК")
        print("="*50)
        print("1. Скачать файлы одной заявки")
        print("2. Скачать файлы нескольких заявок")
        print("3. Скачать файлы заявок из JSON файла")
        print("4. Выйти")
        print("="*50)
        
        choice = input("Выберите действие (1-4): ").strip()
        
        if choice == '1':
            download_single_lead_interactive()
        elif choice == '2':
            download_multiple_leads_interactive()
        elif choice == '3':
            download_from_file_interactive()
        elif choice == '4':
            print("Завершение работы.")
            break
        else:
            print("Неверный выбор. Пожалуйста, выберите от 1 до 4.")
        
        if choice in ['1', '2', '3']:
            input("\nНажмите Enter для продолжения...")

if __name__ == "__main__":
    main_files_downloader()