import requests
import json
import urllib3
from datetime import datetime
import os

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Настройки
API_BASE_URL = "https://tpsg.etpgpb.ru/v1"
TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxODQiLCJzY3AiOiJ2MV9hZG1pbiIsImF1ZCI6bnVsbCwiaWF0IjoxNzU3MDU3OTIwLCJleHAiOjE3NTk2ODc2NjYsImp0aSI6ImE1M2MyZjgwLWNhNWEtNDczMy1iMmYwLWVkMWM3MGZlMmE4OSJ9.pD4u28LZkxaC7gf7jocrZmYfp1V8TwgnG7_tIYqB70w"

class AppealsAPI:
    def __init__(self, base_url, token):
        self.base_url = base_url
        self.token = token
        self.session = requests.Session()
        self.setup_session()
    
    def setup_session(self):
        """Настройка сессии с обходом проблем с прокси"""
        self.session.headers.update({
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'Appeals-API-Client/1.0'
        })
        
        # Отключаем проверку SSL
        self.session.verify = False
        
        # Отключаем использование прокси для этого домена
        self.session.trust_env = False
        
        # Альтернативно: очищаем переменные окружения прокси
        os.environ.pop('HTTP_PROXY', None)
        os.environ.pop('HTTPS_PROXY', None)
        os.environ.pop('http_proxy', None)
        os.environ.pop('https_proxy', None)
    
    def test_connection(self):
        """Тестирование подключения"""
        try:
            response = self.session.get(
                f"{self.base_url}/admin/appeals",
                params={'page': 1, 'per': 1},
                timeout=10
            )
            
            if response.status_code == 200:
                return True, "Подключение успешно"
            else:
                return False, f"Ошибка {response.status_code}: {response.text}"
                
        except requests.exceptions.ProxyError as e:
            # Пробуем без прокси
            try:
                print("Обнаружена проблема с прокси. Пробуем прямое соединение...")
                response = requests.get(
                    f"{self.base_url}/admin/appeals",
                    params={'page': 1, 'per': 1},
                    headers={
                        'Authorization': f'Bearer {self.token}',
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    },
                    verify=False,
                    timeout=10
                )
                
                if response.status_code == 200:
                    return True, "Подключение успешно (прямое соединение)"
                else:
                    return False, f"Ошибка {response.status_code}: {response.text}"
                    
            except Exception as e2:
                return False, f"Ошибка прямого соединения: {e2}"
                
        except requests.exceptions.RequestException as e:
            return False, f"Ошибка соединения: {e}"
    
    def get_appeals_page(self, page=1, per_page=50, filters=None):
        """Получение одной страницы обращений"""
        params = {
            'page': page,
            'per': per_page
        }
        
        if filters:
            params.update(filters)
        
        try:
            response = self.session.get(
                f"{self.base_url}/admin/appeals",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Ошибка API: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.ProxyError:
            # Пробуем прямое соединение при ошибке прокси
            print("Проблема с прокси. Пробуем прямое соединение...")
            try:
                response = requests.get(
                    f"{self.base_url}/admin/appeals",
                    params=params,
                    headers={
                        'Authorization': f'Bearer {self.token}',
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    },
                    verify=False,
                    timeout=30
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    print(f"Ошибка API при прямом соединении: {response.status_code} - {response.text}")
                    return None
                    
            except Exception as e:
                print(f"Ошибка прямого соединения: {e}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса: {e}")
            return None
    
    def get_all_appeals(self, per_page=50, max_pages=None, filters=None):
        """Получение всех обращений с пагинацией"""
        all_appeals = []
        page = 1
        
        while True:
            print(f"Получение страницы {page}...")
            
            data = self.get_appeals_page(page, per_page, filters)
            
            if not data or 'data' not in data:
                print("Нет данных или неверный формат ответа")
                break
                
            appeals = data['data']
            if not appeals:
                print("Больше данных нет")
                break
                
            all_appeals.extend(appeals)
            print(f"Получено {len(appeals)} обращений")
            
            # Проверяем пагинацию
            meta = data.get('meta', {})
            total_pages = meta.get('total_pages', 1)
            
            if page >= total_pages:
                print(f"Достигнута последняя страница. Всего страниц: {total_pages}")
                break
            
            # Ограничение количества страниц если указано
            if max_pages and page >= max_pages:
                print(f"Достигнуто ограничение в {max_pages} страниц")
                break
                
            page += 1
        
        return all_appeals

    def get_appeals_by_category(self, category_ids, per_page=50):
        """Получить обращения по категориям"""
        filters = {
            'category_ids[]': category_ids,
            'included[]': ['user', 'organization', 'subject', 'category']
        }
        return self.get_all_appeals(per_page=per_page, filters=filters)

    def search_appeals(self, search_query, per_page=50):
        """Поиск обращений"""
        if len(search_query) < 3:
            print("Поисковый запрос должен содержать минимум 3 символа")
            return []
        
        filters = {
            'query': search_query,
            'included[]': ['user', 'organization', 'subject', 'category']
        }
        return self.get_all_appeals(per_page=per_page, filters=filters)

# Функция для анализа токена
def analyze_token(token):
    """Анализ JWT токена"""
    try:
        import base64
        import json
        
        parts = token.split('.')
        if len(parts) != 3:
            print("Неверный формат JWT токена")
            return False
        
        # Декодируем payload (вторая часть токена)
        payload_json = base64.b64decode(parts[1] + '==').decode('utf-8')
        payload = json.loads(payload_json)
        
        print("Информация о токене:")
        print(f"Subject: {payload.get('sub')}")
        print(f"Scope: {payload.get('scp')}")
        
        exp_timestamp = payload.get('exp')
        if exp_timestamp:
            exp_time = datetime.fromtimestamp(exp_timestamp)
            now = datetime.now()
            if exp_time < now:
                print(f"Токен истек: {exp_time}")
                return False
            else:
                print(f"Токен действителен до: {exp_time}")
                return True
        else:
            print("Срок действия токена не указан")
            return True
            
    except Exception as e:
        print(f"Ошибка анализа токена: {e}")
        return False

# Функция для проверки сетевых настроек
def check_network_settings():
    """Проверка сетевых настроек"""
    print("\nПроверка сетевых настроек...")
    
    # Проверяем переменные окружения прокси
    proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']
    for var in proxy_vars:
        if var in os.environ:
            print(f"Обнаружена переменная прокси {var}: {os.environ[var]}")
        else:
            print(f"Переменная прокси {var}: не установлена")

# Основная программа
if __name__ == "__main__":
    print("=" * 60)
    print("ПРОГРАММА ДЛЯ ПОЛУЧЕНИЯ ОБРАЩЕНИЙ ИЗ API")
    print("=" * 60)
    
    # Проверяем сетевые настройки
    check_network_settings()
    
    # Анализируем токен
    print("\n1. Анализ токена...")
    is_token_valid = analyze_token(TOKEN)
    
    if not is_token_valid:
        print("ВНИМАНИЕ: Токен может быть недействителен!")
        print("Рекомендуется получить новый токен авторизации.")
    
    # Создаем клиент API
    print("\n2. Создание API клиента...")
    api_client = AppealsAPI(API_BASE_URL, TOKEN)
    
    # Тестируем подключение
    print("\n3. Тестирование подключения к API...")
    success, message = api_client.test_connection()
    print(f"Результат: {message}")
    
    if success:
        # Получаем данные
        print("\n4. Получение обращений...")
        
        # Для теста получаем ограниченное количество
        appeals = api_client.get_all_appeals(per_page=10, max_pages=2)
        
        if appeals:
            print(f"Успешно получено {len(appeals)} обращений")
            
            # Сохраняем данные в файл
            output_file = 'appeals_data.json'
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(appeals, f, ensure_ascii=False, indent=2)
            print(f"Данные сохранены в файл: {output_file}")
            
            # Выводим статистику
            print("\nСтатистика:")
            print(f"Всего обращений: {len(appeals)}")
            
            # Пример первого обращения
            if appeals:
                first_appeal = appeals[0]
                attributes = first_appeal.get('attributes', {})
                print(f"\nПример первого обращения:")
                print(f"ID: {first_appeal.get('id')}")
                print(f"Email: {attributes.get('email')}")
                print(f"Телефон: {attributes.get('phone')}")
                print(f"Дата создания: {attributes.get('created_at')}")
                
        else:
            print("Не удалось получить обращения")
    else:
        print("\nНе удалось подключиться к API. Возможные решения:")
        print("1. Отключите прокси в настройках системы")
        print("2. Используйте VPN")
        print("3. Проверьте интернет-соединение")
        print("4. Убедитесь, что домен tpsg.etpgpb.ru доступен")
    
    print("\n" + "=" * 60)
    print("ЗАВЕРШЕНО")
    print("=" * 60)