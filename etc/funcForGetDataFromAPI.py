import requests
import json
import urllib3
from datetime import datetime
import base64

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
TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxODQiLCJzY3AiOiJ2MV9hZG1pbiIsImF1ZCI6bnVsbCwiaWF0IjoxNzU3MDU3OTIwLCJleHAiOjE3NTk2ODc2NjYsImp0aSI6ImE1M2MyZjgwLWNhNWEtNDczMy1iMmYwLWVkMWM3MGZlMmE4OSJ9.pD4u28LZkxaC7gf7jocrZmYfp1V8TwgnG7_tIYqB70w"

def test_different_auth_methods():
    """Тестируем разные методы авторизации"""
    print("=" * 70)
    print("ТЕСТИРОВАНИЕ РАЗНЫХ МЕТОДОВ АВТОРИЗАЦИИ")
    print("=" * 70)
    
    # Базовые настройки
    proxies = {'http': PROXY_URL, 'https': PROXY_URL}
    params = {'page': 1, 'per': 2}
    url = f"{API_BASE_URL}/admin/appeals/consultations"
    
    # Методы авторизации для тестирования
    auth_methods = [
        {
            'name': 'Bearer Token (стандартный)',
            'headers': {'Authorization': f'Bearer {TOKEN}'}
        },
        {
            'name': 'Basic Auth с токеном как паролем',
            'headers': {'Authorization': f'Basic {base64.b64encode(f":{TOKEN}".encode()).decode()}'}
        },
        {
            'name': 'Basic Auth с admin:token',
            'headers': {'Authorization': f'Basic {base64.b64encode(f"admin:{TOKEN}".encode()).decode()}'}
        },
        {
            'name': 'Basic Auth с token:token',
            'headers': {'Authorization': f'Basic {base64.b64encode(f"{TOKEN}:{TOKEN}".encode()).decode()}'}
        },
        {
            'name': 'X-Auth-Token header',
            'headers': {'X-Auth-Token': TOKEN}
        },
        {
            'name': 'X-Authorization header',
            'headers': {'X-Authorization': f'Bearer {TOKEN}'}
        },
        {
            'name': 'Token в параметрах URL (token)',
            'params': {'token': TOKEN, 'page': 1, 'per': 2},
            'headers': {}
        },
        {
            'name': 'Token в параметрах URL (access_token)',
            'params': {'access_token': TOKEN, 'page': 1, 'per': 2},
            'headers': {}
        },
        {
            'name': 'Token в параметрах URL (auth_token)',
            'params': {'auth_token': TOKEN, 'page': 1, 'per': 2},
            'headers': {}
        },
        {
            'name': 'Token в URL path',
            'url': f"{API_BASE_URL}/admin/appeals/consultations?token={TOKEN}",
            'headers': {}
        },
    ]
    
    # Общие заголовки
    common_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    successful_methods = []
    
    for i, method in enumerate(auth_methods, 1):
        print(f"\n{i}. {method['name']}")
        print("-" * 50)
        
        try:
            # Подготавливаем параметры запроса
            request_headers = common_headers.copy()
            request_headers.update(method.get('headers', {}))
            
            request_params = params.copy()
            request_params.update(method.get('params', {}))
            
            request_url = method.get('url', url)
            
            print(f"URL: {request_url}")
            print(f"Headers: {request_headers}")
            print(f"Params: {request_params}")
            
            response = requests.get(
                request_url,
                headers=request_headers,
                params=request_params,
                proxies=proxies,
                verify=False,
                timeout=30
            )
            
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                print("УСПЕХ!")
                successful_methods.append(method['name'])
                
                # Показываем немного данных
                data = response.json()
                if 'data' in data:
                    print(f"Получено {len(data['data'])} обращений")
                    
            elif response.status_code == 403:
                print("Ошибка 403: Не хватает прав")
                try:
                    error_data = response.json()
                    print(f"Детали: {error_data}")
                except:
                    print(f"Текст: {response.text}")
                    
            elif response.status_code == 401:
                print("Ошибка 401: Неавторизован")
                try:
                    error_data = response.json()
                    print(f"Детали: {error_data}")
                except:
                    print(f"Текст: {response.text}")
            else:
                print(f"Ошибка {response.status_code}")
                print(f"Текст: {response.text[:200]}...")
                
        except Exception as e:
            print(f"Ошибка запроса: {e}")
    
    # Итоги
    print("\n" + "=" * 70)
    print("ИТОГИ ТЕСТИРОВАНИЯ")
    print("=" * 70)
    
    if successful_methods:
        print("УСПЕШНЫЕ МЕТОДЫ:")
        for method in successful_methods:
            print(f"  - {method}")
    else:
        print("Ни один метод авторизации не сработал")

def test_with_token_in_url():
    """Тест с токеном прямо в URL"""
    print("\n" + "=" * 70)
    print("ТЕСТ С ТОКОНОМ В URL")
    print("=" * 70)
    
    proxies = {'http': PROXY_URL, 'https': PROXY_URL}
    
    # Разные варианты URL с токеном
    url_variants = [
        f"{API_BASE_URL}/admin/appeals/consultations?access_token={TOKEN}",
        f"{API_BASE_URL}/admin/appeals/consultations?token={TOKEN}",
        f"{API_BASE_URL}/admin/appeals/consultations?auth_token={TOKEN}",
        f"{API_BASE_URL}/admin/appeals/consultations?api_key={TOKEN}",
    ]
    
    for i, url in enumerate(url_variants, 1):
        print(f"\n{i}. URL: {url[:100]}...")
        
        try:
            response = requests.get(
                url,
                params={'page': 1, 'per': 2},
                proxies=proxies,
                verify=False,
                timeout=30
            )
            
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                print("УСПЕХ!")
                data = response.json()
                print(f"Получено {len(data.get('data', []))} обращений")
            else:
                print(f"Ошибка: {response.status_code}")
                
        except Exception as e:
            print(f"Ошибка: {e}")

def check_token_validity():
    """Проверяем валидность токена"""
    print("\n" + "=" * 70)
    print("ПРОВЕРКА ТОКЕНА")
    print("=" * 70)
    
    # Анализируем JWT токен
    try:
        parts = TOKEN.split('.')
        if len(parts) == 3:
            header, payload, signature = parts
            
            # Декодируем payload
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += '=' * padding
                
            payload_json = base64.b64decode(payload).decode('utf-8')
            payload_data = json.loads(payload_json)
            
            print("JWT Token Analysis:")
            print(f"  Subject (sub): {payload_data.get('sub')}")
            print(f"  Scope (scp): {payload_data.get('scp')}")
            
            # Проверяем срок действия
            if 'exp' in payload_data:
                exp_time = datetime.fromtimestamp(payload_data['exp'])
                now = datetime.now()
                print(f"  Expires: {exp_time}")
                print(f"  Valid: {exp_time > now}")
            else:
                print("  Expiration: Not specified")
                
        else:
            print("Токен не в JWT формате")
            
    except Exception as e:
        print(f"Ошибка анализа токена: {e}")

# Основная программа
if __name__ == "__main__":
    # Проверяем токен
    check_token_validity()
    
    # Тестируем разные методы авторизации
    test_different_auth_methods()
    
    # Тестируем токен в URL
    test_with_token_in_url()
    
    print("\n" + "=" * 70)
    print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("=" * 70)