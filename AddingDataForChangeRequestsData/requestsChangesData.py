import json
import requests
import socket
from pathlib import Path
from datetime import datetime
import time
import os
import urllib3
import pymssql
import uuid

# Программа для получения данных из БД и создания объектов газификации через API администратора 
# (для просмотра результата можно запустить программу getGasObjectData.py)
# для создания заявки использовать ID пользователя 4358

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
API_AUTH_TOKEN = None  
AUTH_RETRY_COUNT = 0
MAX_AUTH_RETRIES = 3

# ID пользователя для которого создаются объекты газификации
TARGET_USER_ID = 161  # Можно сделать настраиваемым

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

# ========== ФУНКЦИИ АВТОРИЗАЦИИ ==========

def get_auth_token():
    """Получает токен авторизации из API"""
    global API_AUTH_TOKEN, AUTH_RETRY_COUNT
    
    try:
        # Проверяем количество попыток
        if AUTH_RETRY_COUNT >= MAX_AUTH_RETRIES:
            print(f"Превышено максимальное количество попыток авторизации ({MAX_AUTH_RETRIES})")
            return None
            
        AUTH_RETRY_COUNT += 1
        
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
        
        # Обработка различных статусов
        if response.status_code == 429:
            print("Превышен лимит запросов (429). Ожидание 60 секунд...")
            time.sleep(60)
            return get_auth_token()  # Рекурсивный вызов после ожидания
            
        elif response.status_code == 201:
            # Пытаемся получить JSON ответ
            try:
                result = response.json()
            except json.JSONDecodeError as e:
                print(f"Ошибка декодирования JSON: {e}")
                print(f"Текст ответа: {response.text[:500]}...")
                return None
            
            jwt_token = result.get('jwt')
            if jwt_token:
                API_AUTH_TOKEN = f"Bearer {jwt_token}"
                AUTH_RETRY_COUNT = 0  # Сбрасываем счетчик при успешной авторизации
                print("Авторизация успешна!")
                print(f"Токен получен: {jwt_token[:50]}...")
                return API_AUTH_TOKEN
            else:
                print("В ответе отсутствует JWT токен")
                print(f"Полный ответ: {result}")
                return None
                
        else:
            print(f"Ошибка авторизации: {response.status_code}")
            print(f"Текст ответа: {response.text[:500]}...")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сетевого запроса при авторизации: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка при авторизации: {e}")
        return None

def refresh_auth_token():
    """Обновляет токен авторизации"""
    global API_AUTH_TOKEN
    print("Обновление токена авторизации...")
    API_AUTH_TOKEN = None
    return get_auth_token()

def ensure_auth():
    """Обеспечивает наличие действительного токена авторизации"""
    global API_AUTH_TOKEN
    if not API_AUTH_TOKEN:
        return get_auth_token()
    return API_AUTH_TOKEN


# остальной код

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========

def main():
    print("=" * 60)
    print("СОЗДАНИЕ ОБЪЕКТОВ ГАЗИФИКАЦИИ ЧЕРЕЗ API АДМИНИСТРАТОРА")
    print("=" * 60)
    
    print("\nНастройка прокси...")
    setup_proxy()
    
    print("\nТестируем прокси соединение...")
    if not test_proxy_connection():
        print("Прокси не работает. Продолжаем без прокси?")
    
    # Основной цикл программы
    while True:
        print("\n" + "=" * 60)
        print("ГЛАВНОЕ МЕНЮ")
        print("=" * 60)
        print("1. Сбор данных из БД и создание объектов газификации")
        print("2. Загрузка данных в API")
        print("3. Тест авторизации")
        print("4. Выход")
        
        choice = input("\nВведите номер варианта: ").strip()
        
        if choice == "1":
            pass # заглушка
        elif choice == "2":
            pass # заглушка
        elif choice == "3":
            test_auth_mode()
        elif choice == "4":
            print("Выход из программы...")
            break
        else:
            print("Неверный выбор! Пожалуйста, введите 1, 2, 3 или 4.")

def test_auth_mode():
    """Режим тестирования авторизации"""
    print("\n" + "=" * 60)
    print("ТЕСТ АВТОРИЗАЦИИ")
    print("=" * 60)
    
    token = get_auth_token()
    if token:
        print("Авторизация успешна!")
    else:
        print("Ошибка авторизации!")

if __name__ == "__main__":
    main()