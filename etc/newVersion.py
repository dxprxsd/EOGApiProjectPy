import json
import requests
import socket
from pathlib import Path
from datetime import datetime
import os
import urllib3
import pymssql

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Настройки прокси
PROXY_HOST = '192.168.1.2'
PROXY_PORT = 8080
PROXY_USER = 'kuzminiv'
PROXY_PASS = '12345678Q!'

# URL прокси
PROXY_URL = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"

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

def get_appeals_categories():
    """Получение категорий обращений через прокси (как в браузере)"""
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    params = {"page": 1, "per": 10}
    url = "https://tpsg.etpgpb.ru/v1/appeals/categories"
    
    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            proxies={"http": PROXY_URL, "https": PROXY_URL},
            timeout=15,
            verify=False
        )
        
        print(f"Статус: {response.status_code}")
        if response.status_code == 200:
            print("Успех!")
            return response.json()
        else:
            print(f"Ответ сервера: {response.status_code}")
            print(f"Текст ответа: {response.text[:200]}...")
            return None
    except Exception as e:
        print(f"Ошибка при запросе: {e}")
        return None

def get_appeals_subjects(category_id=3, page=1, per=10):
    """Получение тем обращений для указанной категории"""
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    params = {"page": page, "per": per}
    url = f"https://tpsg.etpgpb.ru/v1/appeals/categories/{category_id}/subjects"
    
    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            proxies={"http": PROXY_URL, "https": PROXY_URL},
            timeout=15,
            verify=False
        )
        
        print(f"Статус: {response.status_code}")
        if response.status_code == 200:
            print("Успех!")
            return response.json()
        else:
            print(f"Ответ сервера: {response.status_code}")
            print(f"Текст ответа: {response.text[:200]}...")
            return None
    except Exception as e:
        print(f"Ошибка при запросе тем обращений: {e}")
        return None

def save_to_file(data, filename="appeals_categories.txt"):
    """Сохранение данных в файл"""
    try:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write("КАРТОЧКИ КАТЕГОРИЙ ОБРАЩЕНИЙ\n")
            file.write("=" * 50 + "\n")
            file.write(f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n")
            file.write("=" * 50 + "\n\n")
            
            if data and 'data' in data:
                for i, category in enumerate(data['data'], 1):
                    attributes = category.get('attributes', {})
                    
                    file.write(f"КАРТОЧКА #{i}\n")
                    file.write("-" * 30 + "\n")
                    file.write(f"ID: {attributes.get('id', 'N/A')}\n")
                    file.write(f"Название: {attributes.get('name', 'N/A')}\n")
                    file.write(f"Slug: {attributes.get('slug', 'N/A')}\n")
                    file.write(f"Внешний ID: {attributes.get('external_id', 'N/A')}\n")
                    file.write(f"Тип показа: {attributes.get('shown_for_kind', 'N/A')}\n")
                    file.write(f"Активна: {'Да' if attributes.get('active') else 'Нет'}\n")
                    file.write("\n")
                
                file.write("=" * 50 + "\n")
                file.write(f"Всего категорий: {len(data['data'])}\n")
            else:
                file.write("Данные о категориях не получены\n")
        
        print(f"Файл сохранен: {filepath}")
        return True
        
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")
        return False

def save_raw_json_to_file(data, filename="raw_data.json"):
    """Сохранение сырых JSON данных в файл без парсинга"""
    try:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        
        print(f"Сырые JSON данные сохранены в файл: {filepath}")
        return True
        
    except Exception as e:
        print(f"Ошибка при сохранении сырых данных: {e}")
        return False

def save_subjects_to_file(data, filename="appeals_subjects.txt"):
    """Сохранение данных о темах обращений в файл"""
    try:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write("ТЕМЫ ОБРАЩЕНИЙ\n")
            file.write("=" * 50 + "\n")
            file.write(f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n")
            file.write("=" * 50 + "\n\n")
            
            if data and 'data' in data:
                for i, subject in enumerate(data['data'], 1):
                    attributes = subject.get('attributes', {})
                    
                    file.write(f"ТЕМА #{i}\n")
                    file.write("-" * 30 + "\n")
                    file.write(f"ID: {attributes.get('id', 'N/A')}\n")
                    file.write(f"Название: {attributes.get('name', 'N/A')}\n")
                    file.write(f"Slug: {attributes.get('slug', 'N/A')}\n")
                    file.write(f"Внешний ID: {attributes.get('external_id', 'N/A')}\n")
                    file.write(f"Активна: {'Да' if attributes.get('active') else 'Нет'}\n")
                    file.write("\n")
                
                file.write("=" * 50 + "\n")
                file.write(f"Всего тем: {len(data['data'])}\n")
            else:
                file.write("Данные о темах обращений не получены\n")
        
        print(f"Файл сохранен: {filepath}")
        return True
        
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")
        return False

def main():
    print("Настройка прокси...")
    setup_proxy()
    
    print("\nТестируем прокси соединение...")
    if not test_proxy_connection():
        print("Прокси не работает.")
        return
    
    print("\nПробуем сделать запрос к API через прокси...")
    
    # Получаем категории обращений
    categories_data = get_appeals_categories()
    
    if categories_data:
        print(f"\nУспешно получено {len(categories_data.get('data', []))} категорий!")
        save_to_file(categories_data, "categories_from_api.txt")
        save_raw_json_to_file(categories_data, "categories_raw.json")
        
        # Выводим краткую информацию
        print("\nПолученные категории:")
        for cat in categories_data.get('data', []):
            attrs = cat.get('attributes', {})
            print(f"- {attrs.get('name', 'N/A')} (ID: {attrs.get('id', 'N/A')})")
        
        # Получаем темы обращений для первой категории
        if categories_data.get('data'):
            first_category_id = categories_data['data'][0].get('attributes', {}).get('id')
            if first_category_id:
                print(f"\nПолучаем темы обращений для категории ID: {first_category_id}")
                subjects_data = get_appeals_subjects(category_id=first_category_id, page=1, per=50)
                
                if subjects_data:
                    print(f"Успешно получено {len(subjects_data.get('data', []))} тем обращений!")
                    save_subjects_to_file(subjects_data, f"subjects_category_{first_category_id}.txt")
                    save_raw_json_to_file(subjects_data, f"subjects_raw_{first_category_id}.json")
                    
                    # Выводим краткую информацию о темах
                    print("\nПолученные темы:")
                    for subject in subjects_data.get('data', []):
                        attrs = subject.get('attributes', {})
                        print(f"- {attrs.get('name', 'N/A')} (ID: {attrs.get('id', 'N/A')})")
    else:
        print("\nНе удалось получить данные от API.")

if __name__ == "__main__":
    main()