
import json
import requests
import socket
from pathlib import Path
from datetime import datetime
import os
import urllib3
import pymssql

#тестовый проект с возможностью выгрузки данных в json из БД

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

def get_sql_connection():
    """Устанавливаем соединение с SQL Server используя pymssql"""
    try:
        conn = pymssql.connect(
            server=SQL_SERVER,
            port=SQL_PORT,
            user=SQL_USERNAME,
            password=SQL_PASSWORD,
            database=SQL_DATABASE,
            as_dict=True  # Возвращать результаты как словари
        )
        print("Успешное подключение к SQL Server!")
        return conn
        
    except pymssql.Error as e:
        print(f"Ошибка подключения к SQL Server: {e}")
        return None

def get_gez_data(limit=1000):
    """Получаем данные из таблицы gez"""
    conn = get_sql_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # SQL запрос для получения данных
        query = f"""
        SELECT TOP ({limit})
            [ind], [num], [Dat], [PROEKT], [ZAKAZ], [ADRES], [DATA], [prisoed], [davl], [gazosnab],
            [numpr], [d], [harakter], [len], [isp], [p], [s], [dopl], [dv], [tu], [r], [dvtp], [prtype],
            [el], [kor_povr], [agr_gr], [ist_tok], [prinadl], [pom], [dadd], [id_zvk], [rsh_kub], [rsh_tut],
            [grs], [rsh_mln], [obl], [mat], [rsh_tsm], [upd_us], [dvtup], [prl], [selo], [n_zhurn], [vvod],
            [l_type], [adm], [num2], [isp2], [prinadl_txt], [p1314], [prokl], [d_rsch], [mat_rsch], [d_m1],
            [d_m2], [p2_obj], [p3_obj], [str_obj], [dom_obj], [korpus_obj], [adr_txt_obj], [p2_gp], [p3_gp],
            [str_gp], [dom_gp], [korpus_gp], [adr_txt_gp], [tu_status], [tu_kat], [prokl1], [srok2], [isp1],
            [tu1], [gzpr_net], [komm], [dogno], [lg_vrz], [prg_d], [prg_prkl], [prg_dvl], [ehztu], [kat_z],
            [ind_zd], [rsh_curr], [in_home], [snt], [ravetti], [id_uch], [rsh_add], [dvtp2], [vn_uch], [vn_grp],
            [cpr_no], [pometka_s], [isp_pr], [s_vn], [ds_dogaz], [cgno], [coksno], [cgiono], [dat_pr], [date_upd],
            [kmpl], [vn_no], [kub_mlp], [pu_kol], [tmp], [sezon], [kat_ptr], [dat_rsch], [num_1tu], [rsh_dem], [grs_out1]
        FROM [gez].[dbo].[gez]
        """
        
        print("Выполняем запрос к базе данных...")
        cursor.execute(query)
        
        # Получаем все результаты
        results = cursor.fetchall()
        
        print(f"Получено {len(results)} записей из таблицы gez")
        return results
            
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return None
    finally:
        conn.close()

def convert_to_json_serializable(data):
    """Конвертируем данные в JSON-сериализуемый формат"""
    if isinstance(data, datetime):
        return data.isoformat()
    elif hasattr(data, 'isoformat'):
        return data.isoformat()
    return data

def save_gez_to_file(data, filename="gez_data.json"):
    """Сохранение данных из таблицы gez в JSON файл"""
    try:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        filepath = output_dir / filename
        
        # Конвертируем данные в JSON-сериализуемый формат
        serializable_data = []
        for record in data:
            serializable_record = {}
            for key, value in record.items():
                serializable_record[key] = convert_to_json_serializable(value)
            serializable_data.append(serializable_record)
        
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(serializable_data, file, ensure_ascii=False, indent=2, default=str)
        
        print(f"Данные сохранены в файл: {filepath}")
        
        # Также сохраняем в текстовый файл для удобства просмотра
        txt_filepath = output_dir / "gez_data_readable.txt"
        with open(txt_filepath, 'w', encoding='utf-8') as file:
            file.write("ДАННЫЕ ИЗ ТАБЛИЦЫ GEZ\n")
            file.write("=" * 50 + "\n")
            file.write(f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n")
            file.write(f"Всего записей: {len(data)}\n")
            file.write("=" * 50 + "\n\n")
            
            for i, record in enumerate(data, 1):
                file.write(f"ЗАПИСЬ #{i}\n")
                file.write("-" * 30 + "\n")
                for key, value in record.items():
                    file.write(f"{key}: {value}\n")
                file.write("\n")
        
        print(f"Читаемая версия сохранена в: {txt_filepath}")
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
    
    print("\nПолучаем данные из таблицы gez...")
    gez_data = get_gez_data(limit=1000)
    
    if gez_data:
        print(f"\nУспешно получено {len(gez_data)} записей!")
        save_gez_to_file(gez_data, "gez_data.json")
        
        # Выводим краткую информацию о первых записях
        print("\nПервые 5 записей:")
        for i, record in enumerate(gez_data[:5], 1):
            print(f"{i}. ID: {record.get('ind', 'N/A')}, Номер: {record.get('num', 'N/A')}")
    else:
        print("\nНе удалось получить данные из базы данных.")

if __name__ == "__main__":
    main()