import time
import datetime

# тестовая программа работающая в фоновом режиме
# для запуска : python testProg.py

def main():
    print(f"Программа запущена в {datetime.datetime.now()}")
    print("Для остановки нажмите Ctrl+C")
    
    counter = 0
    try:
        while True:
            counter += 1
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{current_time}] Hello #{counter}")
            time.sleep(20)
    except KeyboardInterrupt:
        print(f"\nПрограмма завершена. Всего отправлено {counter} сообщений")

if __name__ == "__main__":
    main()