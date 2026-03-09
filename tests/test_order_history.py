# test_order_history.py
import os
import sys
import time
from datetime import datetime
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
from history_db import OrderHistoryDB

def test_basic_operations():
    """Тест базовых операций с БД"""
    print("=" * 50)
    print("ТЕСТ БАЗОВЫХ ОПЕРАЦИЙ")
    print("=" * 50)
    
    # Инициализация БД
    db = OrderHistoryDB()
    info = db.get_db_info()
    print(f"📊 Информация о БД: {info}")
    
    # Тест добавления заказов
    test_orders = [
        {
            "document_id": "TEST_001",
            "order_number": "ORDER-001",
            "customer": "Тестовый клиент 1",
            "products": ["товар1", "товар2"],
            "total_amount": 1500.50
        },
        {
            "document_id": "TEST_002", 
            "order_number": "ORDER-002",
            "customer": "Тестовый клиент 2",
            "products": ["товар3", "товар4"],
            "total_amount": 2300.00
        },
        {
            "document_id": "TEST_003",
            "order_number": "ORDER-003",
            "customer": "Тестовый клиент 3",
            "products": ["товар5"],
            "total_amount": 500.00
        }
    ]
    
    # Добавляем заказы
    print("\n📝 Добавление тестовых заказов...")
    for order in test_orders:
        db.add_order(order)
        print(f"✅ Добавлен заказ: {order['document_id']}")
    
    # Проверяем все заказы
    print("\n📋 Все заказы в БД:")
    all_orders = db.get_all_orders()
    for i, order in enumerate(all_orders, 1):
        print(f"{i}. {order['document_id']} - {order['customer']} - ТСД: {order.get('tsd_created', False)}")
    
    # Проверяем заказы без ТСД
    print("\n🆕 Заказы без ТСД:")
    orders_without_tsd = db.get_orders_without_tsd()
    for i, order in enumerate(orders_without_tsd, 1):
        print(f"{i}. {order['document_id']} - {order['customer']}")
    
    # Тест поиска заказа
    print("\n🔍 Поиск заказа TEST_002:")
    found_order = db.get_order_by_document_id("TEST_002")
    if found_order:
        print(f"Найден: {found_order['document_id']} - {found_order['customer']}")
    else:
        print("Заказ не найден")
    
    # Тест пометки ТСД
    print("\n📱 Пометка заказа как отправленного на ТСД...")
    db.mark_tsd_created("TEST_001", "INTRO-12345")
    
    # Проверяем заказы без ТСД после пометки
    print("\n🆕 Заказы без ТСД после пометки:")
    orders_without_tsd = db.get_orders_without_tsd()
    for i, order in enumerate(orders_without_tsd, 1):
        print(f"{i}. {order['document_id']} - {order['customer']}")
    
    # Проверяем обновленную информацию о БД
    updated_info = db.get_db_info()
    print(f"\n📊 Обновленная информация о БД: {updated_info}")

def test_error_handling():
    """Тест обработки ошибок"""
    print("\n" + "=" * 50)
    print("ТЕСТ ОБРАБОТКИ ОШИБОК")
    print("=" * 50)
    
    # Тест с неверным путем
    print("\n🚫 Тест с неверным сетевым путем...")
    try:
        db_bad_path = OrderHistoryDB(r"\\invalid_server\invalid_path\orders.json")
        info = db_bad_path.get_db_info()
        print(f"Информация (фолбэк): {info}")
    except Exception as e:
        print(f"Ошибка: {e}")
    
    # Тест добавления дубликата
    print("\n🔄 Тест добавления дубликата...")
    db = OrderHistoryDB()
    duplicate_order = {
        "document_id": "TEST_001",  # Уже существует
        "order_number": "ORDER-DUP",
        "customer": "Дубликат",
        "products": [],
        "total_amount": 0
    }
    db.add_order(duplicate_order)
    print("Попытка добавления дубликата завершена")
    
    # Тест поиска несуществующего заказа
    print("\n❌ Тест поиска несуществующего заказа...")
    non_existent = db.get_order_by_document_id("NON_EXISTENT_999")
    if non_existent is None:
        print("Несуществующий заказ не найден - корректно")
    else:
        print("Ошибка: найден несуществующий заказ")

def test_concurrent_access():
    """Тест 'конкурентного' доступа (симуляция)"""
    print("\n" + "=" * 50)
    print("ТЕСТ КОНКУРЕНТНОГО ДОСТУПА")
    print("=" * 50)
    
    db1 = OrderHistoryDB()
    db2 = OrderHistoryDB()  # Второй экземпляр для симуляции другого пользователя
    
    # Добавляем заказ от первого "пользователя"
    order1 = {
        "document_id": "CONCURRENT_001",
        "order_number": "CONC-001",
        "customer": "Пользователь 1",
        "products": ["товар1"],
        "total_amount": 1000
    }
    db1.add_order(order1)
    print("✅ Пользователь 1 добавил заказ CONCURRENT_001")
    
    # Второй "пользователь" читает заказы
    time.sleep(1)  # Небольшая задержка
    db2.get_all_orders()
    concurrent_order = db2.get_order_by_document_id("CONCURRENT_001")
    
    if concurrent_order:
        print(f"✅ Пользователь 2 видит заказ: {concurrent_order['document_id']}")
    else:
        print("❌ Пользователь 2 не видит заказ")
    
    # Второй "пользователь" помечает как ТСД
    db2.mark_tsd_created("CONCURRENT_001", "INTRO-99999")
    print("✅ Пользователь 2 пометил заказ как отправленный на ТСД")
    
    # Первый "пользователь" проверяет статус
    time.sleep(1)
    status_user1 = db1.get_order_by_document_id("CONCURRENT_001")
    if status_user1 and status_user1.get('tsd_created'):
        print(f"✅ Пользователь 1 видит обновленный статус ТСД: {status_user1.get('tsd_intro_number')}")
    else:
        print("❌ Пользователь 1 не видит обновлений")

def test_performance():
    """Тест производительности"""
    print("\n" + "=" * 50)
    print("ТЕСТ ПРОИЗВОДИТЕЛЬНОСТИ")
    print("=" * 50)
    
    db = OrderHistoryDB()
    start_time = time.time()
    
    # Добавление нескольких заказов
    for i in range(5):
        order = {
            "document_id": f"PERF_TEST_{i:03d}",
            "order_number": f"PERF-{i:03d}",
            "customer": f"Тестовый клиент {i}",
            "products": [f"товар_{j}" for j in range(3)],
            "total_amount": i * 1000 + 500
        }
        db.add_order(order)
    
    add_time = time.time() - start_time
    print(f"⏱️  Добавление 5 заказов: {add_time:.3f} сек")
    
    # Чтение заказов
    start_time = time.time()
    orders = db.get_all_orders()
    read_time = time.time() - start_time
    print(f"⏱️  Чтение {len(orders)} заказов: {read_time:.3f} сек")
    
    # Поиск заказа
    start_time = time.time()
    for i in range(10):
        db.get_order_by_document_id("PERF_TEST_002")
    search_time = time.time() - start_time
    print(f"⏱️  10 поисков заказа: {search_time:.3f} сек")

def cleanup_test_data():
    """Очистка тестовых данных (опционально)"""
    print("\n" + "=" * 50)
    print("ОЧИСТКА ТЕСТОВЫХ ДАННЫХ")
    print("=" * 50)
    
    response = input("Очистить тестовые данные? (y/n): ")
    if response.lower() == 'y':
        db = OrderHistoryDB()
        all_orders = db.get_all_orders()
        
        test_orders_to_remove = []
        for order in all_orders:
            doc_id = order.get('document_id', '')
            if any(prefix in doc_id for prefix in ['TEST_', 'CONCURRENT_', 'PERF_TEST_']):
                test_orders_to_remove.append(doc_id)
        
        if test_orders_to_remove:
            # Создаем новую БД без тестовых данных
            data = db._load_data()
            data["orders"] = [order for order in data["orders"] 
                            if order.get('document_id') not in test_orders_to_remove]
            db._save_data(data)
            print(f"✅ Удалено {len(test_orders_to_remove)} тестовых заказов")
        else:
            print("ℹ️ Тестовые данные не найдены")

def main():
    """Основная функция тестирования"""
    print("🚀 ЗАПУСК ТЕСТОВ СЕТЕВОЙ БАЗЫ ДАННЫХ ЗАКАЗОВ")
    print(f"📅 Время начала: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Запуск тестов
        test_basic_operations()
        test_error_handling()
        test_concurrent_access()
        test_performance()
        
        print("\n" + "=" * 50)
        print("✅ ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ УСПЕШНО")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ ОШИБКА ПРИ ВЫПОЛНЕНИИ ТЕСТОВ: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Предложение очистки данных
        cleanup_test_data()
        print(f"\n🏁 Тестирование завершено в {datetime.now().strftime('%H:%M:%S')}")

if __name__ == "__main__":
    main()
