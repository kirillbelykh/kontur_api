from typing import Optional
import win32com.client
import pythoncom

# Константы
CAPICOM_CURRENT_USER_STORE = 2
CAPICOM_MY_STORE = "My"
CAPICOM_STORE_OPEN_MAXIMUM_ALLOWED = 2

def find_certificate_thumbprint() -> Optional[str]:
    """
    Находит и возвращает thumbprint первого действительного сертификата в хранилище.
    Возвращает None, если сертификаты не найдены.
    """
    pythoncom.CoInitialize()
    
    try:
        store = win32com.client.Dispatch("CAdESCOM.Store")
        store.Open(CAPICOM_CURRENT_USER_STORE, CAPICOM_MY_STORE, CAPICOM_STORE_OPEN_MAXIMUM_ALLOWED)
        
        thumbprint = None
        
        for cert in store.Certificates:
            try:
                # Получаем thumbprint
                current_thumbprint = getattr(cert, "Thumbprint", None)
                if current_thumbprint:
                    thumbprint = current_thumbprint.lower()
                    print(f"✅ Найден сертификат: {thumbprint}")
                    break  # Берем первый найденный
                    
            except Exception as e:
                print(f"⚠️ Ошибка при чтении сертификата: {e}")
                continue
        
        store.Close()
        
        if not thumbprint:
            print("❌ Сертификаты не найдены в хранилище")
        
        return thumbprint
        
    except Exception as e:
        print(f"❌ Ошибка доступа к хранилищу сертификатов: {e}")
        return None
    finally:
        try:
            pythoncom.CoUninitialize()
        except:
            pass

# Более продвинутая версия с дополнительной информацией
def find_certificate_thumbprint_detailed() -> Optional[str]:
    """
    Находит thumbprint сертификата с подробной информацией о найденном сертификате.
    """
    pythoncom.CoInitialize()
    
    try:
        store = win32com.client.Dispatch("CAdESCOM.Store")
        store.Open(CAPICOM_CURRENT_USER_STORE, CAPICOM_MY_STORE, CAPICOM_STORE_OPEN_MAXIMUM_ALLOWED)
        
        certificate_count = 0
        thumbprint = None
        certificate_info = {}
        
        for cert in store.Certificates:
            certificate_count += 1
            try:
                current_thumbprint = getattr(cert, "Thumbprint", None)
                subject = getattr(cert, "SubjectName", "Неизвестно")
                issuer = getattr(cert, "IssuerName", "Неизвестно")
                valid_to = getattr(cert, "ValidToDate", None)
                
                if current_thumbprint:
                    # Сохраняем информацию о первом найденном сертификате
                    if not thumbprint:
                        thumbprint = current_thumbprint
                        certificate_info = {
                            'thumbprint': current_thumbprint.lower(),
                            'subject': subject,
                            'issuer': issuer,
                            'valid_to': valid_to
                        }
                    
                    print(f"Сертификат {certificate_count}:")
                    print(f"  Владелец: {subject}")
                    print(f"  Thumbprint: {current_thumbprint.lower()}")
                    
            except Exception as e:
                print(f"  Ошибка чтения сертификата {certificate_count}: {e}")
                continue
        
        store.Close()
        
        # Выводим итоговую информацию
        print(f"\n📊 ИТОГИ ПОИСКА:")
        print(f"   Всего сертификатов: {certificate_count}")
        
        if thumbprint:
            print(f"✅ ИСПОЛЬЗУЕМ СЕРТИФИКАТ:")
            print(f"   Thumbprint: {certificate_info['thumbprint']}")
            print(f"   Владелец: {certificate_info['subject']}")
            print(f"   Издатель: {certificate_info['issuer']}")
            print(f"   Действует до: {certificate_info['valid_to']}")
        else:
            print("❌ Действительных сертификатов не найдено")
        
        return thumbprint
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        return None
    finally:
        try:
            pythoncom.CoUninitialize()
        except:
            pass

# Простая версия для использования в других скриптах
def get_thumbprint() -> Optional[str]:
    """
    Простая функция для получения thumbprint. 
    Используется в основном проекте.
    """
    try:
        pythoncom.CoInitialize()
        store = win32com.client.Dispatch("CAdESCOM.Store")
        store.Open(CAPICOM_CURRENT_USER_STORE, CAPICOM_MY_STORE, CAPICOM_STORE_OPEN_MAXIMUM_ALLOWED)
        
        for cert in store.Certificates:
            thumbprint = getattr(cert, "Thumbprint", None)
            if thumbprint:
                store.Close()
                pythoncom.CoUninitialize()
                return thumbprint.lower()
                
        store.Close()
        return None
        
    except Exception:
        return None
    finally:
        try:
            pythoncom.CoUninitialize()
        except:
            pass

if __name__ == '__main__':
    # Простой вызов
    print("=== ПРОСТОЙ ПОИСК ===")
    thumbprint = find_certificate_thumbprint()
    print(f"Результат: {thumbprint.lower()}")
    
    print("\n" + "="*50 + "\n")
    
    # Детальный вызов
    print("=== ДЕТАЛЬНЫЙ ПОИСК ===")
    thumbprint_detailed = find_certificate_thumbprint_detailed()
    print(f"Итоговый thumbprint: {thumbprint_detailed.lower()}")
    
    print("\n" + "="*50 + "\n")
    
    # Использование в проекте
    print("=== ДЛЯ ИСПОЛЬЗОВАНИЯ В ПРОЕКТЕ ===")
    thumb = get_thumbprint()
    print(f"Thumbprint для .env: {thumb.lower()}")