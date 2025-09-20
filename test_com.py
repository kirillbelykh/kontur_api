import win32com.client
try:
    signed_data = win32com.client.Dispatch("CAdESCOM.CadesSignedData")
    print("CAdESCOM.CadesSignedData успешно создан")
    try:
        signed_data.ContentEncodingType = 1  # CADESCOM_BASE64_TO_BINARY
        print(f"ContentEncodingType установлен: {signed_data.ContentEncodingType}")
    except Exception as e:
        print(f"Ошибка установки ContentEncodingType: {e}")
except Exception as e:
    print(f"Ошибка создания CAdESCOM.CadesSignedData: {e}")