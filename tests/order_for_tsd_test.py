import unittest
from unittest.mock import Mock
import requests
from typing import List, Dict, Any, Tuple
import logging

# Настраиваем логирование для вывода в консоль (опционально, но полезно)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Константы (замени на свои, если нужно)
BASE = "https://example.com"
WAREHOUSE_ID = "123"

# Здесь вставь полный код функции perform_introduction_from_order_tsd
# (я опускаю его для краткости, но в реальном файле добавь весь def из твоего сообщения)

def perform_introduction_from_order_tsd(
    session: requests.Session,
    codes_order_id: str,
    positions_data: List[Dict[str, str]],
    production_patch: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Создаёт задание ввода в оборот через ТСД.
    """
    result: Dict[str, Any] = {"errors": []}
    
    try:
        logger.info(f"🚀 Начало создания задания ТСД для заказа {codes_order_id}")
        
        # 1. Создаем документ ввода в оборот
        url_create = f"{BASE}/api/v1/codes-introduction?warehouseId={WAREHOUSE_ID}"
        logger.info(f"📝 Создаем документ: {url_create}")
        
        # Отправляем POST запрос для создания документа
        r_create = session.post(url_create, json={}, timeout=30)
        logger.info(f"📡 Статус создания: {r_create.status_code}")
        logger.info(f"📡 Ответ создания: {r_create.text}")
        
        r_create.raise_for_status()
        document_id = r_create.text.strip().strip('"')
        result["introduction_id"] = document_id
        logger.info(f"✅ Создан документ: {document_id}")

        # 2. Обновляем данные production
        url_production = f"{BASE}/api/v1/codes-introduction/{document_id}/production"
        logger.info(f"⚙️ Обновляем production: {url_production}")
        
        # Формируем полный payload для production
        production_payload = {
            "documentNumber": production_patch["documentNumber"],
            "producerInn": "",
            "productionDate": production_patch["productionDate"] + "T00:00:00.000+03:00",
            "productionType": "ownProduction",
            "warehouseId": WAREHOUSE_ID,
            "expirationType": "milkMoreThan72",
            "expirationDate": production_patch["expirationDate"] + "T00:00:00.000+03:00",
            "containsUtilisationReport": True,
            "usageType": "verified",
            "cisType": "unit",
            "fillingMethod": "tsd",
            "batchNumber": production_patch["batchNumber"],
            "isAutocompletePositionsDataNeeded": True,
            "productsHasSameDates": True,
            "productGroup": "wheelChairs"
        }
        
        logger.info(f"📦 Production payload: {production_payload}")
        r_production = session.patch(url_production, json=production_payload, timeout=30)
        logger.info(f"📡 Статус production: {r_production.status_code}")
        
        r_production.raise_for_status()
        result["production_response"] = r_production.json() if r_production.content else {}
        logger.info("✅ Production данные обновлены")

        # 3. Добавляем позиции в документ (упрощенная версия без загрузки XLS)
        url_positions = f"{BASE}/api/v1/codes-introduction/{document_id}/positions"
        logger.info(f"📋 Добавляем позиции: {url_positions}")
        
        # Форматируем позиции для API
        positions_payload = {"rows": []}
        for pos in positions_data:
            position = {
                "name": pos["name"],
                "gtin": pos["gtin"],
                "tnvedCode": production_patch.get("TnvedCode", ""),
                "certificateDocumentNumber": "",
                "certificateDocumentDate": "",
                "costInKopecksWithVat": 0,
                "exciseInKopecks": 0,
                "productGroup": "wheelChairs"
            }
            positions_payload["rows"].append(position)
        
        logger.info(f"📦 Positions payload: {positions_payload}")
        r_positions = session.post(url_positions, json=positions_payload, timeout=30)
        logger.info(f"📡 Статус позиций: {r_positions.status_code}")
        
        r_positions.raise_for_status()
        result["positions_response"] = r_positions.json() if r_positions.content else {}
        logger.info(f"✅ Добавлено {len(positions_data)} позиций")

        # 4. Отправляем задание на ТСД
        url_send_tsd = f"{BASE}/api/v1/codes-introduction/{document_id}/send-to-tsd"
        logger.info(f"📱 Отправляем на ТСД: {url_send_tsd}")
        
        r_send_tsd = session.post(url_send_tsd, timeout=30)
        logger.info(f"📡 Статус отправки ТСД: {r_send_tsd.status_code}")
        
        r_send_tsd.raise_for_status()
        result["send_to_tsd_response"] = r_send_tsd.json() if r_send_tsd.content else {}
        logger.info("✅ Задание отправлено на ТСД")

        # 5. Получаем финальный статус документа
        url_final = f"{BASE}/api/v1/codes-introduction/{document_id}"
        r_final = session.get(url_final, timeout=15)
        r_final.raise_for_status()
        result["final_introduction"] = r_final.json()
        logger.info(f"✅ Финальный статус: {result['final_introduction']}")

        return True, result

    except requests.exceptions.HTTPError as e:
        error_msg = f"❌ HTTP ошибка {e.response.status_code}: {e.response.text}"
        logger.error(error_msg)
        result["errors"].append(error_msg)
        return False, result
    except requests.exceptions.RequestException as e:
        error_msg = f"❌ Ошибка сети: {str(e)}"
        logger.error(error_msg)
        result["errors"].append(error_msg)
        return False, result
    except Exception as e:
        error_msg = f"❌ Неожиданная ошибка: {str(e)}"
        logger.error(error_msg)
        result["errors"].append(error_msg)
        return False, result

class TestPerformIntroductionFromOrderTsd(unittest.TestCase):
    def setUp(self):
        # Мок-ответ для requests
        self.mock_response_class = type('MockResponse', (), {
            'status_code': 200,
            'text': '',
            'content': b'{}',
            'raise_for_status': lambda self: None,
            'json': lambda self: {}
        })()
        
        # Кастомный мок для Session
        self.session = Mock(spec=requests.Session)
        
        # Настраиваем моки для разных запросов
        self.session.post.side_effect = self._mock_post
        self.session.patch.side_effect = self._mock_patch
        self.session.get.return_value = self._create_mock_response(200, json_data={'status': 'final'})
    
    def _create_mock_response(self, status_code, text=None, json_data=None):
        response = Mock()
        response.status_code = status_code
        response.text = text or ''
        response.content = b'{}' if not text else text.encode()
        response.raise_for_status.return_value = None
        response.json.return_value = json_data or {}
        return response
    
    def _mock_post(self, url, json=None, **kwargs):
        print(f"POST URL: {url}")
        if "codes-introduction?" in url:
            # Для создания документа
            return self._create_mock_response(200, text='"doc123"')
        elif "/positions" in url:
            # Здесь перехватываем и выводим payload!
            print("=== POSITIONS PAYLOAD ===")
            print(json)
            print("=== END POSITIONS PAYLOAD ===")
            return self._create_mock_response(200, json_data={'success': True})
        elif "/send-to-tsd" in url:
            return self._create_mock_response(200, json_data={'sent': True})
        else:
            return self._create_mock_response(200, json_data={})
    
    def _mock_patch(self, url, json=None, **kwargs):
        print(f"PATCH URL: {url}")
        if json:
            print("PRODUCTION PAYLOAD:", json)
        return self._create_mock_response(200, json_data={'updated': True})
    
    def test_perform_introduction_success(self):
        """Тест успешного выполнения функции и вывода positions_payload."""
        codes_order_id = "order123"
        positions_data = [
            {"name": "Product1", "gtin": "1234567890123"},
            {"name": "Product2", "gtin": "9876543210987"}
        ]
        production_patch = {
            "documentNumber": "DOC001",
            "productionDate": "2025-10-05",
            "expirationDate": "2025-11-05",
            "batchNumber": "BATCH001",
            "TnvedCode": "1234"  # Это попадёт в tnvedCode позиций
        }
        
        success, result = perform_introduction_from_order_tsd(
            self.session, codes_order_id, positions_data, production_patch
        )
        
        # Ассерты для проверки успеха
        self.assertTrue(success)
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["introduction_id"], "doc123")
        self.assertIn("positions_response", result)
        
        # Проверяем, что POST на positions был вызван
        post_calls = [call for call in self.session.post.call_args_list if "/positions" in str(call[0][0])]
        self.assertEqual(len(post_calls), 1)

if __name__ == '__main__':
    unittest.main()