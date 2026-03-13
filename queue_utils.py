from typing import Any


def remove_order_by_document_id(download_list: list[dict[str, Any]], document_id: str | None) -> bool:
    """Удаляет заказ из активной очереди по document_id."""
    if not document_id:
        return False

    remaining_items = [
        item for item in download_list
        if item.get("document_id") != document_id
    ]
    removed = len(remaining_items) != len(download_list)
    if removed:
        download_list[:] = remaining_items
    return removed
