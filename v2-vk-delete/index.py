import os
import json
import time
import requests
import logging  # NEW LOGGER

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    force=True
)
logger = logging.getLogger(__name__)

def handler(event, context):
    logger.info("Delete function started. Payload: %s", event)  # NEW LOGGER

    # Если event — строка, пытаемся распарсить как JSON
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except json.JSONDecodeError:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid JSON input"})
            }


    # Проверка, что event содержит необходимые данные
    if not all(k in event for k in ("peer_id", "message_id")):
        logger.error("Missing required parameters in delete payload.")  # NEW LOGGER
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing required parameters"})
        }

    peer_id = event["peer_id"]
    message_id = event["message_id"]

    # Получение времени задержки из переменной окружения
    delay_seconds = int(os.environ.get("DELETE_DELAY_SECONDS", "60"))

    # Ожидание перед удалением
    time.sleep(delay_seconds)

    # Удаление сообщения через VK API
    vk_token = os.environ.get("VK_TOKEN")  # берём именно ту переменную, которая есть

    if not vk_token:
        logger.critical("VK_API_TOKEN not set in delete function.")  # NEW LOGGER
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "VK_API_TOKEN is not set"})
        }

    delete_message_url = "https://api.vk.com/method/messages.delete"
    params = {
        "peer_id": peer_id,
        "message_ids": message_id,   # VK API expects message_ids        
        "delete_for_all": 1,
        "access_token": vk_token,
        "v": "5.131"
    }

    logger.debug("Calling VK messages.delete with %s", params)

    vk_response = requests.post(delete_message_url, params=params)
    vk_data = vk_response.json()

    logger.debug("VK delete response: %s", vk_data)  # NEW LOGGER

    if "error" in vk_data:
        logger.error("Error deleting message: %s", vk_data["error"])  # NEW LOGGER
        return {
            "statusCode": 500,
            "body": json.dumps({"error": vk_data["error"]})
        }

    logger.info("Message deleted successfully. peer_id=%s, message_id=%s", peer_id, message_id)  # NEW LOGGER

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "Message deleted"})
    }
