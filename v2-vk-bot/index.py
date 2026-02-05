import os
import json
import requests
import time
import logging
import threading 
import re  # new: для распознавания URL в тексте
import uuid
import secrets
import string
from datetime import datetime, timedelta
import ydb
import ydb.iam  # импорт провайдеров IAM
from contextlib import contextmanager
import socket
from urllib.parse import urlparse
import urllib.parse 
from ydb import Driver, DriverConfig, credentials
from threading import Lock # 2025/05/27
from hashlib import md5
import hashlib
import base64
import traceback
from typing import Optional
# from ydb.types import Int64Value   не сущ
# from ydb.types import Timestamp
# from ydb import issues, operations, scripting, scheme, table, tracing  # Стандартные импорты
# from ydb.timestamp import Timestamp  # Основной импорт
# from ydb._utilities import Timestamp  # Внутренний модуль

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    force=True
)
logger = logging.getLogger(__name__)

#logger.info(f"YDB SDK version: {ydb.__version__}")
#logger.info(f"YDB module path: {ydb.__file__}")

ADMIN_IDS = {
    574829952  # Основной тестовый администратор
    # Другие ID можно добавить через запятую
}

# Переменные окружения
CONFIRMATION_TOKEN = os.environ.get("CONFIRMATION_TOKEN")
VK_API_TOKEN = os.environ.get("VK_API_TOKEN")
VK_API_SECRET = os.environ.get("VK_API_SECRET")  # Secret из настроек Callback
DELETE_FUNCTION_ID = os.environ.get("DELETE_FUNCTION_ID")
DELETE_FUNCTION_URL = f"https://functions.yandexcloud.net/{DELETE_FUNCTION_ID}?integration=async"  # NEW
IAM_TOKEN = os.environ.get("IAM_TOKEN")  # NEW

send_url = "https://api.vk.com/method/messages.send"

VK_APP_ID = os.environ.get('VK_APP_ID')  
VK_CLIENT_SECRET = os.environ.get('VK_CLIENT_SECRET')
REDIRECT_URI = os.environ.get('REDIRECT_URI')


# Настройки TTL и сроков (настраиваемые)
VIP_DURATION_DAYS = 3  # срок действия VIP-постов
ORDINARY_ASSIGN_COUNT = 15  # количество обычных ссылок в задании
ADDITIONAL_ASSIGN_COUNT = 10  # количество ссылок для дополнительного задания



# Принудительный сброс соединения
#if 'driver' in globals():
#    driver.stop()



# Инициализация драйвера и пула сессий
endpoint = os.getenv("YDB_ENDPOINT")
database = os.getenv("YDB_DATABASE")

if not endpoint or not database:
    raise RuntimeError("YDB_ENDPOINT or YDB_DATABASE is not set")

credentials = ydb.iam.MetadataUrlCredentials()
driver_config = ydb.DriverConfig(endpoint, database, credentials=credentials)
driver = ydb.Driver(driver_config)

try:
    driver.wait(timeout=10)
    logger.info("YDB driver initialized successfully.")
except ydb.Error as e:
    logger.error(f"Failed to initialize YDB driver: {e}")
    raise

# Создание пула сессий
session_pool = ydb.SessionPool(driver)


#===================================================================================
# Утилита вызова второй функции удаления
def invoke_delete(peer_id: int, message_id: int):                     # СДЕЛАНО
    
    #Отправляет POST ко второй функции и логирует результат.
    
    logger.info("invoke_delete started for message_id=%s", message_id)
    try:
        headers = {
            "Authorization": f"Bearer {IAM_TOKEN}",  # NEW
            "Content-Type": "application/json"
        }
        payload = {"peer_id": peer_id, "message_id": message_id}
        resp = requests.post(
            DELETE_FUNCTION_URL,
            json=payload #,            headers=headers  # NEW
        )
        logger.info(
            "DeleteFunction (%s) responded with %s: %s",
            DELETE_FUNCTION_ID, resp.status_code, resp.text
        )
    except Exception as e:
        logger.error("Failed to invoke delete function: %s", e)
#======== 12 ===========================================================================
# для отладки обращения к БД 2025.05.10
def execute_query_in_db(query: str) -> dict:                          # СДЕЛАНО
    """Метод для выполнения запросов в БД через сессии YDB."""
    
    logger.info(f"12.1. Executing query: {query}")
    
    # Функция execute_query выполняет переданный SQL-запрос через сессию YDB, 
    # обрабатывая параметры и управляя транзакцией.   
    def execute_query(session):
        try:
                       
            # Создание объекта транзакции
            tx = session.transaction()
                        
            # Выполняем запрос БЕЗ параметров
            result = tx.execute(query, commit_tx=True)            
            return result
        except Exception as inner_e:
            logger.error("12.6. TRANSACTION ERROR: %s", str(inner_e), exc_info=True)
            raise
                
    try:        
        result = session_pool.retry_operation_sync(execute_query)        
        return result
    except ydb.Error as e:        
        return {"error": str(e)}
    except Exception as e:
        logger.error("12.11. === DB QUERY FAILED ===")
        logger.error("12.12. FATAL ERROR: %s", str(e), exc_info=True)
        return {"error": str(e)} 
#======== 2 =============================================================================
def extract_link_from_text(text: str) -> str:                         # СДЕЛАНО    
    #Вычленяет первую URL из текста.    
    pattern = r"(https?://[^\s]+)"
    match = re.search(pattern, text)
    return match.group(1) if match else None
#======== 14 ===========================================================================
def parse_vk_post_link(link: str):                                    # СДЕЛАНО
    """
    Парсит ссылку VK на пост и возвращает (owner_id, post_id).
    Пример ссылки: https://vk.com/wall-123456_789
    
    match = re.search(r"vk\.com/wall(-?\d+)_(\d+)", link)
    if match:
        owner_id = int(match.group(1))
        post_id = int(match.group(2))
        return owner_id, post_id
    return None, None
    """    
    patterns = [
        r"(?:vk\.com|vkvideo\.ru)/(?:wall|clip|video)(-?\d+)_(\d+)",
        r"vk\.com/\w+\?.*wall(-?\d+)_(\d+)"
    ] # noqa: W605 (игнорировать предупреждение о неверном escape-символе)
    
    for pattern in patterns:
        match = re.search(pattern, link)
        if match:
            return int(match.group(1)), int(match.group(2))
    
    return None, None  # Если формат не распознан
#======= 10 ============================================================================
def save_post_to_db(link: str, user_id: int, is_admin: bool = False) -> str:          # СДЕЛАНО
    """
    Сохраняет запись о посте в таблицу posts.
    При is_admin=True — дублирует запись в vip_posts.
    Возвращает post_id.
    """
    
    owner_id, post_id_raw = parse_vk_post_link(link)
    if owner_id is None or post_id_raw is None:
        logger.error("10.2. Invalid link: %s", link)
        return None  # или другая логика
      
    
    post_id = f"{owner_id}_{post_id_raw}"

    # now = datetime.utcnow()
    
    raw_query = f"""
    UPSERT INTO posts (post_id, owner_id, is_admin, user_id, added_at, message_text)
    VALUES (
        '{post_id.replace("'", "''")}',
        {int(owner_id)},
        {1 if is_admin else 0},
        {int(user_id)},
        CurrentUtcDatetime(),
        '{link.replace("'", "''")}'
    )
    """
        
    try:
        result = execute_query_in_db(raw_query)        
        return post_id
    except Exception as e:
        logger.error(f"10.3. Error saving post: {e}", exc_info=True)
        raise

    
    
    if is_admin:
        # Формируем дату истечения (текущее время + VIP_DURATION_DAYS дней)
        expires_at = f"DateTime::AddDays(CurrentUtcDatetime(), {VIP_DURATION_DAYS})"

        raw_vip_query = f"""
        UPSERT INTO vip_posts (post_id, owner_id, added_at, expires_at)
        VALUES (
            '{post_id.replace("'", "''")}',
            {int(owner_id)},
            CurrentUtcDatetime(),
            {expires_at}
        )
        """

        try:
            result = execute_query_in_db(raw_vip_query)        
            return post_id
        except Exception as e:
            logger.error(f"10.4. Error saving VIP post: {e}", exc_info=True)
            raise


    return post_id
#==== 7 ===============================================================================
def send_vk_message(peer_id, text, random_id, keyboard=None):
    try:
        params = {
            "peer_id": peer_id,
            "message": text,
            "random_id": random_id,
            "access_token": VK_API_TOKEN,  # Токен сообщества
            "v": "5.131"
        }
        
        # Добавляем клавиатуру, если она есть (только для ЛС и бесед)
        if keyboard : # and peer_id > 2000000000:  # Проверка, что это чат (не обсуждение)
            params["keyboard"] = json.dumps(keyboard)
            
        response = requests.post(
            "https://api.vk.com/method/messages.send",
            data=params
        ).json()

        # Логируем ответ для отладки
        logger.info(f"VK API response: {response}")

        if "error" in response:
            logger.error(f"VK API error: {response['error']}")
            return None
            
        return response.get("response", {})
    
    except Exception as e:
        logger.error(f"send_vk_message error: {str(e)}")
        return None
# ===== 3 ============================================================
def add_vip_post(link: str, admin_id: int) -> bool:     # СДЕЛАНО
    """
    Добавляет VIP-пост в базу данных
    :param link: Ссылка на пост в формате https://vk.com/wall{owner_id}_{post_id}
    :param admin_id: ID администратора, добавившего пост
    :return: True при успешном добавлении, False при ошибке
    """
    try:
        # logger.info(f"3.1. Adding VIP post from admin {admin_id}: {link}")
        
        # Парсим ссылку для получения owner_id и post_id
        owner_id, post_id_raw = parse_vk_post_link(link)
        if not owner_id or not post_id_raw:
            logger.error(f"3.2. Invalid VIP post link format: {link}")
            return False

        post_id = f"{owner_id}_{post_id_raw}"
        now = datetime.utcnow()
        expires_at = f"DateTime::AddDays(CurrentUtcDatetime(), {VIP_DURATION_DAYS})"

        # Формируем запрос для основной таблицы posts
        upsert_post_query = f"""
        UPSERT INTO posts (post_id, owner_id, is_admin, user_id, added_at, message_text)
        VALUES (
            '{post_id.replace("'", "''")}',
            {int(owner_id)},
            1,
            {admin_id},
            CurrentUtcDatetime(),
            '{link.replace("'", "''")}'
        )
        """

        # Формируем запрос для таблицы vip_posts
        upsert_vip_query = f"""
        UPSERT INTO vip_posts (post_id, owner_id, added_at, expires_at)
        VALUES (
            '{post_id.replace("'", "''")}',
            {int(owner_id)},
            CurrentUtcDatetime(),
            CurrentUtcDatetime()
        )
        """
        #{expires_at}

        # Выполняем оба запроса в транзакции
        def execute_queries(session):
            # Выполняем первый запрос
            session.transaction().execute(
                upsert_post_query,
                commit_tx=True
            )
            # Выполняем второй запрос
            session.transaction().execute(
                upsert_vip_query,
                commit_tx=True
            )
            return True

        # Используем retry_operation_sync для устойчивости
        session_pool.retry_operation_sync(execute_queries)
                
        return True

    except Exception as e:
        logger.error(f"3.4. Failed to add VIP post: {str(e)}", exc_info=True)
        return False
#++++++ 4 ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def list_vip_posts() -> str:                            # СДЕЛАНО
    """
    Финальная версия с учетом структуры данных из логов
    """
    try:
        # logger.info("4.1. Starting VIP posts listing (final version)")
        
        now = datetime.utcnow()
        three_days_ago = now - timedelta(days=3)
        

        # Ваш рабочий запрос
        query = """
        SELECT 
            v.post_id,
            v.owner_id,
            v.added_at,
            p.message_text
        FROM vip_posts AS v
        JOIN posts AS p ON v.post_id = p.post_id
        ORDER BY v.added_at DESC
        """
        
        # 3. Выполняем запрос (возвращает список строк)
        def execute_query(session):
            result = session.transaction().execute(
                query,
                commit_tx=True
            )
            return result[0].rows if result else []

        rows = session_pool.retry_operation_sync(execute_query)      
        
        
        if not rows:
            return "VIP-постов не найдено."

        # Логируем первую строку для проверки
        first_row = dict(rows[0].items())
        # logger.info(f"4.6. First row content: {first_row}")

        recent_posts = []
        for i, row in enumerate(rows, 1):
            try:
                # Преобразуем строку в словарь
                row_dict = dict(row.items())
                
                # Доступ к полям с префиксами таблиц
                added_at_timestamp = row_dict.get('v.added_at')
                added_at = datetime.fromtimestamp(added_at_timestamp) if added_at_timestamp else datetime.min
                post_id = row_dict.get('v.post_id', 'N/A')
                message_text = row_dict.get('p.message_text', 'NO_TEXT')
                owner_id = row_dict.get('v.owner_id', 'N/A')
                
                # logger.info(f"4.7. Row {i}: post_id={post_id}, added_at={added_at}")
                
                if added_at > three_days_ago:
                    recent_posts.append({
                        'number': i,
                        'text': message_text,
                        'id': post_id,
                        'owner': owner_id,
                        'time': added_at.strftime('%Y-%m-%d %H:%M')
                    })
                    
            except Exception as e:
                logger.error(f"4.8. Error processing row {i}: {str(e)}")
                continue

        if not recent_posts:
            return "Нет свежих VIP-постов (за последние 3 дня)."
        
        result_msg = ["Свежие VIP-посты:"]
        for post in recent_posts:
            result_msg.append(
                f"{post['number']}. {post['text']}\n"
                f"   Владелец: {post['owner']}\n"
                f"   Добавлен: {post['time']}"
            )
        
        return "\n".join(result_msg)

    except Exception as e:
        logger.error(f"4.9. Critical error: {str(e)}", exc_info=True)
        return "Ошибка при обработке запроса"


    #return "Активные VIP-посты:\n1. https://example.com/vip1\n2. https://example.com/vip2"
#++++++ 5 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def delete_vip_post(link: str) -> bool:                 # СДЕЛАНО
    try:
        # logger.info(f"5.1. Starting delete_vip_post for: {link}")
        
        owner_id, post_id_raw = parse_vk_post_link(link)
        if not owner_id or not post_id_raw:
            logger.error("5.2. Invalid link format")
            return False

        post_id = f"{owner_id}_{post_id_raw}"
        logger.info(f"5.3. Formatted post_id: {post_id}")

        delete_query = f"""
        DELETE FROM vip_posts
        WHERE post_id = '{post_id.replace("'", "''")}'
        """
        
        def execute(session):
            # Выполняем DELETE и сразу проверяем существование
            session.transaction().execute(delete_query, commit_tx=True)
            
            # Дополнительная проверка, что пост действительно удалён
            check_query = f"""
            SELECT COUNT(*) as cnt 
            FROM vip_posts 
            WHERE post_id = '{post_id.replace("'", "''")}'
            """
            result = session.transaction().execute(check_query, commit_tx=True)
            return result[0].rows[0].cnt == 0  # True если пост больше не существует

        success = session_pool.retry_operation_sync(execute)
        
        return success

    except Exception as e:
        logger.error(f"5.5. Delete error: {str(e)}", exc_info=True)
        return False
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


    # Реальная реализация будет:
    # 1. Искать assignments с status='overdue'
    # 2. Форматировать в отчет
    # return "Просроченные задания:\n1. Пользователь 123 - 2 задания\n2. Пользователь 456 - 1 задание"

def show_overdue_tasks() -> str:   
    # Возвращает администратору список просроченных заданий  
    try:
        query = """
        SELECT 
            a.user_id,
            a.post_id,
            p.message_text,
            a.assigned_at
        FROM assignments a
        JOIN posts p ON a.post_id = p.post_id
        WHERE a.status = 'overdue'
        ORDER BY a.assigned_at DESC
        """
        # ... (аналогично list_vip_posts())
    except Exception as e:
        logger.error(f"Error fetching overdue tasks: {str(e)}")
        return "Ошибка при получении списка просрочек"

#*******************************************
def update_interactions(user_id: int) -> bool:
    """
    Актуализирует статусы лайков/комментариев для всех активных заданий пользователя
    """
    try:
        logger.info(f"=== update_interactions СТАРТ для user {user_id} ===")
        
        # 1. ОДИН раз получаем токен для всего процесса
        user_token = get_user_token(user_id)
        if not user_token:
            logger.error(f"=== Не удалось получить токен для user {user_id} ===")
            return False
            
        logger.info(f"=== Токен получен, начинаем проверку взаимодействий ===")
        
        # 2. Получить все interactions для проверки (невыполненные + с ошибками)
        query = f"""
        SELECT 
            assignment_id,
            post_nn,
            is_vip,
            liked, 
            commented
        FROM interactions2 
        WHERE user_id = {user_id} 
          AND (liked IN (0, -1) OR commented IN (0, -1))
        """
        
        def get_interactions(session):
            result = session.transaction().execute(query)
            return result[0].rows if result and result[0].rows else []
        
        interactions = session_pool.retry_operation_sync(get_interactions)
        logger.info(f"=== Найдено interactions для проверки: {len(interactions)} ===")
        
        if not interactions:
            return True  # Нечего обновлять
            
        # 3. Для каждого поста проверить актуальный статус
        updates = []
        for interaction in interactions:
            try:
                post_nn = interaction.post_nn
                owner_id, post_id = post_nn.split('_')
                
                # Вызов обновленного метода
                has_liked, has_commented = vk_check_like_and_comment_with_token(
                    user_token=user_token,
                    user_id=int(user_id),
                    owner_id=int(owner_id), 
                    post_id=int(post_id)
                )
                
                # Обновляем только если не было ошибки (-1)
                current_liked = interaction.liked
                current_commented = interaction.commented
                
                # Обновляем только валидные результаты (не -1)
                new_liked = has_liked if has_liked != -1 else current_liked
                new_commented = has_commented if has_commented != -1 else current_commented
                
                if new_liked != current_liked or new_commented != current_commented:
                    updates.append({
                        'assignment_id': interaction.assignment_id,
                        'post_nn': post_nn,
                        'new_liked': new_liked,
                        'new_commented': new_commented
                    })
                    
            except Exception as e:
                logger.error(f"Ошибка проверки поста {interaction.post_nn}: {str(e)}")
                continue
        
        # 4. Применить обновления к БД
        if updates:
            for update in updates:
                update_query = f"""
                UPDATE interactions2 
                SET 
                    liked = {update['new_liked']},
                    commented = {update['new_commented']},
                    updated_at = CurrentUtcDatetime()
                WHERE 
                    assignment_id = '{update['assignment_id']}'
                    AND post_nn = '{update['post_nn']}'
                    AND user_id = {user_id}
                """
                execute_query_in_db(update_query)
            
            logger.info(f"=== Обновлено {len(updates)} interactions ===")
        else:
            logger.info("=== Изменений не обнаружено ===")
            
        return True
        
    except Exception as e:
        logger.error(f"=== ОШИБКА update_interactions: {str(e)} ===", exc_info=True)
        return False

#*******************************************
def update_assignments_status(user_id: int) -> bool:
    """
    Проверяет для каждого активного задания пользователя:
    - Все ли VIP-посты выполнены (лайк+комментарий)
    - Выполнено ли достаточно обычных постов (10 из 15)
    Обновляет status в assignments: 'pending' → 'completed'
    """
# ======= 21 =================================================================================================
def generate_assignment(link: str, user_id: int) -> bool:
    """
    Формирует задание для пользователя:
    - 15 обычных постов (из таблицы posts)
    - Все активные VIP-посты (из таблицы vip_posts)
    - Сохраняет задание в таблицу assignments (одну строку для одного вызова метода)
    - сохраняет несколько строк с содержимым этого задания в таблицу interactions2.
    Возвращает True при успешном создании задания.
    """
    try:
        logger.info(f"1. Starting assignment generation for user {user_id} with link {link}")
        
        # Парсим ссылку для получения owner_id и post_id
        owner_id, post_id_raw = parse_vk_post_link(link)
        if not owner_id or not post_id_raw:
            logger.error(f"2. Invalid post link format: {link}")
            return False

        post_id = f"{owner_id}_{post_id_raw}"
        assignment_id = f"{user_id}_{owner_id}_{post_id_raw}"

        logger.info(f"3. Parsed link: owner_id={owner_id}, post_id_raw={post_id_raw}")

        # === ДОБАВЛЕНА ПРОВЕРКА ДУБЛИКАТОВ ===
        check_existing_query = f"""
        SELECT COUNT(*) as count 
        FROM assignments 
        WHERE user_id = {user_id} AND post_id = '{post_id.replace("'", "''")}' AND status = 'pending'
        """
        
        def check_existing_assignment(session):
            with session.transaction() as tx:
                result = tx.execute(check_existing_query)
                count = result[0].rows[0].count if result and result[0].rows else 0
                logger.info(f"3.1. Found {count} existing assignments for this post")
                return count > 0
        
        has_existing = session_pool.retry_operation_sync(check_existing_assignment)
        if has_existing:
            logger.info(f"3.2. User {user_id} already has active assignment for post {post_id}. Skipping.")
            return True
        # === КОНЕЦ ПРОВЕРКИ ДУБЛИКАТОВ ===

        # 1. Получаем обычные посты (ограничиваем 15 для теста, потом вернем 2)
        get_regular_posts_query = f"""
        SELECT post_id, owner_id, user_id
        FROM posts
        WHERE is_admin = 0 AND user_id != {user_id}
        ORDER BY added_at DESC
        LIMIT 15
        """

        # 2. Получаем VIP-посты
        get_vip_posts_query = f"""
        SELECT post_id, owner_id
        FROM vip_posts
        """

        # Функция для удаления дублей
        def get_unique_posts(posts_list):
            seen = set()
            unique_posts = []
            for post in posts_list:
                if post.post_id not in seen:
                    seen.add(post.post_id)
                    unique_posts.append(post)
            return unique_posts

        def execute_queries(session):
            try:
                # Создаем отдельную транзакцию для чтения данных
                with session.transaction() as tx:
                    logger.info("4. Executing regular posts query")
                    regular_posts_result = tx.execute(get_regular_posts_query)
                    regular_posts = regular_posts_result[0].rows
                    logger.info(f"5. Found {len(regular_posts)} regular posts")

                    logger.info("6. Executing VIP posts query")
                    vip_posts_result = tx.execute(get_vip_posts_query)
                    vip_posts = vip_posts_result[0].rows
                    logger.info(f"7. Found {len(vip_posts)} VIP posts")

                # УДАЛЯЕМ ДУБЛИРУЮЩИЕСЯ ПОСТЫ
                regular_posts = get_unique_posts(regular_posts)
                vip_posts = get_unique_posts(vip_posts)
                
                # ОГРАНИЧИВАЕМ КОЛИЧЕСТВО ДЛЯ ТЕСТА
                regular_posts = regular_posts[:2]  # Только 2 обычных поста для теста
                # vip_posts = vip_posts[:4]  # Можно ограничить и VIP если нужно

                logger.info(f"8. After deduplication: {len(regular_posts)} regular, {len(vip_posts)} VIP posts")

                # Создаем новую транзакцию для записи данных
                with session.transaction() as tx:
                    # 3. Создаем запись в таблице assignments
                    upsert_assignment_query = f"""
                    UPSERT INTO assignments (assignment_id, user_id, post_id, status, assigned_at)
                    VALUES (
                        '{assignment_id.replace("'", "''")}',
                        {user_id},
                        '{post_id.replace("'", "''")}',
                        'pending',
                        CurrentUtcDatetime()
                    )
                    """
                    logger.info("9. Creating assignment record")
                    tx.execute(upsert_assignment_query)

                    # 4. Добавляем записи в таблицу interactions2 для обычных постов
                    logger.info("10. Adding regular posts to interactions2")
                    for idx, post in enumerate(regular_posts, 1):
                        try:
                            interaction_query = f"""
                            UPSERT INTO interactions2 (
                                assignment_id, user_id, post_nn, is_vip, liked, commented, 
                                comment_text, updated_at
                            )
                            VALUES (
                                '{assignment_id.replace("'", "''")}',
                                {user_id},
                                '{post.post_id.replace("'", "''")}',
                                0,
                                0,
                                0,
                                NULL,
                                CurrentUtcDatetime()
                            )
                            """
                            tx.execute(interaction_query)
                        except Exception as e:
                            logger.error(f"10.{idx}. Error adding regular post {post.post_id}: {str(e)}")
                            continue

                    # 5. Добавляем записи в таблицу interactions2 для VIP-постов
                    logger.info("11. Adding VIP posts to interactions2")
                    for idx, post in enumerate(vip_posts, 1):
                        try:
                            interaction_query = f"""
                            UPSERT INTO interactions2 (
                                assignment_id, user_id, post_nn, is_vip, liked, commented, 
                                comment_text, updated_at
                            )
                            VALUES (
                                '{assignment_id.replace("'", "''")}',
                                {user_id},
                                '{post.post_id.replace("'", "''")}',
                                1,
                                0,
                                0,
                                NULL,
                                CurrentUtcDatetime()
                            )
                            """
                            tx.execute(interaction_query)
                        except Exception as e:
                            logger.error(f"11.{idx}. Error adding VIP post {post.post_id}: {str(e)}")
                            continue

                    logger.info("12. Committing transaction")
                    tx.commit()
                return True

            except Exception as inner_e:
                logger.error(f"13. Error in transaction: {str(inner_e)}", exc_info=True)
                raise

        logger.info("14. Starting retryable operation")
        result = session_pool.retry_operation_sync(execute_queries)
        
        if result:
            logger.info(f"15. Assignment generated successfully for user {user_id}")
            return True
        else:
            logger.error("16. Failed to generate assignment (retry operation returned False)")
            return False

    except Exception as e:
        logger.error(f"17. Critical error in generate_assignment: {str(e)}", exc_info=True)
        return False
# ======= Форматирует задание пользователю ======================================================================================
def format_assignment_message(user_id: int) -> str:
    """
    Форматирует задание пользователя в читаемое сообщение
    """
    try:
        logger.info(f"=== format_assignment_message СТАРТ для user {user_id} ===")
        
        query = f"""
        SELECT 
            i.assignment_id,
            i.post_nn,
            i.is_vip,
            i.liked,
            i.commented,
            p.message_text
        FROM interactions2 AS i
        JOIN assignments AS a ON i.assignment_id = a.assignment_id
        LEFT JOIN posts AS p ON i.post_nn = p.post_id
        WHERE a.user_id = {user_id} AND a.status = 'pending'
        ORDER BY i.is_vip DESC, i.updated_at DESC
        LIMIT 20
        """
        
        def execute_query(session):
            result = session.transaction().execute(query)
            return result[0].rows if result and result[0].rows else []
        
        assignments = session_pool.retry_operation_sync(execute_query)
        logger.info(f"=== Найдено заданий: {len(assignments)} ===")
        
        if not assignments:
            return "📋 У вас нет активных заданий. Отправьте ссылку на пост чтобы получить задание."
        
        # Статистика
        total = len(assignments)
        completed = sum(1 for a in assignments if a['i.liked'] == 1 and a['i.commented'] == 1)
        vip_count = sum(1 for a in assignments if a['i.is_vip'])
        regular_count = total - vip_count
        
        # Форматируем сообщение
        message = [f"📋 Ваше задание ({completed}/{total} выполнено):\n"]
        
        # Функция для определения символов статуса
        def get_status_symbols(liked, commented):
            liked_symbol = "♥️" if liked == 1 else "❌" if liked == 0 else "🔄"
            commented_symbol = "💌" if commented == 1 else "❌" if commented == 0 else "🔃"
            return f"{liked_symbol}{commented_symbol}"
        
        # VIP-посты
        vip_assignments = [a for a in assignments if a['i.is_vip']]
        if vip_assignments:
            message.append(f"\n⭐ VIP-посты ({len(vip_assignments)}):")
            for i, assignment in enumerate(vip_assignments, 1):
                symbols = get_status_symbols(assignment['i.liked'], assignment['i.commented'])
                link = assignment['p.message_text'] if assignment['p.message_text'] else f"https://vk.com/wall{assignment['i.post_nn']}"
                message.append(f"{i}. {symbols} {link}")
        
        # Обычные посты
        regular_assignments = [a for a in assignments if not a['i.is_vip']]
        if regular_assignments:
            required = min(10, len(regular_assignments))
            message.append(f"\n📝 Обычные посты (лайкните {required} из {len(regular_assignments)}):")
            for i, assignment in enumerate(regular_assignments, 1):
                symbols = get_status_symbols(assignment['i.liked'], assignment['i.commented'])
                link = assignment['p.message_text'] if assignment['p.message_text'] else f"https://vk.com/wall{assignment['i.post_nn']}"
                message.append(f"{i}. {symbols} {link}")
        
        message.append(f"\n⏰ Время на выполнение: 24 часа")
        message.append("🔄🔃 - проверка не удалась")
        message.append("Для проверки выполнения отправьте 'Задание'")
        
        result = "\n".join(message)
        logger.info(f"=== ФОРМАТИРОВАНИЕ УСПЕШНО ===")
        return result
        
    except Exception as e:
        logger.error(f"=== ОШИБКА: {str(e)} ===", exc_info=True)
        return "❌ Ошибка загрузки задания"
# =============13z ===========================================================================================
def vk_check_like_and_comment_with_token(user_token: str, user_id: int, owner_id: int, post_id: int) -> tuple[int, int]:
    """
    Проверяет лайк и комментарий с УЖЕ готовым токеном
    :param user_token: Готовый access_token пользователя
    :param user_id: ID пользователя VK
    :param owner_id: ID владельца поста
    :param post_id: ID поста
    :return: (has_liked, has_commented)
    -1 - ошибка проверки, 0 - не выполнено, 1 - выполнено
    """
    logger.info("13z Проверка лайка и комментария с готовым токеном: user_id=%s, owner_id=%s, post_id=%s", 
                user_id, owner_id, post_id)

    # Проверка лайка
    like_params = {
        "type": "post",
        "owner_id": owner_id,
        "item_id": post_id,
        "user_id": user_id,
        "access_token": user_token,  # Используем переданный токен
        "v": "5.131"
    }
    
    
    
    like_url = "https://api.vk.com/method/likes.isLiked"
   
    try:
        like_response = requests.get(like_url, params=like_params, timeout=10)
        like_data = like_response.json()
    except Exception as e:
        logger.error(f"13z Ошибка сети при проверке лайка: {str(e)}")
        return -1, -1  # Ошибка проверки

    has_liked = 0
    if "response" in like_data:
        has_liked = 1 if like_data["response"].get("liked") == 1 else 0
        logger.info(f"13z Статус лайка: {has_liked}")
    else:
        logger.error(f"13z Ошибка при проверке лайка: {like_data.get('error')}")
        return -1, -1  # Ошибка проверки

    # Проверка комментария
    comment_params = {
        "owner_id": owner_id,
        "post_id": post_id,
        "access_token": user_token,
        "v": "5.131",
        "count": 100,
        "offset": 0
    }
    
    comment_url = "https://api.vk.com/method/wall.getComments"
    has_commented = 0

    try:
        while True:
            comment_response = requests.get(comment_url, params=comment_params, timeout=10)
            comment_data = comment_response.json()

            if "response" not in comment_data:
                logger.error(f"13z Ошибка при получении комментариев: {comment_data.get('error')}")
                return has_liked, -1  # Лайк проверен, комментарий - ошибка

            items = comment_data["response"].get("items", [])
            
            # Ищем комментарий пользователя
            if any(comment.get("from_id") == user_id for comment in items):
                has_commented = 1
                logger.info("13z Комментарий найден")
                break

            if len(items) < comment_params["count"]:
                break  # Достигнут конец списка

            comment_params["offset"] += comment_params["count"]

        logger.info(f"13z Итоговый результат: liked={has_liked}, commented={has_commented}")
        return has_liked, has_commented

    except Exception as e:
        logger.error(f"13z Ошибка при проверке комментариев: {str(e)}")
        return has_liked, -1  # Лайк проверен, комментарий - ошибка





# ==== 6 ================================================
def send_and_delete(peer_id, text, reply_to=None, keyboard=None): 
    try:                                  
        """Расширенная версия с поддержкой клавиатуры"""
        sent_id = send_vk_message(
            peer_id=peer_id,
            text=text,
            random_id = reply_to,
            keyboard=keyboard
        )

        if not sent_id:
            raise ValueError("Не удалось отправить сообщение")
        
        threading.Thread(
            target=invoke_delete,
            args=(peer_id, sent_id),
            daemon=True
        ).start()
        
        return {"statusCode": 200, "body": json.dumps({"response": sent_id})}

    except Exception as e:
        logger.error(f"send_and_delete failed: {str(e)}")
        return {"statusCode": 500}
# ==========АВТОРИЗАЦИЯ==========================================
def get_user_token(user_id: int) -> str:
    
    # Получает access_token пользователя с автоматическим обновлением при необходимости
    
    try:
        logger.info(f"Поиск токена для пользователя {user_id}")
        
        # 1. Получить все токены из БД
        query = f"""
        SELECT access_token, refresh_token, device_id, obtained_at
        FROM user_tokens
        WHERE user_id = {user_id}
        LIMIT 1
        """
        
        def execute(session):
            result = session.transaction().execute(query, commit_tx=True)
            return result[0].rows[0] if result[0].rows else None
        
        tokens = session_pool.retry_operation_sync(execute)
        
        if not tokens:
            logger.info(f"Токен для пользователя {user_id} не найден")
            return None

        # 2. Проверить срок действия токена (timestamp из БД -> datetime)
        current_time = datetime.utcnow()
        # Преобразуем timestamp из БД в datetime
        token_time = datetime.fromtimestamp(tokens.obtained_at)
        token_age = current_time - token_time
        
        logger.info(f"=== ТОКЕН ПОЛУЧЕН из БД: age={token_age}, нужно_обновлять={token_age >= timedelta(minutes=55)} ===")

        if token_age < timedelta(minutes=55) and False:
            logger.info(f"Токен пользователя {user_id} еще действителен")
            return tokens.access_token
        # 3. Токен просрочен - обновляем
        logger.info(f"=== НАЧАЛО ОБНОВЛЕНИЯ ТОКЕНА для user {user_id} ===")
        
        new_tokens = refresh_user_token(user_id, tokens.refresh_token, tokens.device_id)
        
        if new_tokens:
            # Сохраняем новые токены
            save_success = save_token_to_db(
                user_id=user_id,
                access_token=new_tokens['access_token'],
                refresh_token=new_tokens['refresh_token'],
                device_id=tokens.device_id,
                expires_in=new_tokens.get('expires_in', 3600)
            )
            
            if save_success:
                logger.info(f"=== Токен пользователя {user_id} УСПЕШНО ОБНОВЛЕН ===")
                return new_tokens['access_token']
            else:
                logger.error(f"=== НЕ УДАЛОСЬ СОХРАНИТЬ обновленный токен для {user_id} ===")
                return tokens.access_token  # ← Возвращаем старый токен при ошибке сохранения
        else:
            logger.error(f"=== ОШИБКА ОБНОВЛЕНИЯ ТОКЕНА для {user_id} ===")
            return tokens.access_token  # ← ВОТ ИСПРАВЛЕНИЕ! Возвращаем старый токен

    except Exception as e:
        logger.error(f"=== КРИТИЧЕСКАЯ ОШИБКА в get_user_token: {str(e)} ===", exc_info=True)
        return None

# =:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
def generate_pkce():
    """Генерация code_verifier и code_challenge для PKCE"""
    # Генерация случайного code_verifier (43-128 символов)
    code_verifier = ''.join(secrets.choice(string.ascii_letters + string.digits + '-._~') for _ in range(43))
    
    # Создание code_challenge (SHA-256 + base64url)
    code_challenge = hashlib.sha256(code_verifier.encode('ascii')).digest()
    code_challenge = base64.urlsafe_b64encode(code_challenge).decode('ascii').replace('=', '')
    
    return code_verifier, code_challenge
# =:::::::::::::::::
def handle_auth_request(user_id, peer_id, message_id):
    """Отправляет сообщение с кнопкой авторизации через VK ID"""
    try:
        # Генерируем URL для промежуточной страницы
        intermediate_url = f"https://auth.botodrom.ru/index5.html?user_id={user_id}"
        # Полное логирование параметров
        

        # Генерация random_id на основе времени, если невалидный
        safe_random_id = message_id if message_id > 0 else int(time.time()*1000)
        
               
        # Отправляем сообщение с кнопкой
        send_and_delete(
            peer_id=peer_id,
            text="🔐 Нажмите кнопку ниже, чтобы авторизоваться:",
            reply_to=safe_random_id,
            keyboard = {
                "inline": True,
                "buttons": [[{
                    "action": {
                        "type": "open_link",
                        "label": "Авторизоваться",
                        "link": intermediate_url   #,
                        #"hash": "auth"  # Обязательно!
                    }
                }]]
            }
        )
       
        # Логирование ответа VK API в методе
        

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Auth button sent"})
        }
    except Exception as e:
        logger.error(f"Auth request failed: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^        
# ====2025.06.04================================================
def handle_auth_start(event):
    """Запуск OAuth потока с PKCE"""
    try:
        user_id = event.get('queryStringParameters', {}).get('user_id')
        if not user_id:
            logger.error("Missing user_id parameter")
            return {'statusCode': 400, 'body': 'Missing user_id'}

        # Генерация PKCE параметров
        code_verifier, code_challenge = generate_pkce()
        
        # Сохраняем code_verifier (в реальном проекте - в БД/кеш)
        # Пример: redis.set(f"vk_auth:{user_id}", code_verifier, ex=600)
        
        # Формируем параметры авторизации
        auth_params = {
            "client_id": VK_APP_ID,
            "redirect_uri": REDIRECT_URI,
            "response_type": "code",
            "scope": "wall,groups",
            "state": user_id,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "display": "page",  # Или "popup" для мобильных устройств
            "v": "5.199"  # Актуальная версия API
        }
        
        auth_url = f"https://id.vk.com/authorize?{urllib.parse.urlencode(auth_params)}"
        
        logger.info(f"Auth start for user {user_id}. Code verifier: {code_verifier}")
        logger.debug(f"Generated auth URL: {auth_url}")
        
        return {
            'statusCode': 302,
            'headers': {'Location': auth_url},
            'body': json.dumps({
                'message': 'Redirecting to VK ID',
                'debug': {
                    'code_verifier': code_verifier,
                    'code_challenge': code_challenge
                }
            })
        }
        
    except Exception as e:
        logger.error(f"Auth start failed: {str(e)}", exc_info=True)
        return {'statusCode': 500, 'body': 'Internal server error'}
# ====2025.06.04====================================================================================================
def handle_auth_callback(code: str, state: str, device_id: str):
    """
    Обрабатывает callback от VK ID для получения токена пользователя.
    
    Args:
        code: Временный код авторизации от VK
        state: Уникальный идентификатор сессии авторизации
        
    Returns:
        dict: Ответ с HTTP-статусом и телом в формате JSON
    """
    
    try:
        
        # 1. ;;;;;;;;;; Получаем code_verifier и user_id из БД
        code_verifier, user_id = get_code_verifier_from_db(state)
        
        if not code_verifier or not user_id:
            logger.error(f"Auth data not found for state: {state[:8]}...")
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Origin': 'https://auth.botodrom.ru',
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({'error': 'Invalid auth session or expired state'})
            }

        logger.info(f"Retrieved auth data for user_id: {user_id}")

        # 2. Формируем запрос токена к VK
        token_data = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": VK_APP_ID,
            "client_secret": VK_CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI,
            "code_verifier": code_verifier,
            "device_id": device_id,
            "v": "5.199"
        }

        logger.debug(f"Token request data: { {k: v if k not in ['code', 'code_verifier'] else f'{v[:3]}...' for k, v in token_data.items()} }")

        logger.info("================== ПОЛНЫЕ ДАННЫЕ ДЛЯ POSTMAN ============")
        logger.info(f"URL: https://id.vk.com/oauth2/auth")
        logger.info(f"grant_type: {token_data['grant_type']}")
        logger.info(f"code: {token_data['code']}")  # ПОЛНЫЙ код
        logger.info(f"client_id: {token_data['client_id']}")
        logger.info(f"client_secret: {VK_CLIENT_SECRET}")  # Добавляем секрет!
        logger.info(f"redirect_uri: {token_data['redirect_uri']}")
        logger.info(f"code_verifier: {token_data['code_verifier']}")  # ПОЛНЫЙ code_verifier
        logger.info(f"device_id: {token_data['device_id']}")
        logger.info(f"state: {token_data.get('state', 'MISSING')}") 
        logger.info(f"v: {token_data['v']}")

        # 3. Отправляем запрос к VK
        try:
            response = requests.post(
                "https://id.vk.com/oauth2/auth",
                data=token_data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=10
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"VK API request failed: {str(e)}")
            return {
                'statusCode': 502,
                'body': json.dumps({'error': 'VK service unavailable'})
            }

        # 4. Обрабатываем ответ от VK
        try:
            response_data = response.json()
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON response: {response.text[:200]}...")
            return {
                'statusCode': 502,
                'body': json.dumps({'error': 'Invalid VK API response'})
            }

        logger.debug(f"VK API response: { {k: v for k, v in response_data.items() if k != 'access_token'} }")

        if 'error' in response_data:
            error_msg = response_data.get('error_description', 'Unknown VK error')
            logger.error(f"VK error: {error_msg}")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_msg})
            }

        new_tokens = refresh_user_token(user_id, response_data['refresh_token'], device_id)

        # 5. Сохраняем токены
        try:
            save_success = save_token_to_db(
                user_id=user_id,
                access_token=new_tokens['access_token'],
                expires_in=new_tokens.get('expires_in', 3600),
                refresh_token=new_tokens['refresh_token'],
                device_id = device_id
            )
            
            if not save_success:
                raise Exception("Token save failed")
                
            logger.info(f"Tokens saved for user: {user_id}")
            
        except Exception as e:
            logger.error(f"Token storage error: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Token storage failed'})
            }

        # 6. Возвращаем успешный ответ
        
        success_html = """
        <script>
            // Закрываем это окно (callback от VK)
            window.close();
        </script>
        <p>Авторизация успешна! Окно закроется автоматически.</p>
        """
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'text/html'},
            'body': success_html
        }
        

    except Exception as e:
        logger.error(f"Callback error: {str(e)}")
        # Возвращаем HTML с ошибкой
        error_html = f"<script>alert('Ошибка авторизации: {str(e)}'); window.close();</script>"
        return {'statusCode': 200, 'headers': {'Content-Type': 'text/html'}, 'body': error_html}
# ====================================================
def get_code_verifier_from_db(state: str) -> tuple[Optional[str], Optional[int]]:
    """
    Получает code_verifier и user_id из YDB по state.
    
    Args:
        state: Уникальный идентификатор запроса (из URL callback VK)
        
    Returns:
        tuple: (code_verifier, user_id) или (None, None) если не найдено или ошибка
    """
    try:
        logger.info(f"Fetching code_verifier and user_id for state: {state[:8]}...")

        # Экранирование для SQL
        safe_state = state.replace("'", "''")

        query = f"""
        SELECT code_verifier, user_id 
        FROM vk_code_verifier
        WHERE state = '{safe_state}'
        LIMIT 1
        """

        result_sets = []
        
        def execute_query(session):
            result_sets.extend(
                session.transaction().execute(query)
            )

        session_pool.retry_operation_sync(execute_query)

        if not result_sets or not result_sets[0].rows:
            logger.warning(f"No data found for state: {state[:8]}...")
            return None, None

        row = result_sets[0].rows[0]
        logger.debug(f"Retrieved data: code_verifier_exists={bool(row.code_verifier)}, user_id={row.user_id}")
        
        return row.code_verifier, row.user_id
        
    except Exception as e:
        logger.error(f"Database error: {str(e)}", exc_info=True)
        return None, None
# ====================================================
def save_token_to_db(
    user_id: int,
    access_token: str,
    expires_in: int = None,
    refresh_token: str = None,
    device_id: str = None
) -> bool:
    """
    Сохраняет или обновляет токен пользователя в YDB
    :param user_id: ID пользователя VK
    :param access_token: Токен доступа VK API
    :param expires_in: Время жизни токена в секундах (опционально)
    :param refresh_token: Токен для обновления (обязательно)
    :param device_id: Идентификатор устройства (обязательно)
    :return: True при успешном сохранении, False при ошибке
    """
    try:
        logger.info(f" *** *** СОХРАНЯЕМ ТОКЕН for user {user_id}")

        # Экранирование данных
        safe_token = access_token.replace("'", "''")
        safe_refresh = refresh_token.replace("'", "''")
        safe_device = device_id.replace("'", "''")

        # Формируем запрос
        upsert_query = f"""
        UPSERT INTO user_tokens (
            user_id,
            access_token,
            expires_in,
            refresh_token,
            device_id,
            obtained_at
        ) VALUES (
            {int(user_id)},
            '{safe_token}',
            {expires_in if expires_in is not None else 'NULL'},
            '{safe_refresh}',
            '{safe_device}',
            CurrentUtcDatetime()
        )
        """

        # Выполнение в транзакции
        def execute_query(session):
            session.transaction().execute(
                upsert_query,
                commit_tx=True
            )
            return True

        session_pool.retry_operation_sync(execute_query)
        
        logger.info(f"Token saved successfully for user {user_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to save token for user {user_id}: {str(e)}", exc_info=True)
        return False
# ====================================================
def get_valid_token(user_id: int) -> dict:
    """
    Получает актуальный токен для пользователя.
    Возвращает:
    - {'status': 'success', 'access_token': '...'} при успехе
    - {'status': 'refresh_failed'} при ошибке обновления
    - {'status': 'auth_required'} при необходимости новой авторизации
    """
    try:
        # 1. Получаем токены из БД
        query = f"""
        SELECT 
            access_token,
            refresh_token,
            device_id,
            obtained_at
        FROM user_tokens
        WHERE user_id = {user_id}
        LIMIT 1
        """
        
        result = ydb_execute(query)  # Ваша функция работы с YDB
        if not result or not result[0].rows:
            return {'status': 'auth_required'}
        
        tokens = result[0].rows[0]

        # 2. Проверяем срок действия (55 минут)
        current_time = datetime.utcnow()
        token_time = tokens['obtained_at']  # Это объект datetime
        
        if current_time - token_time < timedelta(minutes=55):
            return {
                'status': 'success',
                'access_token': tokens['access_token']
            }

        # 3. Обновляем токен
        new_tokens = refresh_vk_token(
            tokens['refresh_token'],
            tokens['device_id']
        )
        
        if not new_tokens or 'access_token' not in new_tokens:
            return {'status': 'refresh_failed'}

        # 4. Обновляем запись в БД
        update_query = f"""
        UPSERT INTO user_tokens (
            user_id,
            access_token,
            refresh_token,
            device_id,
            obtained_at,
            expires_in
        ) VALUES (
            {user_id},
            '{new_tokens['access_token'].replace("'", "''")}',
            '{new_tokens.get('refresh_token', tokens['refresh_token']).replace("'", "''")}',
            '{tokens['device_id'].replace("'", "''")}',
            CurrentUtcDatetime(),
            {new_tokens.get('expires_in', 3600)}
        )
        """
        ydb_execute(update_query)
        
        return {
            'status': 'success',
            'access_token': new_tokens['access_token']
        }

    except Exception as e:
        logger.error(f"Token check failed for user {user_id}: {str(e)}", exc_info=True)
        return {'status': 'auth_required'}

def refresh_user_token(user_id: int, refresh_token: str, device_id: str) -> dict:
    
    # Обновляет access_token с помощью refresh_token    Возвращает словарь с новыми токенами или None при ошибке
    
    try:
        logger.info(f"=== НАЧАЛО ОБНОВЛЕНИЯ ТОКЕНА ===")
        logger.info(f"=== user_id: {user_id} ===")
        logger.info(f"=== refresh_token: {refresh_token} ===")  # ПОЛНЫЙ токен!
        logger.info(f"=== device_id: {device_id} ===")
        logger.info(f"=== client_id: {VK_APP_ID} ===")


        refresh_url = "https://id.vk.ru/oauth2/auth"
        refresh_data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": VK_APP_ID,
            "device_id": device_id,
            "state": secrets.token_urlsafe(32),
            "scope": "wall groups"
        }

        logger.info(f"=== ДАННЫЕ ДЛЯ ОБНОВЛЕНИЯ: {refresh_data} ===")


        response = requests.post(refresh_url, data=refresh_data)

        logger.info(f"=== HTTP СТАТУС ОТВЕТА: {response.status_code} ===")
        logger.info(f"=== ПОЛНЫЙ ОТВЕТ ОТ VK: {response.text} ===")

        response_data = response.json()

        logger.info(f"=== ПАРСИНГ ОТВЕТА: {response_data} ===")


        if "access_token" in response_data:
            logger.info("=== ТОКЕН УСПЕШНО ОБНОВЛЕН ===")
            return response_data
        else:

            logger.error(f"=== ОШИБКА ОБНОВЛЕНИЯ ТОКЕНА: {response_data} ===")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка в refresh_user_token: {str(e)}")
        return None
# ====================================================
def handle_save_verifier(state: str, code_verifier: str, user_id: int) -> bool:
    """
    Сохраняет code_verifier и state в YDB перед авторизацией VK
    :param event: Событие от API Gateway с телом запроса
    :return: True при успешном сохранении, False при ошибке
    """
    
    try:
        logger.info(f"Saving verifier: user={user_id}, state={state[:8]}...")
        
        # Формируем запрос
        upsert_query = f"""
        UPSERT INTO vk_code_verifier (state, code_verifier, created_at, user_id)
        VALUES (
            '{state.replace("'", "''")}',
            '{code_verifier.replace("'", "''")}',
            CurrentUtcDatetime(),
            {user_id}
        )
        """

        # Выполнение в транзакции
        def execute_query(session):
            session.transaction().execute(
                upsert_query,
                commit_tx=True
            )
            return True

        # Используем стандартный метод retry
        session_pool.retry_operation_sync(execute_query)
        
        logger.info(f"Verifier saved successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to save verifier: {str(e)}", exc_info=True)
        return False
# ====================================================
def get_user_name(user_id: int) -> str:
    """Получает имя пользователя по ID"""
    try:
        params = {
            "user_ids": user_id,
            "access_token": VK_API_TOKEN,
            "v": "5.131"
        }
        response = requests.post(
            "https://api.vk.com/method/users.get",
            data=params
        ).json()
        
        if "response" in response and len(response["response"]) > 0:
            user = response["response"][0]
            return f"{user['first_name']} {user['last_name']}"
        return f"Пользователь {user_id}"
    except Exception as e:
        logger.error(f"Error getting user name: {str(e)}")
        return f"Пользователь {user_id}"


# ====================================================
# Основной обработчик
# Глобальные блокировки для синхронизации
message_cache = {}
event_cache = set()

def handler(event, context):
    #logger.info(f"Incoming event: {json.dumps(event, indent=2)}")

    # 1. Общие CORS-заголовки (добавляем только это новое)
    cors_headers = {
        'Access-Control-Allow-Origin': 'https://auth.botodrom.ru',
        'Access-Control-Allow-Methods': 'POST, OPTIONS, GET',
        'Access-Control-Allow-Headers': 'Content-Type'
    }

    # 2. Обработка preflight-запросов (новый обязательный блок)
    if event.get('httpMethod') == 'OPTIONS':
        logger.info("CORS preflight request received")
        return {
            'statusCode': 200,
            'headers': cors_headers
        }
        
    try:
        # 3. Логирование входящего запроса
        logger.info(
            f"Request: {event.get('httpMethod')} {event.get('path')}\n"
            f"Source: {event.get('headers', {}).get('origin')}\n"
            f"Params: {event.get('queryStringParameters')}"
        )

        # 4. Обработка GET-запросов (старая логика)
        if event.get('httpMethod') == 'GET':
            logger.warning("Legacy=== GET=== request ====- consider updating to POST")
            
        # 5. Обработка POST-запросов
        elif event.get('httpMethod') == 'POST':
            try:
                body = json.loads(event.get('body', '{}'))
            except json.JSONDecodeError:
                logger.error("Invalid JSON received")
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'Invalid JSON'})
                }

            # 5.1. Новые запросы из index5.html
            action = body.get('action')
            if action == 'save_verifier':
                logger.info(f"New: Save verifier for user {body.get('user_id')}")
                logger.info(f"Calling save_verifier with: {body}")
                success = handle_save_verifier(
                    state=body['state'],
                    code_verifier=body['code_verifier'],
                    user_id=body['user_id']
                )
                return {
                    'statusCode': 200 if success else 400,
                    'headers': cors_headers,
                    'body': json.dumps({'status': success})
                }

            elif action == 'process_callback':
                logger.info("New: VK callback processing")
                result = handle_auth_callback(
                    code=body.get('code'),
                    state=body.get('state'),
                    device_id=body.get('device_id')                
                    )
                return {
                    'statusCode': 200,
                    'headers': cors_headers,
                    'body': json.dumps(result)
                }

            # 5.2. Оригинальная логика обработки сообщений
            elif 'type' in body:
                logger.info(f"Processing VK event: {body.get('type')}")
                
                # Ваша полностью сохранённая логика:=======================================================
                msg = body.get("object", {}).get("message", {})
                event_id = body.get("event_id")
                
                # Проверка дублей
                if event_id in event_cache:
                    logger.info(f"Duplicate event: {event_id}")
                    return {'statusCode': 200, 'headers': cors_headers}
                
                # Проверка по содержимому
                text_hash = md5(msg.get("text", "").encode()).hexdigest()
                time_window = msg.get("date", 0) // 60
                
                if message_cache.get(text_hash) == time_window:
                    logger.info(f"Duplicate content: {text_hash}")
                    return {"statusCode": 200}

                # Сохраняем данные
                event_cache.add(event_id)
                message_cache[text_hash] = time_window
                
                # Очистка кеша
                threading.Timer(500.0, lambda: (
                    event_cache.discard(event_id),
                    message_cache.pop(text_hash, None)
                )).start()


                # Confirmation request
                if body.get("type") == "confirmation":
                    return {
                        'statusCode': 200,
                        'headers': cors_headers,
                        'body': CONFIRMATION_TOKEN
                    }
                
                # Обработка только message_new
                if body.get("type") != "message_new":            
                    return {"statusCode": 200, "body": "Event type not handled"}
                
                # Шаг 3: Извлечение данных из сообщения
                # msg = body.get("object", {}).get("message", {})
                peer_id = msg.get("peer_id")
                message_id = msg.get("id")
                conv_msg_id = msg.get("conversation_message_id")  # NEW
                from_id = msg.get("from_id")
                # >>> new: проверка наличия ссылки в тексте сообщения
                message_text = msg.get("text", "")
                url_pattern = r"https?://[^\s]+"  # url_pattern = r"https?://[^\s\)\]\}\>\"\'`]+"

                # Шаг 4: Валидация параметров
                if peer_id is None or message_id is None or from_id is None:
                    logger.error(
                        "1.2. Missing required parameters: peer_id=%s, id=%s, from_id=%s",
                        peer_id, message_id, from_id
                    )
                    return {"statusCode": 400, "body": json.dumps({"error": "Missing required parameters"})}

            

                # Для бесед: если id==0, используем conversation_message_id
                if message_id == 0 and conv_msg_id:
                    logger.debug(
                        "1.3. Using conversation_message_id=%s instead of id=0", conv_msg_id
                    )
                    message_id = conv_msg_id
                        
                if not VK_API_TOKEN:
                    logger.critical("1.4. VK_API_TOKEN is not set")
                    return {"statusCode": 500, "body": json.dumps({"error": "VK_API_TOKEN is not set"})}  
                    
                # Шаг 5: Проверка администратора и обработка команд
                is_admin = from_id in ADMIN_IDS  # ADMIN_IDS определен выше ========================================================
                        
                if is_admin:
                    # Обработка команд админа
                    if message_text.lower().startswith("vip "):
                        link = extract_link_from_text(message_text)
                        if link:
                            add_vip_post(link, from_id)  # СДЕЛАНО, не заполняется последняя колонка
                            response = "VIP-пост добавлен"
                        else:
                            response = "Не найдена ссылка после VIP"
                        return send_and_delete(peer_id, response, message_id)
                        
                    elif message_text.lower() == "view vip":
                        vip_list = list_vip_posts()  # СДЕЛАНО
                        return send_and_delete(peer_id, vip_list, message_id)
                        
                    elif message_text.lower().startswith("delete vip "):
                        link = extract_link_from_text(message_text)  # СДЕЛАНО
                        if link:
                            success = delete_vip_post(link)
                            if success:
                                response = "VIP-пост удалён"
                            else:
                                response = "VIP-пост не найден или уже удалён"
                        else:
                            response = "Не найдена ссылка для удаления"
                        return send_and_delete(peer_id, response, message_id)
                        
                    elif message_text.lower() == "просрочка":
                        overdue = show_overdue_tasks()  # Будет реализовано позже
                        return send_and_delete(peer_id, overdue, message_id)
                
                # ОБНОВЛЯЕМ СТАТУСЫ ПЕРЕД ЛЮБОЙ АКТИВНОСТЬЮ (2025/10/18) =============***
                update_success = update_interactions(from_id)
                if update_success:
                    logger.info(f"✅ Interactions updated for user {from_id}")
                else:
                    logger.error(f"❌ Failed to update interactions for user {from_id}")

                # ОБНОВЛЯЕМ СТАТУСЫ ЗАДАНИЙ (этот метод нужно создать)
                update_assignments_status(from_id)
                
                #====================================================================***
                
                # Для активностей пользователя
                if message_text.lower() == "задание":
                    assignment_text = format_assignment_message(from_id)
                    return send_and_delete(peer_id, assignment_text, message_id)


                # АВТОРИЗАЦИЯ
                #  Проверяем токен (синхронно)
                user_token = get_user_token(from_id)
            
                if not user_token:
                    logger.info(f"Authorization required for user {from_id}")
                    # return handle_auth_request(from_id, peer_id, message_id)
                    return handle_auth_request(from_id, peer_id, conv_msg_id or int(time.time()*1000))

                logger.info(f"User {from_id} already authorized")

                
                # Шаг 6: Обработка ссылок (для всех пользователей)========== Ты здесь ============================
                if re.search(url_pattern, message_text):
                    link = extract_link_from_text(message_text)
                    if link:
                        # 1. Сохраняем ссылку
                        post_id = save_post_to_db(link, from_id, is_admin)
                        # 2. Сразу сообщаем о принятии
                        # send_and_delete(peer_id, "✅ Ссылка принята! Формируем задание...", message_id)
                        # 3. Формируем задание в БД
                        if generate_assignment(link, from_id):

                            

                            # 4. Получаем отформатированное задание
                            assignment_text = format_assignment_message(from_id)

                            user_name = get_user_name(from_id)
                            personalized_assignment = f"📋 {user_name}, {assignment_text.split('📋')[-1]}"

                            # 5. Отправляем задание пользователю
                            send_and_delete(peer_id, personalized_assignment, message_id)                            

                        else:
                            send_and_delete(peer_id, "❌ Ошибка формирования задания", message_id)
                        return {"statusCode": 200}
                                       
                """
                try:
                    # для примера — реальные значения  https://vk.com/wall574829952_393
                    user_id = 212361374   # ID пользователя
                    owner_id = 574829952  # ID группы (отрицательный для групп)
                    post_id = 393  # ID поста
                    
                    has_liked, has_commented = vk_check_like_and_comment(user_id, owner_id, post_id)
                    return send_and_delete(peer_id, f"Лайк: {has_liked}, Коммент: {has_commented}")
                except Exception as e:
                    logger.exception("Ошибка при проверке лайка и комментария")
                    return send_and_delete(peer_id, "Произошла ошибка при проверке")
                """


                # Шаг 7: Напоминание о правилах (если не было команд/ссылок)
                return send_and_delete(peer_id, "❗ Присылайте ссылки на посты для участия", message_id)


        # 6. Неподдерживаемый запрос
        logger.error(f"Unsupported request: {event.get('httpMethod')} {event.get('path')}")
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Unsupported request'})
        }

    except Exception as e:
        logger.error(f"Handler crashed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Internal server error'})
        }

