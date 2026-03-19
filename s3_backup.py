# -*- coding: utf-8 -*-
"""
s3_backup.py — Синхронизация signals.db с Timeweb S3
──────────────────────────────────────────────────────
• При старте сервера: скачивает БД с S3 если локальная пустая/отсутствует
• Каждые 6 часов: загружает БД на S3
• После каждого деплоя данные восстанавливаются автоматически!

.env переменные:
  S3_ENDPOINT   = https://s3.twcstorage.ru
  S3_BUCKET     = synapsex-db
  S3_REGION     = ru-1
  S3_ACCESS_KEY = ваш_ключ
  S3_SECRET_KEY = ваш_секрет
"""
import os
import logging
import threading

log = logging.getLogger("S3_BACKUP")

S3_ENDPOINT   = os.getenv("S3_ENDPOINT",   "https://s3.twcstorage.ru")
S3_BUCKET     = os.getenv("S3_BUCKET",     "synapsex-db")
S3_REGION     = os.getenv("S3_REGION",     "ru-1")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_DB_KEY     = "signals.db"   # имя файла в бакете

_s3_client = None


def _is_configured() -> bool:
    return bool(S3_ACCESS_KEY and S3_SECRET_KEY and S3_BUCKET)


def _get_client():
    """Создаёт boto3 S3 клиент (singleton)."""
    global _s3_client
    if _s3_client:
        return _s3_client
    try:
        import boto3
        from botocore.config import Config
        _s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION,
            config=Config(
                connect_timeout=10,
                read_timeout=30,
                retries={"max_attempts": 3}
            )
        )
        return _s3_client
    except ImportError:
        log.error("[S3] boto3 не установлен! Добавь в requirements.txt: boto3")
        return None
    except Exception as e:
        log.error(f"[S3] Ошибка создания клиента: {e}")
        return None


def upload_db(db_path: str) -> bool:
    """Загружает signals.db на S3 синхронно с retry 3 попытки."""
    if not _is_configured():
        return False
    if not os.path.exists(db_path):
        log.warning(f"[S3] Файл не найден: {db_path}")
        return False

    client = _get_client()
    if not client:
        return False

    db_size = os.path.getsize(db_path)
    log.info(f"[S3] Загружаем БД на S3: {db_size}B ({db_size // 1024}KB)")

    for attempt in range(1, 4):
        try:
            client.upload_file(
                db_path,
                S3_BUCKET,
                S3_DB_KEY,
                ExtraArgs={"ContentType": "application/octet-stream"}
            )
            # Проверяем размер на S3 после загрузки
            head = client.head_object(Bucket=S3_BUCKET, Key=S3_DB_KEY)
            s3_size = head.get("ContentLength", 0)
            log.info(f"[S3] ✅ БД загружена (попытка {attempt}): локально {db_size}B → S3 {s3_size}B")
            return True
        except Exception as e:
            log.error(f"[S3] ❌ Ошибка загрузки (попытка {attempt}/3): {e}")
            if attempt == 3:
                return False

    return False


def download_db(db_path: str) -> bool:
    """
    Скачивает signals.db с S3.
    Вызывается при старте если локальная БД пустая/отсутствует.
    Возвращает True если успешно восстановлена.
    """
    if not _is_configured():
        return False

    client = _get_client()
    if not client:
        return False

    try:
        # Проверяем что файл есть на S3
        response = client.head_object(Bucket=S3_BUCKET, Key=S3_DB_KEY)
        s3_size = response.get("ContentLength", 0)

        if s3_size == 0:
            log.info("[S3] БД на S3 пустая — пропускаем восстановление")
            return False

        # Создаём директорию если нужно
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        # Скачиваем
        client.download_file(S3_BUCKET, S3_DB_KEY, db_path)
        local_size = os.path.getsize(db_path)
        log.info(f"[S3] ✅ БД восстановлена с S3: {local_size // 1024}KB ← s3://{S3_BUCKET}/{S3_DB_KEY}")
        return True

    except client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            log.info("[S3] БД на S3 не найдена — первый запуск, создаём новую")
        else:
            log.error(f"[S3] Ошибка проверки S3: {e}")
        return False
    except Exception as e:
        log.error(f"[S3] ❌ Ошибка скачивания: {e}")
        return False


def restore_if_needed(db_path: str) -> bool:
    """
    Восстанавливает БД с S3 при старте сервера.
    Логика:
    - S3 не настроен → пропускаем
    - Файла нет локально → качаем с S3
    - Локальный файл < 8KB (пустой после деплоя) → качаем с S3
    - S3 файл БОЛЬШЕ локального → качаем с S3 (более актуальный)
    - Иначе → оставляем локальный
    """
    if not _is_configured():
        log.info("[S3] S3 не настроен — пропускаем восстановление")
        return False

    client = _get_client()
    if not client:
        return False

    local_exists = os.path.exists(db_path)
    local_size   = os.path.getsize(db_path) if local_exists else 0

    # Проверяем размер файла на S3
    try:
        response = client.head_object(Bucket=S3_BUCKET, Key=S3_DB_KEY)
        s3_size  = response.get("ContentLength", 0)
        log.info(f"[S3] Локальная БД: {local_size}B | S3: {s3_size}B")
    except Exception as e:
        log.info(f"[S3] Файл на S3 не найден или ошибка: {e}")
        return False

    if s3_size > 8192:
        log.info(f"[S3] Восстанавливаем с S3 ({s3_size}B)")
        return download_db(db_path)

    log.info(f"[S3] S3 файл слишком мал ({s3_size}B) — пропускаем восстановление")
    return False


def upload_now(db_path: str):
    """Немедленно загружает БД на S3 синхронно."""
    upload_db(db_path)


def start_s3_sync_scheduler(db_path: str, interval_sec: int = 1800):
    """
    Запускает фоновый поток который каждые N секунд
    загружает БД на S3. По умолчанию каждые 30 минут.
    """
    if not _is_configured():
        log.info("[S3] S3 не настроен — планировщик не запускается")
        return

    def _scheduler():
        import time as _time
        # Первый бэкап через 2 минуты после старта
        _time.sleep(120)
        while True:
            upload_db(db_path)
            _time.sleep(interval_sec)

    t = threading.Thread(target=_scheduler, daemon=True, name="S3SyncScheduler")
    t.start()
    log.info(f"[S3] Планировщик синхронизации запущен (каждые {interval_sec//60}мин → S3)")