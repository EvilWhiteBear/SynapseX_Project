#!/usr/bin/env python3
"""
Запусти этот скрипт ОДИН РАЗ на сервере:
  python3 fix_firebase.py

Исправляет firebase_key.json — убирает невидимые спецсимволы
которые вызывают ошибку "Invalid control character at line 1 column 162"
"""
import json, re, os, shutil

KEY_FILE = "firebase_key.json"

if not os.path.exists(KEY_FILE):
    print(f"✗ Файл {KEY_FILE} не найден!")
    print("  Убедитесь что скрипт запускается из корня проекта (рядом с app.py)")
    exit(1)

# Бэкап
shutil.copy(KEY_FILE, KEY_FILE + ".bak")
print(f"✓ Бэкап создан: {KEY_FILE}.bak")

with open(KEY_FILE, 'r', encoding='utf-8') as f:
    raw = f.read()

print(f"  Оригинальный размер: {len(raw)} байт")

# Убираем BOM если есть
raw = raw.lstrip('\ufeff')

# Убираем control characters (кроме \n \t \r)
raw_fixed = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', raw)

if raw_fixed != raw:
    removed = len(raw) - len(raw_fixed)
    print(f"  Удалено невидимых символов: {removed}")
else:
    print("  Невидимых символов не найдено")

# Парсим JSON
try:
    data = json.loads(raw_fixed)
    print("✓ JSON парсится корректно")
except json.JSONDecodeError as e:
    print(f"✗ JSON ошибка после чистки: {e}")
    # Пробуем агрессивную замену
    raw_fixed = raw_fixed.replace('\r\n', '\n').replace('\r', '\n')
    try:
        data = json.loads(raw_fixed)
        print("✓ JSON исправлен заменой переносов строк")
    except json.JSONDecodeError as e2:
        print(f"✗ Не удалось исправить: {e2}")
        print("  Скачайте новый firebase_key.json из Firebase Console")
        exit(1)

# Исправляем private_key — должен содержать настоящие \n не \\n
if 'private_key' in data:
    pk = data['private_key']
    # Заменяем литеральные \\n на настоящие \n
    pk_fixed = pk.replace('\\n', '\n')
    # Убеждаемся что ключ начинается правильно
    if '-----BEGIN RSA PRIVATE KEY-----' in pk_fixed or '-----BEGIN PRIVATE KEY-----' in pk_fixed:
        data['private_key'] = pk_fixed
        print(f"✓ private_key исправлен ({pk.count('\\\\n')} замен)")
    else:
        print(f"  private_key выглядит корректно")

# Проверяем обязательные поля
required = ['type', 'project_id', 'private_key_id', 'private_key', 
            'client_email', 'client_id', 'auth_uri', 'token_uri']
missing = [f for f in required if f not in data]
if missing:
    print(f"⚠ Отсутствуют поля: {missing}")
else:
    print(f"✓ Все обязательные поля присутствуют")
    print(f"  project_id: {data.get('project_id')}")
    print(f"  client_email: {data.get('client_email')}")

# Сохраняем исправленный файл
with open(KEY_FILE, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print(f"\n✓ {KEY_FILE} сохранён (размер: {os.path.getsize(KEY_FILE)} байт)")
print("\n→ Перезапусти приложение: touch Procfile  или  systemctl restart synapsex")
