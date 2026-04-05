# -*- coding: utf-8 -*-
"""Добавляет корень проекта в sys.path для импорта модулей."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
