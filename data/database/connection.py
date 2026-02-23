# -*- coding: utf-8 -*-
"""
Модуль для підключення до MongoDB.
"""

import os
from typing import Optional
from pymongo import MongoClient
from pymongo.database import Database
from config.settings import Settings


class MongoDBConnection:
    """Клас для управління підключенням до MongoDB."""
    
    _client: Optional[MongoClient] = None
    _database: Optional[Database] = None
    _init_pid: Optional[int] = None  # PID процесу, в якому ініціалізовано (для fork-safety)
    
    @classmethod
    def initialize(cls, settings: Settings) -> None:
        """
        Ініціалізує підключення до MongoDB.
        Після fork (uvicorn workers) створює нове підключення — PyMongo не підтримує
        використання одного клієнта в кількох процесах.
        """
        pid = os.getpid()
        if cls._client is None or cls._init_pid != pid:
            if cls._client is not None:
                try:
                    cls._client.close()
                except Exception:
                    pass
                cls._client = None
                cls._database = None
            connection_string = cls._build_connection_string(settings)
            cls._client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000
            )
            cls._database = cls._client[settings.mongodb_database_name]
            cls._init_pid = pid
            
            # Перевірка підключення
            try:
                cls._client.admin.command('ping')
            except Exception as e:
                raise ConnectionError(f"Не вдалося підключитися до MongoDB: {e}")
    
    @classmethod
    def _build_connection_string(cls, settings: Settings) -> str:
        """
        Формує рядок підключення до MongoDB.
        
        Args:
            settings: Об'єкт налаштувань застосунку
            
        Returns:
            Рядок підключення до MongoDB
        """
        if settings.mongodb_username and settings.mongodb_password:
            return (
                f"mongodb://{settings.mongodb_username}:{settings.mongodb_password}"
                f"@{settings.mongodb_host}:{settings.mongodb_port}/"
                f"?authSource={settings.mongodb_auth_source}"
            )
        else:
            return f"mongodb://{settings.mongodb_host}:{settings.mongodb_port}/"
    
    @classmethod
    def get_database(cls) -> Database:
        """
        Отримує об'єкт бази даних.
        
        Returns:
            Об'єкт бази даних MongoDB
            
        Raises:
            RuntimeError: Якщо підключення не ініціалізовано
        """
        if cls._database is None:
            raise RuntimeError("Підключення до MongoDB не ініціалізовано. Викличте MongoDBConnection.initialize()")
        return cls._database
    
    @classmethod
    def get_client(cls) -> MongoClient:
        """
        Отримує клієнт MongoDB.
        
        Returns:
            Клієнт MongoDB
            
        Raises:
            RuntimeError: Якщо підключення не ініціалізовано
        """
        if cls._client is None:
            raise RuntimeError("Підключення до MongoDB не ініціалізовано. Викличте MongoDBConnection.initialize()")
        return cls._client
    
    @classmethod
    def close(cls) -> None:
        """Закриває підключення до MongoDB."""
        if cls._client:
            cls._client.close()
            cls._client = None
            cls._database = None
            cls._init_pid = None
