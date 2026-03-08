# -*- coding: utf-8 -*-
"""
Репозиторій для роботи з колекцією users.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository


class UsersRepository(BaseRepository):
    """Репозиторій для роботи з користувачами."""
    
    def __init__(self):
        """Ініціалізація репозиторію."""
        super().__init__('users')
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index('user_id', unique=True)
            self._indexes_created = True
        except Exception:
            pass
    
    def find_by_user_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Знаходить користувача за ідентифікатором.
        
        Args:
            user_id: Ідентифікатор користувача Telegram
            
        Returns:
            Документ або None, якщо не знайдено
        """
        self._ensure_indexes()
        return self.find_one({'user_id': user_id})
    
    def create_user(
        self,
        user_id: int,
        role: str,
        nickname: str,
        modified_by: int
    ) -> bool:
        """
        Створює нового користувача.
        
        Args:
            user_id: Ідентифікатор користувача Telegram
            role: Роль користувача ('user' або 'admin')
            nickname: Псевдонім користувача
            modified_by: Ідентифікатор користувача, який вніс зміни
            
        Returns:
            True якщо успішно, False якщо користувач вже існує
        """
        existing = self.find_by_user_id(user_id)
        if existing:
            return False
        
        document = {
            'user_id': user_id,
            'role': role,
            'nickname': nickname,
            'is_blocked': False,
            'last_modified_by': str(modified_by),
            'last_modified_at': datetime.now(timezone.utc)
        }
        
        self.create(document)
        return True
    
    def update_user(
        self,
        user_id: int,
        update_data: Dict[str, Any],
        modified_by: int
    ) -> bool:
        """
        Оновлює користувача.
        
        Args:
            user_id: Ідентифікатор користувача
            update_data: Дані для оновлення
            modified_by: Ідентифікатор користувача, який вніс зміни
            
        Returns:
            True якщо успішно
        """
        user = self.find_by_user_id(user_id)
        if not user:
            return False
        
        update_data['last_modified_by'] = str(modified_by)
        update_data['last_modified_at'] = datetime.now(timezone.utc)
        
        return self.update_by_id(user['_id'], {'$set': update_data})
    
    def get_all_users(self) -> List[Dict[str, Any]]:
        """
        Отримує всіх користувачів.
        
        Returns:
            Список користувачів
        """
        return self.find_many(sort=[('user_id', 1)])
