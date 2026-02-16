# -*- coding: utf-8 -*-
"""
Сервіс для управління користувачами Telegram бота.
"""

from typing import Optional, Dict, Any
from data.repositories.users_repository import UsersRepository


class UserService:
    """Сервіс для управління користувачами (білий список)."""
    
    def __init__(self, users_config_path: str = None):
        """
        Ініціалізація сервісу.
        
        Args:
            users_config_path: Шлях до YAML файлу з користувачами (залишено для сумісності, не використовується)
        """
        self.repository = UsersRepository()
    
    def is_user_authorized(self, user_id: int) -> bool:
        """
        Перевіряє, чи авторизований користувач.
        
        Args:
            user_id: Ідентифікатор користувача Telegram
            
        Returns:
            bool: True якщо користувач авторизований та не заблокований
        """
        user = self.get_user(user_id)
        if not user:
            return False
        
        return not user.get('is_blocked', False)
    
    def is_admin(self, user_id: int) -> bool:
        """
        Перевіряє, чи є користувач адміністратором.
        
        Args:
            user_id: Ідентифікатор користувача Telegram
            
        Returns:
            bool: True якщо користувач є адміністратором
        """
        user = self.get_user(user_id)
        if not user:
            return False
        
        return user.get('role') == 'admin' and not user.get('is_blocked', False)
    
    def get_admin_user_ids(self):
        """
        Повертає список Telegram user_id усіх адміністраторів (для сповіщень, напр. від агента безпеки).

        Returns:
            List[int]: Список user_id з role='admin' та не заблокованих
        """
        users = self.repository.get_all_users()
        return [
            int(u["user_id"])
            for u in users
            if u.get("role") == "admin" and not u.get("is_blocked", False)
        ]

    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Отримує інформацію про користувача.
        
        Args:
            user_id: Ідентифікатор користувача Telegram
            
        Returns:
            Dict з інформацією про користувача або None
        """
        return self.repository.find_by_user_id(user_id)
    
    def add_user(
        self,
        user_id: int,
        role: str,
        nickname: str,
        modified_by: int
    ) -> bool:
        """
        Додає нового користувача.
        
        Args:
            user_id: Ідентифікатор користувача Telegram
            role: Роль користувача ('user' або 'admin')
            nickname: Псевдонім користувача
            modified_by: Ідентифікатор користувача, який вніс зміни
            
        Returns:
            bool: True якщо додавання успішне, False інакше
        """
        return self.repository.create_user(user_id, role, nickname, modified_by)
    
    def block_user(self, user_id: int, modified_by: int) -> bool:
        """
        Блокує користувача.
        
        Args:
            user_id: Ідентифікатор користувача для блокування
            modified_by: Ідентифікатор користувача, який вніс зміни
            
        Returns:
            bool: True якщо блокування успішне, False інакше
        """
        return self.repository.update_user(
            user_id,
            {'is_blocked': True},
            modified_by
        )
    
    def unblock_user(self, user_id: int, modified_by: int) -> bool:
        """
        Розблоковує користувача.
        
        Args:
            user_id: Ідентифікатор користувача для розблоковування
            modified_by: Ідентифікатор користувача, який вніс зміни
            
        Returns:
            bool: True якщо розблоковування успішне, False інакше
        """
        return self.repository.update_user(
            user_id,
            {'is_blocked': False},
            modified_by
        )
    
    def get_user_nickname(self, user_id: int) -> Optional[str]:
        """
        Отримує псевдонім користувача.
        
        Args:
            user_id: Ідентифікатор користувача Telegram
            
        Returns:
            Псевдонім користувача або None
        """
        user = self.get_user(user_id)
        if user:
            return user.get('nickname')
        return None
