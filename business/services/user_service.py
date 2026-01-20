# -*- coding: utf-8 -*-
"""
Сервіс для управління користувачами Telegram бота.
"""

import yaml
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone


class UserService:
    """Сервіс для управління користувачами (білий список)."""
    
    def __init__(self, users_config_path: str):
        """
        Ініціалізація сервісу.
        
        Args:
            users_config_path: Шлях до YAML файлу з користувачами
        """
        self.users_config_path = Path(users_config_path)
        self._users: List[Dict[str, Any]] = []
        self._load_users()
    
    def _load_users(self) -> None:
        """Завантажує користувачів з YAML файлу."""
        if not self.users_config_path.exists():
            # Якщо файл не існує, створюємо порожній список
            self._users = []
            return
        
        try:
            with open(self.users_config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                if config and 'users' in config:
                    self._users = config['users'] or []
                else:
                    self._users = []
        except Exception as e:
            print(f"Помилка завантаження користувачів з {self.users_config_path}: {e}")
            self._users = []
    
    def _save_users(self) -> bool:
        """
        Зберігає користувачів у YAML файл.
        
        Returns:
            bool: True якщо збереження успішне, False інакше
        """
        try:
            # Створюємо директорію, якщо не існує
            self.users_config_path.parent.mkdir(parents=True, exist_ok=True)
            
            config = {'users': self._users}
            with open(self.users_config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
            return True
        except Exception as e:
            print(f"Помилка збереження користувачів у {self.users_config_path}: {e}")
            return False
    
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
    
    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Отримує інформацію про користувача.
        
        Args:
            user_id: Ідентифікатор користувача Telegram
            
        Returns:
            Dict з інформацією про користувача або None
        """
        for user in self._users:
            if user.get('user_id') == user_id:
                return user
        return None
    
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
        # Перевіряємо, чи користувач вже існує
        if self.get_user(user_id):
            return False
        
        new_user = {
            'user_id': user_id,
            'role': role,
            'nickname': nickname,
            'is_blocked': False,
            'last_modified_by': str(modified_by),
            'last_modified_at': datetime.now(timezone.utc).isoformat()
        }
        
        self._users.append(new_user)
        return self._save_users()
    
    def block_user(self, user_id: int, modified_by: int) -> bool:
        """
        Блокує користувача.
        
        Args:
            user_id: Ідентифікатор користувача для блокування
            modified_by: Ідентифікатор користувача, який вніс зміни
            
        Returns:
            bool: True якщо блокування успішне, False інакше
        """
        user = self.get_user(user_id)
        if not user:
            return False
        
        user['is_blocked'] = True
        user['last_modified_by'] = str(modified_by)
        user['last_modified_at'] = datetime.now(timezone.utc).isoformat()
        
        return self._save_users()
    
    def unblock_user(self, user_id: int, modified_by: int) -> bool:
        """
        Розблоковує користувача.
        
        Args:
            user_id: Ідентифікатор користувача для розблоковування
            modified_by: Ідентифікатор користувача, який вніс зміни
            
        Returns:
            bool: True якщо розблоковування успішне, False інакше
        """
        user = self.get_user(user_id)
        if not user:
            return False
        
        user['is_blocked'] = False
        user['last_modified_by'] = str(modified_by)
        user['last_modified_at'] = datetime.now(timezone.utc).isoformat()
        
        return self._save_users()
    
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
