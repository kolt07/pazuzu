# -*- coding: utf-8 -*-
"""
Сервіс артефактів: реєстрація згенерованих файлів (Excel, звіти) з TTL та власником.
Клієнт отримує artifact_id і може забрати файл через get_artifact; TTL видаляє старі.
"""

import uuid
import logging
from typing import Dict, Any, Optional, List

from data.repositories.artifact_repository import ArtifactRepository

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 3600
CHAT_FILE_TTL_SECONDS = 10 * 24 * 3600  # 10 днів


class ArtifactService:
    def __init__(self):
        self._repo: Optional[ArtifactRepository] = None

    @property
    def repo(self) -> ArtifactRepository:
        if self._repo is None:
            self._repo = ArtifactRepository()
        return self._repo

    def register(
        self,
        user_id: Optional[str],
        artifact_type: str,
        content_base64: str,
        metadata: Optional[Dict[str, Any]] = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        artifact_id: Optional[str] = None,
        with_download_token: bool = False,
    ) -> str:
        aid = artifact_id or str(uuid.uuid4())
        download_token = str(uuid.uuid4()) if with_download_token else None
        self.repo.create(
            artifact_id=aid,
            user_id=user_id,
            artifact_type=artifact_type,
            content_base64=content_base64,
            metadata=metadata,
            ttl_seconds=ttl_seconds,
            download_token=download_token,
        )
        logger.info("Artifact registered: %s type=%s user_id=%s", aid, artifact_type, user_id)
        return aid

    def register_with_token(
        self,
        user_id: Optional[str],
        artifact_type: str,
        content_base64: str,
        metadata: Optional[Dict[str, Any]] = None,
        ttl_seconds: int = CHAT_FILE_TTL_SECONDS,
    ) -> Dict[str, Any]:
        """
        Реєструє артефакт з токеном для завантаження (для мобільних посилань).
        Повертає {artifact_id, download_token} для формування URL.
        """
        aid = str(uuid.uuid4())
        download_token = str(uuid.uuid4())
        self.repo.create(
            artifact_id=aid,
            user_id=user_id,
            artifact_type=artifact_type,
            content_base64=content_base64,
            metadata=metadata or {},
            ttl_seconds=ttl_seconds,
            download_token=download_token,
        )
        logger.info("Artifact registered: %s type=%s user_id=%s", aid, artifact_type, user_id)
        return {"artifact_id": aid, "download_token": download_token}

    def get_artifact(self, artifact_id: str) -> Optional[Dict[str, Any]]:
        doc = self.repo.get_by_artifact_id(artifact_id)
        if not doc:
            return None
        from datetime import datetime, timezone
        expires_at = doc.get("expires_at")
        if expires_at:
            now = datetime.now(timezone.utc)
            # PyMongo повертає naive datetime (UTC); нормалізуємо для порівняння
            if isinstance(expires_at, str):
                expires_at = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            elif expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at < now:
                return None
        return doc

    def delete_expired(self) -> int:
        return self.repo.delete_expired()

    def delete_by_ids(self, artifact_ids: List[str], user_id: Optional[str] = None) -> int:
        """Видаляє артефакти за списком ID. Якщо user_id вказано — лише артефакти цього користувача."""
        if not artifact_ids:
            return 0
        return self.repo.delete_by_ids(artifact_ids, user_id=user_id)
