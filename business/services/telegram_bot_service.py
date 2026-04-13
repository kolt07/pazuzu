# -*- coding: utf-8 -*-
"""
Сервіс для роботи з Telegram ботом.
"""

import asyncio
import concurrent.futures
import os
import re
import threading
import uuid
import zipfile
import tempfile
import yaml
import time
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton, BotCommand, MenuButtonWebApp, WebAppInfo
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    filters,
    ContextTypes
)

from config.settings import Settings
from business.services.prozorro_service import ProZorroService
from business.services.user_service import UserService
from business.services.source_data_load_service import run_full_pipeline
from business.services.logging_service import LoggingService
from business.services.multi_agent_service import MultiAgentService
from business.services.agent_test_runner_service import AgentTestRunnerService
from utils.file_utils import create_zip_archive
from utils.date_utils import format_datetime_display


# Стани для ConversationHandler (адмін-діалоги)
WAITING_USER_ID, WAITING_NICKNAME, WAITING_CONFIRM_USER, WAITING_BLOCK_USER_ID, WAITING_CONFIRM_BLOCK = range(5)


class TelegramBotService:
    """Сервіс для роботи з Telegram ботом."""
    
    def __init__(self, settings: Settings):
        """
        Ініціалізація сервісу.
        
        Args:
            settings: Налаштування застосунку
        """
        self.settings = settings
        self.user_service = UserService(settings.telegram_users_config_path)
        self.prozorro_service = ProZorroService(settings)
        self.logging_service = LoggingService()
        self.llm_agent_service = None  # Ініціалізується лише для адмін-тесту агента
        self.application = None
        self._running = False
        self._bot_loop = None  # event loop бота (для відправки з планувальника)

        if not settings.telegram_bot_token:
            raise ValueError("Telegram bot token не вказано в налаштуваннях")
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обробка команди /start."""
        user_id = update.effective_user.id
        
        if not self.user_service.is_user_authorized(user_id):
            await update.message.reply_text("Ваш користувач не авторизований. Зареєструйтесь у адміністратора")
            return
        
        await self.show_main_menu(update, context)

    async def app_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Команда відкриття та авторизації в Telegram Mini App."""
        user_id = update.effective_user.id
        if not self.user_service.is_user_authorized(user_id):
            await update.message.reply_text(
                "Ваш користувач не авторизований. Зареєструйтесь у адміністратора — після цього ви зможете відкрити застосунок."
            )
            return
        mini_app_url = (getattr(self.settings, "mini_app_base_url", None) or "").strip()
        if not mini_app_url:
            await update.message.reply_text(
                "Mini App поки не налаштовано (не вказано base_url)."
            )
            return
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Відкрити застосунок", web_app=WebAppInfo(url=mini_app_url))]
        ])
        await update.message.reply_text(
            "Ви авторизовані. Натисніть кнопку нижче, щоб відкрити застосунок у Telegram:",
            reply_markup=keyboard,
        )
    
    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Показує головне меню (кнопки швидких дій + адміністрування для адмінів)."""
        user_id = update.effective_user.id
        is_admin = self.user_service.is_admin(user_id)

        keyboard = []
        if is_admin:
            keyboard.append([KeyboardButton("⚙️ Адміністрування")])

        text = "Оберіть швидку дію нижче або відкрийте застосунок. Адміністратори можуть використати кнопку нижче."
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()
        inline_kbd = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📊 Звіт за день", callback_data="quick:report_last_day"),
                InlineKeyboardButton("📈 Звіт за тиждень", callback_data="quick:report_last_week"),
            ],
            [InlineKeyboardButton("📥 Експорт даних", callback_data="quick:export_data")],
        ])

        if update.callback_query:
            msg = await update.callback_query.message.reply_text(
                text=text, reply_markup=reply_markup,
            )
        else:
            msg = await update.message.reply_text(text=text, reply_markup=reply_markup)
        await msg.reply_text("Швидкі дії:", reply_markup=inline_kbd)
        context.user_data['last_menu_message_id'] = msg.message_id
        context.user_data['last_menu_chat_id'] = msg.chat.id
    
    async def _send_main_menu_to_chat(self, chat_id: int, user_id: int) -> None:
        """Надсилає головне меню в чат (лише адміністрування для адмінів)."""
        is_admin = self.user_service.is_admin(user_id)
        keyboard = []
        if is_admin:
            keyboard.append([KeyboardButton("⚙️ Адміністрування")])
        text = "Оберіть швидку дію або відкрийте застосунок."
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()
        sent = await self.application.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
        if user_id not in self.application.user_data:
            self.application.user_data[user_id] = {}
        self.application.user_data[user_id]['last_menu_message_id'] = sent.message_id
        self.application.user_data[user_id]['last_menu_chat_id'] = chat_id
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обробка текстових повідомлень (кнопок з клавіатури)."""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not self.user_service.is_user_authorized(user_id):
            await update.message.reply_text("Ваш користувач не авторизований. Зареєструйтесь у адміністратора")
            return

        # Видаляємо попереднє повідомлення «Виберіть дію» після вибору дії чи підтвердження
        mid = context.user_data.pop('last_menu_message_id', None)
        cid = context.user_data.pop('last_menu_chat_id', None)
        if mid is not None and cid == chat_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass

        text = update.message.text

        if text == "⚙️ Адміністрування":
            await self.show_admin_menu(update, context)
        elif text == "🔙 Повернутись":
            await self.show_main_menu(update, context)
        elif text == "➕ Додати користувача":
            await self.start_add_user(update, context)
        elif text == "➕ Додати адміністратора":
            await self.start_add_admin(update, context)
        elif text == "🚫 Заблокувати користувача":
            await self.start_block_user(update, context)
        elif text == "📥 Отримати файл налаштувань ProZorro":
            await self.handle_get_prozorro_config(update, context)
        elif text == "📤 Завантажити файл налаштувань ProZorro":
            await self.handle_upload_prozorro_config(update, context)
        elif text == "🔄 Оновити дані за добу":
            await self._run_data_update(update, context, days=1)
        elif text == "🔄 Оновити дані за тиждень":
            await self._run_data_update(update, context, days=7)
        elif text == "🧪 Тестування агента":
            await self._run_agent_test(update, context)
        elif text == "✅ Так":
            if context.user_data.get('pending_generate_week'):
                context.user_data.pop('pending_generate_week')
                await self.handle_generate_file(update, context, days=7)
                await self.show_main_menu(update, context)
            else:
                await update.message.reply_text("Немає активного запиту на підтвердження.")
        elif text == "❌ Відміна":
            context.user_data.pop('pending_generate_week', None)
            await self.show_main_menu(update, context)
        else:
            # Логуємо повідомлення, не відповідаємо (LLM-агент використовується лише в застосунку)
            self.logging_service.log_user_action(
                user_id=user_id,
                action="bot_message_received",
                message=f"Повідомлення в боті: {text[:200]}{'...' if len(text) > 200 else ''}",
                metadata={"text_length": len(text)},
            )

    async def handle_quick_action_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обробка натискання кнопок швидких дій (Звіт за день/тиждень, Експорт)."""
        if not update.callback_query:
            return
        user_id = update.effective_user.id
        if not self.user_service.is_user_authorized(user_id):
            await update.callback_query.answer("Не авторизовано.")
            return
        data = (update.callback_query.data or "").strip()
        if not data.startswith("quick:"):
            await update.callback_query.answer()
            return
        intent = data.replace("quick:", "")
        if intent not in ("report_last_day", "report_last_week", "export_data"):
            await update.callback_query.answer()
            return
        await update.callback_query.answer()
        if intent == "report_last_day":
            await self.handle_get_file(update, context, days=1)
        elif intent == "report_last_week":
            await self.handle_get_file(update, context, days=7)
        elif intent == "export_data":
            mini_app_url = (getattr(self.settings, "mini_app_base_url", None) or "").strip()
            msg = "Експорт даних доступний у застосунку. "
            if mini_app_url:
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Відкрити застосунок", web_app=WebAppInfo(url=mini_app_url))]
                ])
                await update.callback_query.message.reply_text(msg + "Натисніть кнопку нижче:", reply_markup=keyboard)
            else:
                await update.callback_query.message.reply_text(msg + "Відкрийте застосунок через команду /app.")
    
    async def start_add_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Початок діалогу додавання користувача."""
        message = update.message or (update.callback_query.message if update.callback_query else None)
        if not message:
            return ConversationHandler.END
        
        context.user_data['admin_role'] = 'user'
        keyboard = [[KeyboardButton("🔙 Повернутись")]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await message.reply_text(
            "Введіть ідентифікатор користувача Telegram (число):\n\n"
            "Або поділіться контактом",
            reply_markup=reply_markup
        )
        return WAITING_USER_ID
    
    async def start_add_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Початок діалогу додавання адміністратора."""
        message = update.message or (update.callback_query.message if update.callback_query else None)
        if not message:
            return ConversationHandler.END
        
        context.user_data['admin_role'] = 'admin'
        keyboard = [[KeyboardButton("🔙 Повернутись")]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await message.reply_text(
            "Введіть ідентифікатор користувача Telegram (число):\n\n"
            "Або поділіться контактом",
            reply_markup=reply_markup
        )
        return WAITING_USER_ID
    
    async def start_block_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Початок діалогу блокування користувача."""
        message = update.message or (update.callback_query.message if update.callback_query else None)
        if not message:
            return ConversationHandler.END
        
        keyboard = [[KeyboardButton("🔙 Повернутись")]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await message.reply_text(
            "Введіть ідентифікатор користувача для блокування:",
            reply_markup=reply_markup
        )
        return WAITING_BLOCK_USER_ID
    
    async def handle_get_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days: int) -> None:
        """Обробка отримання файлу з БД за період до збереженої дати оновлення."""
        message = update.message or (update.callback_query.message if update.callback_query else None)
        if not message:
            return
        
        user_id = update.effective_user.id
        
        await message.reply_text("Формую файл з даних БД...")
        
        try:
            # Генеруємо Excel в пам'яті з даних БД
            excel_bytes = self.prozorro_service.generate_excel_from_db(days)
            
            if not excel_bytes:
                await message.reply_text(f"Дані за {days} {'день' if days == 1 else 'днів'} не знайдено в БД.")
                self.logging_service.log_user_action(
                    user_id=user_id,
                    action='download_file',
                    message=f"Спроба скачати файл за {days} днів - дані не знайдено",
                    metadata={'days': days}
                )
                return
            
            # Отримуємо дату оновлення для формування назви файлу
            from data.repositories.app_data_repository import AppDataRepository
            app_data_repo = AppDataRepository()
            update_date = app_data_repo.get_update_date(days)
            
            if update_date:
                date_from = update_date - timedelta(days=days)
                archive_internal_name = f"Звіт по нерухомості ({format_datetime_display(date_from, '%d.%m.%Y')}-{format_datetime_display(update_date, '%d.%m.%Y')}).xlsx"
                zip_filename = f"Звіт по нерухомості ({format_datetime_display(date_from, '%d.%m.%Y')}-{format_datetime_display(update_date, '%d.%m.%Y')}).zip"
            else:
                archive_internal_name = f"Звіт по нерухомості ({days} днів).xlsx"
                zip_filename = f"Звіт по нерухомості ({days} днів).zip"
            
            # Створюємо тимчасовий ZIP файл в пам'яті
            import zipfile
            from io import BytesIO
            
            zip_bytes = BytesIO()
            with zipfile.ZipFile(zip_bytes, 'w', zipfile.ZIP_DEFLATED) as zipf:
                excel_bytes.seek(0)
                zipf.writestr(archive_internal_name, excel_bytes.read())
            
            zip_bytes.seek(0)
            
            # Відправляємо файл
            await context.bot.send_document(
                chat_id=message.chat_id,
                document=zip_bytes,
                filename=zip_filename
            )
            
            await message.reply_text("Файл успішно відправлено!")
            
            # Логуємо успішне скачування
            self.logging_service.log_user_action(
                user_id=user_id,
                action='download_file',
                message=f"Скачано файл за {days} днів з БД",
                metadata={'days': days, 'update_date': update_date.isoformat() if update_date else None}
            )
                
        except Exception as e:
            await message.reply_text(f"Помилка при формуванні та відправці файлу: {e}")
            self.logging_service.log_user_action(
                user_id=user_id,
                action='download_file',
                message=f"Помилка скачування файлу за {days} днів",
                metadata={'days': days},
                error=str(e)
            )
    
    async def handle_generate_file_week_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Показує підтвердження для формування файлу за тиждень."""
        message = update.message or (update.callback_query.message if update.callback_query else None)
        if not message:
            return
        
        context.user_data['pending_generate_week'] = True
        
        keyboard = [
            [KeyboardButton("✅ Так"), KeyboardButton("❌ Відміна")]
        ]
        
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await message.reply_text(
            "Формування файла за тиждень може зайняти декілька годин та витрачає ліміти використання ЛЛМ. Ви точно хочете запустити це формування?",
            reply_markup=reply_markup
        )
    
    async def handle_generate_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days: int) -> None:
        """Обробка формування файлу."""
        message = update.message or (update.callback_query.message if update.callback_query else None)
        if not message:
            return
        
        user_id = update.effective_user.id
        chat_id = message.chat_id
        
        await message.reply_text(f"Почато формування файлу за {days} {'день' if days == 1 else 'днів'}...")
        
        # Логуємо початок формування файлу
        self.logging_service.log_user_action(
            user_id=user_id,
            action='generate_file',
            message=f"Запит на формування файлу за {days} днів",
            metadata={'days': days}
        )
        
        # Запускаємо формування файлу асинхронно
        asyncio.create_task(self._generate_file_async(chat_id, user_id, days))
    
    async def _generate_file_async(self, chat_id: int, user_id: int, days: int) -> None:
        """Асинхронна функція для формування файлу. Оновлення даних через pipeline raw → LLM (Phase 1 без LLM)."""
        try:
            loop = asyncio.get_event_loop()
            await self._send_progress_message(chat_id, "Phase 1: завантаження сирих даних (без LLM)...")

            result = await loop.run_in_executor(
                None,
                lambda: run_full_pipeline(
                    settings=self.settings,
                    sources=["olx", "prozorro"],
                    days=days,
                )
            )
            p1 = result.get("phase1", {})
            p2 = result.get("phase2", {})
            await self._send_progress_message(
                chat_id,
                f"Дані оновлено. ProZorro: {p1.get('prozorro', {}).get('count', 0)} аукц., "
                f"OLX: {p1.get('olx', {}).get('total_listings', 0)} огол. LLM оброблено: ProZorro {p2.get('prozorro_llm_processed', 0)}, OLX {p2.get('olx_llm_processed', 0)}."
            )
            update_date = self.prozorro_service.app_data_repository.get_update_date(days)
            if not update_date:
                from datetime import datetime, timezone
                update_date = datetime.now(timezone.utc)
            if result.get("phase1"):
                self.logging_service.log_user_action(
                    user_id=user_id,
                    action='generate_file',
                    message=f"Дані за {days} днів успішно оновлено (pipeline)",
                    metadata={
                        'days': days,
                        'phase1': result.get("phase1"),
                        'phase2': result.get("phase2"),
                        'update_date': update_date.isoformat() if update_date else None
                    }
                )
            # Генеруємо та відправляємо файл користувачу
            try:
                excel_bytes = self.prozorro_service.generate_excel_from_db(days)
                if excel_bytes:
                    if update_date:
                        date_from = update_date - timedelta(days=days)
                        archive_internal_name = f"Звіт по нерухомості ({format_datetime_display(date_from, '%d.%m.%Y')}-{format_datetime_display(update_date, '%d.%m.%Y')}).xlsx"
                        zip_filename = f"Звіт по нерухомості ({format_datetime_display(date_from, '%d.%m.%Y')}-{format_datetime_display(update_date, '%d.%m.%Y')}).zip"
                    else:
                        archive_internal_name = f"Звіт по нерухомості ({days} днів).xlsx"
                        zip_filename = f"Звіт по нерухомості ({days} днів).zip"
                    import zipfile
                    from io import BytesIO
                    zip_bytes = BytesIO()
                    with zipfile.ZipFile(zip_bytes, 'w', zipfile.ZIP_DEFLATED) as zipf:
                        excel_bytes.seek(0)
                        zipf.writestr(archive_internal_name, excel_bytes.read())
                    zip_bytes.seek(0)
                    await self.application.bot.send_document(
                        chat_id=chat_id,
                        document=zip_bytes,
                        filename=zip_filename
                    )
                    await self._send_main_menu_to_chat(chat_id, user_id)
                else:
                    await self._send_progress_message(chat_id, "Немає даних для формування файлу.")
            except Exception as e:
                await self._send_progress_message(
                    chat_id,
                    f"Помилка при формуванні та відправці файлу: {e}"
                )
        except Exception as e:
            await self._send_progress_message(
                chat_id,
                f"Помилка: {e}"
            )
            self.logging_service.log_user_action(
                user_id=user_id,
                action='generate_file',
                message=f"Виняток при формуванні файлу за {days} днів",
                metadata={'days': days},
                error=str(e)
            )
    
    async def _send_progress_message(self, chat_id: int, text: str) -> None:
        """Відправляє повідомлення про прогрес."""
        try:
            if self.application and self.application.bot:
                await self.application.bot.send_message(chat_id=chat_id, text=text)
            else:
                print(f"Не вдалося відправити повідомлення: application не ініціалізовано")
        except Exception as e:
            print(f"Помилка відправки повідомлення: {e}")
    
    async def show_admin_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Показує меню адміністратора."""
        keyboard = [
            [KeyboardButton("➕ Додати користувача")],
            [KeyboardButton("➕ Додати адміністратора")],
            [KeyboardButton("🚫 Заблокувати користувача")],
            [KeyboardButton("🔄 Оновити дані за добу"), KeyboardButton("🔄 Оновити дані за тиждень")],
            [KeyboardButton("🧪 Тестування агента")],
            [KeyboardButton("📥 Отримати файл налаштувань ProZorro")],
            [KeyboardButton("📤 Завантажити файл налаштувань ProZorro")],
            [KeyboardButton("🔙 Повернутись")]
        ]
        
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        text = "Меню адміністратора:"
        message = update.message or (update.callback_query.message if update.callback_query else None)
        if message:
            await message.reply_text(text=text, reply_markup=reply_markup)
    
    async def handle_user_id_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обробка введення ідентифікатора користувача."""
        user_id_to_add = None
        
        # Перевіряємо, чи це контакт
        if update.message.contact:
            user_id_to_add = update.message.contact.user_id
        elif update.message.text:
            # Спробуємо розпарсити як число
            try:
                user_id_to_add = int(update.message.text.strip())
            except ValueError:
                keyboard = [[KeyboardButton("🔙 Повернутись")]]
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                await update.message.reply_text(
                    "Помилка: введіть коректний числовий ідентифікатор користувача.",
                    reply_markup=reply_markup
                )
                return WAITING_USER_ID
        else:
            keyboard = [[KeyboardButton("🔙 Повернутись")]]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(
                "Помилка: введіть ідентифікатор користувача або поділіться контактом.",
                reply_markup=reply_markup
            )
            return WAITING_USER_ID
        
        if user_id_to_add is None:
            keyboard = [[KeyboardButton("🔙 Повернутись")]]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(
                "Помилка: не вдалося отримати ідентифікатор користувача.",
                reply_markup=reply_markup
            )
            return WAITING_USER_ID
        
        context.user_data['pending_user_id'] = user_id_to_add
        role = context.user_data.get('admin_role', 'user')
        context.user_data['pending_role'] = role
        
        keyboard = [[KeyboardButton("🔙 Повернутись")]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(
            "Введіть псевдонім:",
            reply_markup=reply_markup
        )
        
        return WAITING_NICKNAME
    
    async def handle_nickname_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обробка введення псевдоніма."""
        nickname = update.message.text.strip()
        user_id_to_add = context.user_data.get('pending_user_id')
        role = context.user_data.get('pending_role', 'user')
        
        role_text = "користувача" if role == 'user' else "адміністратора"
        
        keyboard = [
            [KeyboardButton("✅ Так"), KeyboardButton("🔙 Повернутись")]
        ]
        
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            f"Додати {role_text} сервісу з псевдонімом {nickname}?",
            reply_markup=reply_markup
        )
        
        context.user_data['pending_nickname'] = nickname
        context.user_data['pending_confirm_add'] = True
        
        return WAITING_CONFIRM_USER
    
    async def handle_block_user_id_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обробка введення ідентифікатора користувача для блокування."""
        user_id_input = update.message.text.strip()
        
        try:
            user_id_to_block = int(user_id_input)
        except ValueError:
            keyboard = [[KeyboardButton("🔙 Повернутись")]]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(
                "Помилка: введіть коректний числовий ідентифікатор користувача.",
                reply_markup=reply_markup
            )
            return WAITING_BLOCK_USER_ID
        
        context.user_data['pending_block_user_id'] = user_id_to_block
        context.user_data['pending_confirm_block'] = True
        
        keyboard = [
            [KeyboardButton("✅ Так"), KeyboardButton("🔙 Повернутись")]
        ]
        
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            f"Заблокувати користувача з ID {user_id_to_block}?",
            reply_markup=reply_markup
        )
        
        return WAITING_CONFIRM_BLOCK
    
    def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Скасовує поточну операцію."""
        context.user_data.clear()
        return ConversationHandler.END
    
    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обробник помилок для всіх винятків у боті."""
        import logging
        from telegram.error import NetworkError, TimedOut, RetryAfter
        
        logger = logging.getLogger(__name__)
        
        error = context.error
        if error is None:
            return
        
        error_type = type(error).__name__
        error_msg = str(error)
        error_str_lower = error_msg.lower()
        
        # Мережеві помилки — лише одне коротке попередження, без повного traceback
        is_network_error = (
            isinstance(error, NetworkError) or
            isinstance(error, TimedOut) or
            "NetworkError" in error_type or
            "RemoteProtocolError" in error_type or
            "ReadError" in error_type or
            "httpx" in error_type or
            "connection" in error_str_lower or
            "disconnected" in error_str_lower or
            "timeout" in error_str_lower
        )
        
        if is_network_error:
            logger.warning(
                "Мережева помилка Telegram (retry автоматично): %s — %s",
                error_type,
                error_msg,
            )
            return
        
        # Обробляємо RetryAfter окремо
        if isinstance(error, RetryAfter):
            logger.warning(
                f"Rate limit досягнуто, очікування {error.retry_after} секунд: {error_msg}"
            )
            return
        
        # Для інших помилок — повний traceback і сповіщення користувача
        logger.error(
            "Помилка в Telegram боті: %s: %s",
            error_type,
            error_msg,
            exc_info=error,
        )
        try:
            if update and isinstance(update, Update):
                user_id = update.effective_user.id if update.effective_user else None
                chat_id = update.effective_chat.id if update.effective_chat else None
                
                self.logging_service.log_user_action(
                    user_id=user_id,
                    action='bot_error',
                    message=f"Помилка бота: {error_type}",
                    error=error_msg,
                    metadata={'chat_id': chat_id, 'error_type': error_type}
                )
                
                # Спробуємо відправити повідомлення користувачу про помилку (якщо це можливо)
                if chat_id and user_id:
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text="⚠️ Виникла помилка при обробці запиту. Спробуйте ще раз або зверніться до адміністратора."
                        )
                    except Exception:
                        # Якщо не вдалося відправити повідомлення, просто логуємо
                        pass
        except Exception as e:
            # Якщо виникла помилка при обробці помилки, просто логуємо
            logger.error(f"Помилка при обробці error handler: {e}", exc_info=e)

    def setup_handlers(self) -> None:
        """Налаштовує обробники команд та повідомлень."""
        # ConversationHandler для адміністраторських діалогів
        admin_conv_handler = ConversationHandler(
            entry_points=[
                MessageHandler(filters.Regex("^➕ Додати користувача$"), self.start_add_user),
                MessageHandler(filters.Regex("^➕ Додати адміністратора$"), self.start_add_admin),
                MessageHandler(filters.Regex("^🚫 Заблокувати користувача$"), self.start_block_user)
            ],
            states={
                WAITING_USER_ID: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex("^🔙 Повернутись$"), self.handle_user_id_input),
                    MessageHandler(filters.CONTACT, self.handle_user_id_input),
                    MessageHandler(filters.Regex("^🔙 Повернутись$"), self._back_to_admin_menu)
                ],
                WAITING_NICKNAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex("^🔙 Повернутись$"), self.handle_nickname_input),
                    MessageHandler(filters.Regex("^🔙 Повернутись$"), self._back_to_admin_menu)
                ],
                WAITING_CONFIRM_USER: [
                    MessageHandler(filters.Regex("^✅ Так$"), self._confirm_add_user),
                    MessageHandler(filters.Regex("^🔙 Повернутись$"), self._back_to_admin_menu)
                ],
                WAITING_BLOCK_USER_ID: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex("^🔙 Повернутись$"), self.handle_block_user_id_input),
                    MessageHandler(filters.Regex("^🔙 Повернутись$"), self._back_to_admin_menu)
                ],
                WAITING_CONFIRM_BLOCK: [
                    MessageHandler(filters.Regex("^✅ Так$"), self._confirm_block_user),
                    MessageHandler(filters.Regex("^🔙 Повернутись$"), self._back_to_admin_menu)
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        
        # Додаємо обробники (спочатку ConversationHandler, потім обробник документів, потім загальний MessageHandler)
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("app", self.app_command))
        self.application.add_handler(CallbackQueryHandler(self.handle_quick_action_callback))
        self.application.add_handler(admin_conv_handler)
        self.application.add_handler(MessageHandler(filters.Document.ALL, self.handle_document))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        # Error handler вже зареєстровано в методі run() перед setup_handlers()
        # Додаємо тут тільки якщо він ще не зареєстрований (для безпеки)
        if not hasattr(self.application, '_error_handlers') or not self.application._error_handlers:
            self.application.add_error_handler(self.error_handler)
    
    async def _back_to_admin_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Повертає до меню адміністратора."""
        context.user_data.clear()
        await self.show_admin_menu(update, context)
        return ConversationHandler.END
    
    async def _confirm_add_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Підтверджує додавання користувача."""
        user_id = update.effective_user.id
        user_id_to_add = context.user_data.get('pending_user_id')
        role = context.user_data.get('pending_role', 'user')
        nickname = context.user_data.get('pending_nickname', '')
        
        success = self.user_service.add_user(user_id_to_add, role, nickname, user_id)
        if success:
            await update.message.reply_text(
                f"Користувач {nickname} (ID: {user_id_to_add}) успішно додано як {role}."
            )
            self.logging_service.log_user_action(
                user_id=user_id,
                action='admin_action',
                message=f"Додано користувача {nickname} (ID: {user_id_to_add}) як {role}",
                metadata={'action': 'add_user', 'target_user_id': user_id_to_add, 'role': role}
            )
        else:
            await update.message.reply_text(
                "Помилка: не вдалося додати користувача. Можливо, він вже існує."
            )
            self.logging_service.log_user_action(
                user_id=user_id,
                action='admin_action',
                message=f"Спроба додати користувача {user_id_to_add} - помилка",
                metadata={'action': 'add_user', 'target_user_id': user_id_to_add},
                error='Користувач вже існує або помилка збереження'
            )
        context.user_data.clear()
        await self.show_admin_menu(update, context)
        return ConversationHandler.END
    
    async def _confirm_block_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Підтверджує блокування користувача."""
        user_id = update.effective_user.id
        user_id_to_block = context.user_data.get('pending_block_user_id')
        success = self.user_service.block_user(user_id_to_block, user_id)
        if success:
            await update.message.reply_text(f"Користувач (ID: {user_id_to_block}) успішно заблокований.")
            self.logging_service.log_user_action(
                user_id=user_id,
                action='admin_action',
                message=f"Заблоковано користувача (ID: {user_id_to_block})",
                metadata={'action': 'block_user', 'target_user_id': user_id_to_block}
            )
        else:
            await update.message.reply_text("Помилка: не вдалося заблокувати користувача.")
            self.logging_service.log_user_action(
                user_id=user_id,
                action='admin_action',
                message=f"Спроба заблокувати користувача {user_id_to_block} - помилка",
                metadata={'action': 'block_user', 'target_user_id': user_id_to_block},
                error='Користувач не знайдено або помилка збереження'
            )
        context.user_data.clear()
        await self.show_admin_menu(update, context)
        return ConversationHandler.END
    
    async def handle_get_prozorro_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Відправляє файл конфігурації ProZorro користувачу."""
        try:
            config = self.prozorro_service.get_classification_codes_config()
            
            # Створюємо тимчасовий файл
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
                temp_path = f.name
            
            try:
                with open(temp_path, 'rb') as f:
                    await update.message.reply_document(
                        document=f,
                        filename='ProZorro_clasification_codes.yaml',
                        caption='Файл налаштувань кодів класифікації ProZorro'
                    )
            finally:
                # Видаляємо тимчасовий файл
                if os.path.exists(temp_path):
                    os.remove(temp_path)
        except Exception as e:
            await update.message.reply_text(f"Помилка при отриманні файлу конфігурації: {e}")
    
    async def handle_upload_prozorro_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обробляє завантаження файлу конфігурації ProZorro."""
        await update.message.reply_text(
            "Будь ласка, надішліть файл налаштувань ProZorro (YAML формат).\n"
            "Файл має містити структуру:\n"
            "classification_codes:\n"
            "  - code: '0612'\n"
            "    description: 'Опис коду'\n"
            "  - code: '0613'\n"
            "    description: 'Опис коду'\n"
            "..."
        )
        context.user_data['waiting_for_prozorro_config'] = True

    def _run_data_update_sync(self, days: int) -> Dict[str, Any]:
        """Синхронне оновлення даних через pipeline raw → main → LLM (Phase 1 без LLM). Для виклику з фонового потоку."""
        result = run_full_pipeline(
            settings=self.settings,
            sources=["olx", "prozorro"],
            days=days,
        )
        return {"pipeline_result": result}

    async def _run_data_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days: int) -> None:
        """Запускає оновлення даних за добу (days=1) або за тиждень (days=7) у фоновому потоці та надсилає підсумок після завершення."""
        period_text = "за добу" if days == 1 else "за тиждень"
        chat_id = update.effective_chat.id
        loop = asyncio.get_event_loop()
        bot = context.bot

        await update.message.reply_text(f"Запущено оновлення даних {period_text}. Повідомлення прийде після завершення.")

        def work() -> None:
            try:
                res = self._run_data_update_sync(days)
                r = res.get("pipeline_result", {})
                p1 = r.get("phase1", {})
                p2 = r.get("phase2", {})
                olx_p1 = p1.get("olx", {})
                prozorro_p1 = p1.get("prozorro", {})
                o_ok = f"✓ raw: {olx_p1.get('total_listings', 0)} огол., LLM: {p2.get('olx_llm_processed', 0)}"
                p_ok = f"✓ raw: {prozorro_p1.get('count', 0)} аукц., LLM: {p2.get('prozorro_llm_processed', 0)}"
                summary = f"Оновлення даних {period_text} завершено (Phase 1 — без LLM).\nProZorro: {p_ok}\nOLX: {o_ok}"
            except Exception as e:
                summary = f"Оновлення даних {period_text}: помилка — {e!s}"
            fut = asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=chat_id, text=summary), loop)
            try:
                fut.result(timeout=10)
            except Exception:
                pass

        threading.Thread(target=work, daemon=True, name="DataUpdate").start()

    async def _run_agent_test(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Запускає тестування LLM-помічника тест-агентом (генерація кейсів, прогон, перевірка по БД) та надсилає звіт."""
        message = update.message or (update.callback_query.message if update.callback_query else None)
        if not message:
            return
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id

        if self.llm_agent_service is None:
            try:
                loop = asyncio.get_running_loop()
                def notify_admins(nmsg: str, uid: Optional[str], det: Optional[str]) -> None:
                    asyncio.run_coroutine_threadsafe(
                        self._notify_admins_async(nmsg, uid, det),
                        loop,
                    )
                self.llm_agent_service = MultiAgentService(
                    self.settings,
                    user_service=self.user_service,
                    notify_admins_fn=notify_admins,
                )
            except Exception as e:
                await message.reply_text(f"❌ Помилка ініціалізації LLM агента: {e}")
                await self.show_admin_menu(update, context)
                return

        await message.reply_text("🧪 Запускаю тестування агента (генерація кейсів, прогон, перевірка по БД). Це може зайняти кілька хвилин…")

        def run_test_sync() -> Dict[str, Any]:
            runner = AgentTestRunnerService(self.settings)
            return runner.run_all(self.llm_agent_service, generate_with_llm=True)

        import concurrent.futures
        loop = asyncio.get_event_loop()
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                report = await loop.run_in_executor(executor, run_test_sync)
        except Exception as e:
            await message.reply_text(f"❌ Помилка тестування: {e}")
            await self.show_admin_menu(update, context)
            return

        full_text = report.get("full_report_text", "")
        total_passed = report.get("total_passed", 0)
        total_failed = report.get("total_failed", 0)
        chunk_size = 4000
        for i in range(0, len(full_text), chunk_size):
            chunk = full_text[i : i + chunk_size]
            try:
                await message.reply_text(chunk)
            except Exception as e:
                await message.reply_text(f"Не вдалося надіслати частину звіту: {e}")
                break
        await message.reply_text(f"Підсумок: пройдено {total_passed}, не пройдено {total_failed}.")
        self.logging_service.log_user_action(
            user_id=user_id,
            action="admin_action",
            message="Запуск тестування агента",
            metadata={"total_passed": total_passed, "total_failed": total_failed},
        )
        await self.show_admin_menu(update, context)

    async def handle_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обробляє завантажені документи."""
        if not context.user_data.get('waiting_for_prozorro_config'):
            return
        
        document = update.message.document
        if not document:
            await update.message.reply_text("Помилка: файл не знайдено.")
            context.user_data.pop('waiting_for_prozorro_config', None)
            await self.show_admin_menu(update, context)
            return
        
        # Перевіряємо, чи це YAML файл
        file_name = document.file_name or ''
        if not (file_name.endswith('.yaml') or file_name.endswith('.yml')):
            await update.message.reply_text(
                "Помилка: файл має бути у форматі YAML (.yaml або .yml)"
            )
            context.user_data.pop('waiting_for_prozorro_config', None)
            await self.show_admin_menu(update, context)
            return
        
        try:
            # Завантажуємо файл
            file = await context.bot.get_file(document.file_id)
            
            # Створюємо тимчасовий файл
            with tempfile.NamedTemporaryFile(mode='w+b', suffix='.yaml', delete=False) as temp_file:
                await file.download_to_drive(temp_file.name)
                temp_path = temp_file.name
            
            try:
                # Читаємо та валідуємо файл
                with open(temp_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                
                # Валідація
                if not isinstance(config, dict):
                    raise ValueError("Файл має містити словник (dictionary)")
                
                if 'classification_codes' not in config:
                    raise ValueError("Файл має містити ключ 'classification_codes'")
                
                if not isinstance(config['classification_codes'], list):
                    raise ValueError("'classification_codes' має бути списком")
                
                # Валідація кожного елемента
                for idx, item in enumerate(config['classification_codes']):
                    if not isinstance(item, dict):
                        raise ValueError(f"Елемент {idx + 1} має бути словником")
                    if 'code' not in item:
                        raise ValueError(f"Елемент {idx + 1} має містити поле 'code'")
                    if not isinstance(item['code'], str) or not item['code'].strip():
                        raise ValueError(f"Елемент {idx + 1}: поле 'code' має бути непустим рядком")
                
                # Зберігаємо конфігурацію
                success = self.prozorro_service.save_classification_codes_config(config)
                
                if success:
                    codes_count = len(config['classification_codes'])
                    await update.message.reply_text(
                        f"✅ Файл конфігурації успішно завантажено!\n"
                        f"Додано {codes_count} кодів класифікації."
                    )
                else:
                    await update.message.reply_text(
                        "❌ Помилка: не вдалося зберегти файл конфігурації."
                    )
            finally:
                # Видаляємо тимчасовий файл
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            
            context.user_data.pop('waiting_for_prozorro_config', None)
            await self.show_admin_menu(update, context)
            
        except yaml.YAMLError as e:
            await update.message.reply_text(f"Помилка парсингу YAML файлу: {e}")
            context.user_data.pop('waiting_for_prozorro_config', None)
            await self.show_admin_menu(update, context)
        except ValueError as e:
            await update.message.reply_text(f"Помилка валідації файлу: {e}")
            context.user_data.pop('waiting_for_prozorro_config', None)
            await self.show_admin_menu(update, context)
        except Exception as e:
            await update.message.reply_text(f"Помилка при обробці файлу: {e}")
            context.user_data.pop('waiting_for_prozorro_config', None)
            await self.show_admin_menu(update, context)
    
    async def _notify_admins_async(
        self,
        message: str,
        offending_user_id: Optional[str],
        details: Optional[str],
    ) -> None:
        """Надсилає повідомлення усім адміністраторам (наприклад про спрацьовування агента безпеки)."""
        if not self.application or not self.application.bot:
            return
        admin_ids = self.user_service.get_admin_user_ids()
        body = f"⚠️ Агент безпеки: {message}"
        if offending_user_id:
            body += f"\nКористувач (TG id): {offending_user_id}"
        if details:
            body += f"\nДеталі: {details[:500]}"
        for chat_id in admin_ids:
            try:
                await self.application.bot.send_message(chat_id=chat_id, text=body)
            except Exception as e:
                # Логуємо, але не падаємо
                pass

    async def _back_to_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Повертає до головного меню."""
        context.user_data.clear()
        await self.show_main_menu(update, context)
        return ConversationHandler.END
    
    async def post_init(self, application: Application) -> None:
        """Викликається після ініціалізації бота."""
        self._bot_loop = asyncio.get_running_loop()
        await application.bot.set_my_commands([
            BotCommand("start", "Запустити бота"),
            BotCommand("app", "Відкрити застосунок"),
        ])
        mini_app_url = (getattr(self.settings, "mini_app_base_url", None) or "").strip()
        if mini_app_url and mini_app_url.lower().startswith("https://"):
            try:
                await application.bot.set_chat_menu_button(
                    menu_button=MenuButtonWebApp(
                        text="Відкрити застосунок",
                        web_app=WebAppInfo(url=mini_app_url),
                    )
                )
            except Exception as e:
                print(f"Попередження: не вдалося встановити кнопку Mini App: {e}")
        elif mini_app_url:
            print("Попередження: Mini App base_url має бути HTTPS (наприклад ngrok або ваш домен). Кнопка меню не встановлена.")
    
    def run(self) -> None:
        """Запускає бота."""
        if self._running:
            return
        
        # Налаштовуємо Application з покращеною обробкою помилок
        builder = Application.builder().token(self.settings.telegram_bot_token).post_init(self.post_init)
        
        # Налаштовуємо HTTPXRequest для кращої обробки мережевих помилок
        # (API python-telegram-bot v20+ не приймає готовий http_client, лише параметри/kwargs)
        try:
            import httpx
            from telegram.request import HTTPXRequest
            
            # Передаємо налаштування через параметри HTTPXRequest / httpx_kwargs
            request = HTTPXRequest(
                read_timeout=30.0,
                connect_timeout=10.0,
                http_version="2",
                httpx_kwargs={
                    "limits": httpx.Limits(
                        max_keepalive_connections=5,
                        max_connections=10,
                    ),
                    "http2": True,
                },
            )
            builder = builder.request(request)
        except (ImportError, AttributeError) as e:
            # Якщо HTTPXRequest недоступний (старі версії бібліотеки), використовуємо стандартні налаштування
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Не вдалося налаштувати HTTPXRequest, використовуємо стандартні налаштування: {e}")
        
        self.application = builder.build()
        
        # Реєструємо error handler ПЕРЕД setup_handlers, щоб він точно був зареєстрований
        self.application.add_error_handler(self.error_handler)
        
        self.setup_handlers()
        
        self._running = True
        print("Telegram бот запущено")
        
        # Запускаємо polling з покращеною обробкою помилок
        try:
            self.application.run_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=False,  # Не скидаємо очікуючі оновлення
                close_loop=False,  # Не закриваємо event loop при помилках
                stop_signals=None,  # У контейнері бот запускається з non-main thread
            )
        except KeyboardInterrupt:
            print("\nОтримано сигнал переривання, зупиняємо бота...")
            self.stop()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Критична помилка при роботі бота: {e}", exc_info=e)
            raise
    
    def send_message_to_chat_sync(self, chat_id: int, text: str, timeout: float = 15) -> None:
        """
        Відправляє текстове повідомлення в чат з контексту іншого потоку (наприклад планувальника).
        Викликає send_message у потоку бота через run_coroutine_threadsafe.
        """
        if not self.application or not self.application.bot or not self._bot_loop:
            return
        try:
            fut = asyncio.run_coroutine_threadsafe(
                self.application.bot.send_message(chat_id=chat_id, text=text),
                self._bot_loop,
            )
            fut.result(timeout=timeout)
        except Exception as e:
            print(f"Помилка відправки повідомлення планувальника: {e}")

    def send_document_to_chat_sync(
        self,
        chat_id: int,
        file_path: str,
        filename: str,
        caption: Optional[str] = None,
        timeout: float = 30,
    ) -> None:
        """
        Відправляє файл у чат з контексту іншого потоку (наприклад планувальника).
        """
        if not self.application or not self.application.bot or not self._bot_loop:
            return
        try:
            file_bytes = Path(file_path).read_bytes()
            fut = asyncio.run_coroutine_threadsafe(
                self.application.bot.send_document(
                    chat_id=chat_id,
                    document=BytesIO(file_bytes),
                    filename=filename,
                    caption=caption,
                ),
                self._bot_loop,
            )
            fut.result(timeout=timeout)
        except Exception as e:
            print(f"Помилка відправки файлу планувальника: {e}")

    def stop(self) -> None:
        """Зупиняє бота."""
        if not self._running:
            return
        
        self._running = False
        if self.application:
            self.application.stop()
        print("Telegram бот зупинено")
