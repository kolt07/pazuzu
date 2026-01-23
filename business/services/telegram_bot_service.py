# -*- coding: utf-8 -*-
"""
Сервіс для роботи з Telegram ботом.
"""

import asyncio
import os
import re
import zipfile
import tempfile
import yaml
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton, BotCommand
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
from business.services.logging_service import LoggingService
from business.services.llm_agent_service import LLMAgentService
from utils.file_utils import create_zip_archive


# Стани для ConversationHandler
WAITING_USER_ID, WAITING_NICKNAME, WAITING_CONFIRM_USER, WAITING_BLOCK_USER_ID, WAITING_CONFIRM_BLOCK, WAITING_LLM_QUERY = range(6)


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
        self.llm_agent_service = None  # Ініціалізується при першому використанні
        self.application = None
        self._running = False
        
        if not settings.telegram_bot_token:
            raise ValueError("Telegram bot token не вказано в налаштуваннях")
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обробка команди /start."""
        user_id = update.effective_user.id
        
        if not self.user_service.is_user_authorized(user_id):
            await update.message.reply_text("Ваш користувач не авторизований. Зареєструйтесь у адміністратора")
            return
        
        await self.show_main_menu(update, context)
    
    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Показує головне меню."""
        user_id = update.effective_user.id
        is_admin = self.user_service.is_admin(user_id)
        
        # Отримуємо дати оновлень з БД
        from data.repositories.app_data_repository import AppDataRepository
        app_data_repo = AppDataRepository()
        update_dates = app_data_repo.get_all_update_dates()
        
        day_period = ""
        if update_dates.get('1d'):
            update_date = update_dates['1d']
            day_period = f" (оновлено {update_date.strftime('%d.%m, %H:%M')})"
        
        week_period = ""
        if update_dates.get('7d'):
            update_date = update_dates['7d']
            week_period = f" (оновлено {update_date.strftime('%d.%m, %H:%M')})"
        
        keyboard = [
            [KeyboardButton(f"📥 Скачати файл за добу{day_period}")],
            [KeyboardButton(f"📥 Скачати файл за тиждень{week_period}")],
            [KeyboardButton("📊 Сформувати файл за добу")],
            [KeyboardButton("📊 Сформувати файл за тиждень")]
        ]
        
        if is_admin:
            keyboard.append([KeyboardButton("🤖 Спитати LLM")])
            keyboard.append([KeyboardButton("⚙️ Адміністрування")])
        
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        text = "Виберіть дію:"
        if update.callback_query:
            await update.callback_query.message.reply_text(text=text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text=text, reply_markup=reply_markup)
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обробка текстових повідомлень (кнопок з клавіатури)."""
        user_id = update.effective_user.id
        
        if not self.user_service.is_user_authorized(user_id):
            await update.message.reply_text("Ваш користувач не авторизований. Зареєструйтесь у адміністратора")
            return
        
        text = update.message.text
        
        if text.startswith("📥 Скачати файл за добу"):
            await self.handle_get_file(update, context, days=1)
        elif text.startswith("📥 Скачати файл за тиждень"):
            await self.handle_get_file(update, context, days=7)
        elif text == "📊 Сформувати файл за добу":
            await self.handle_generate_file(update, context, days=1)
        elif text == "📊 Сформувати файл за тиждень":
            await self.handle_generate_file_week_confirmation(update, context)
        elif text == "⚙️ Адміністрування":
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
        elif text == "✅ Так":
            # Підтвердження формування файлу за тиждень
            if context.user_data.get('pending_generate_week'):
                context.user_data.pop('pending_generate_week')
                await self.handle_generate_file(update, context, days=7)
                # Повертаємося на стартове меню
                await self.show_main_menu(update, context)
            else:
                await update.message.reply_text("Немає активного запиту на підтвердження.")
        elif text == "❌ Відміна":
            context.user_data.pop('pending_generate_week', None)
            await self.show_main_menu(update, context)
        else:
            await update.message.reply_text("Невідома команда. Використовуйте кнопки меню.")
    
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
                archive_internal_name = f"Звіт по нерухомості ({date_from.strftime('%d.%m.%Y')}-{update_date.strftime('%d.%m.%Y')}).xlsx"
                zip_filename = f"Звіт по нерухомості ({date_from.strftime('%d.%m.%Y')}-{update_date.strftime('%d.%m.%Y')}).zip"
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
        """Асинхронна функція для формування файлу."""
        try:
            loop = asyncio.get_event_loop()
            
            # Для тижня використовуємо оптимізовану паралельну обробку
            if days == 7:
                await self._send_progress_message(
                    chat_id,
                    "Почато оптимізовану обробку тижня: паралельна обробка по днях..."
                )
            else:
                # Отримуємо аукціони (синхронна операція, виконуємо в executor)
                auctions = await loop.run_in_executor(
                    None,
                    self.prozorro_service.get_real_estate_auctions,
                    days
                )
                
                if not auctions:
                    await self._send_progress_message(
                        chat_id,
                        "Аукціони не знайдено."
                    )
                    return
                
                # Аналізуємо аукціони перед збереженням для отримання статистики
                stats = await loop.run_in_executor(
                    None,
                    self.prozorro_service._analyze_auctions_before_save,
                    auctions
                )
                
                # Відправляємо повідомлення про статистику тільки якщо планується викликів LLM
                if stats['llm_planned'] > 0:
                    message = (
                        f"Знайдено {stats['total']} попередньо відібраних аукціонів.\n"
                        f"З них:\n"
                        f"• Без змін: {stats['unchanged']}\n"
                        f"• Змінено: {stats['changed']}\n"
                        f"• Планується викликів LLM: {stats['llm_planned']}"
                    )
                    
                    estimated_minutes = stats['llm_planned'] * 14 / 60
                    message += f"\n\nПриблизний час обробки: {estimated_minutes:.1f} хвилин"
                    
                    await self._send_progress_message(chat_id, message)
            
            # Зберігаємо файл (синхронна операція, виконуємо в executor)
            # Передаємо вже отримані аукціони, щоб уникнути повторного виклику API
            if days == 7:
                # Для тижня не передаємо аукціони, бо там використовується оптимізована обробка
                result = await loop.run_in_executor(
                    None,
                    lambda: self.prozorro_service.fetch_and_save_real_estate_auctions(
                        days=days,
                        user_id=user_id
                    )
                )
            else:
                # Для інших періодів передаємо вже отримані аукціони
                result = await loop.run_in_executor(
                    None,
                    lambda: self.prozorro_service.fetch_and_save_real_estate_auctions(
                        days=days,
                        user_id=user_id,
                        auctions=auctions
                    )
                )
            
            if days == 7 and result.get('success'):
                await self._send_progress_message(
                    chat_id,
                    f"Паралельна обробка завершена. Об'єдную файли..."
                )
            
            if result['success']:
                await self._send_progress_message(
                    chat_id,
                    f"Дані успішно оновлено. Знайдено {result.get('count', 0)} аукціонів."
                )
                
                # Логуємо успішне оновлення
                self.logging_service.log_user_action(
                    user_id=user_id,
                    action='generate_file',
                    message=f"Дані за {days} днів успішно оновлено",
                    metadata={
                        'days': days,
                        'count': result.get('count'),
                        'update_date': result.get('update_date').isoformat() if result.get('update_date') else None
                    }
                )
                
                # Генеруємо та відправляємо файл користувачу
                try:
                    excel_bytes = self.prozorro_service.generate_excel_from_db(days)
                    if excel_bytes:
                        update_date = result.get('update_date')
                        if update_date:
                            date_from = update_date - timedelta(days=days)
                            archive_internal_name = f"Звіт по нерухомості ({date_from.strftime('%d.%m.%Y')}-{update_date.strftime('%d.%m.%Y')}).xlsx"
                            zip_filename = f"Звіт по нерухомості ({date_from.strftime('%d.%m.%Y')}-{update_date.strftime('%d.%m.%Y')}).zip"
                        else:
                            archive_internal_name = f"Звіт по нерухомості ({days} днів).xlsx"
                            zip_filename = f"Звіт по нерухомості ({days} днів).zip"
                        
                        # Створюємо ZIP в пам'яті
                        import zipfile
                        from io import BytesIO
                        
                        zip_bytes = BytesIO()
                        with zipfile.ZipFile(zip_bytes, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            excel_bytes.seek(0)
                            zipf.writestr(archive_internal_name, excel_bytes.read())
                        
                        zip_bytes.seek(0)
                        
                        # Відправляємо файл
                        await self.application.bot.send_document(
                            chat_id=chat_id,
                            document=zip_bytes,
                            filename=zip_filename
                        )
                except Exception as e:
                    await self._send_progress_message(
                        chat_id,
                        f"Помилка при формуванні та відправці файлу: {e}"
                    )
            else:
                error_msg = result.get('message', 'Невідома помилка')
                await self._send_progress_message(
                    chat_id,
                    f"Помилка при формуванні файлу: {error_msg}"
                )
                self.logging_service.log_user_action(
                    user_id=user_id,
                    action='generate_file',
                    message=f"Помилка формування файлу за {days} днів",
                    metadata={'days': days},
                    error=error_msg
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
    
    def setup_handlers(self) -> None:
        """Налаштовує обробники команд та повідомлень."""
        # ConversationHandler для LLM запитів
        llm_conv_handler = ConversationHandler(
            entry_points=[
                MessageHandler(filters.Regex("^🤖 Спитати LLM$"), self.start_llm_query)
            ],
            states={
                WAITING_LLM_QUERY: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex("^🔙 Повернутись$"), self.handle_llm_query),
                    MessageHandler(filters.Regex("^🔙 Повернутись$"), self._back_to_main_menu)
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        
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
        self.application.add_handler(llm_conv_handler)
        self.application.add_handler(admin_conv_handler)
        self.application.add_handler(MessageHandler(filters.Document.ALL, self.handle_document))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
    
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
    
    async def start_llm_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Починає діалог з LLM."""
        user_id = update.effective_user.id
        
        if not self.user_service.is_admin(user_id):
            await update.message.reply_text("Ця функція доступна тільки для адміністраторів.")
            return ConversationHandler.END
        
        await update.message.reply_text(
            "🤖 Задайте питання LLM аналітику.\n\n"
            "Аналітик може:\n"
            "• Дослідити структуру бази даних\n"
            "• Виконати аналітичні запити\n"
            "• Згенерувати звіти\n"
            "• Відповісти на питання про дані\n\n"
            "Напишіть ваше питання або натисніть '🔙 Повернутись' для виходу.",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("🔙 Повернутись")]], resize_keyboard=True)
        )
        
        return WAITING_LLM_QUERY
    
    async def handle_llm_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обробляє запит до LLM."""
        user_id = update.effective_user.id
        user_query = update.message.text
        
        if not self.user_service.is_admin(user_id):
            await update.message.reply_text("Ця функція доступна тільки для адміністраторів.")
            return ConversationHandler.END
        
        # Ініціалізуємо LLM агента, якщо ще не ініціалізовано
        if self.llm_agent_service is None:
            try:
                self.llm_agent_service = LLMAgentService(self.settings)
            except Exception as e:
                await update.message.reply_text(
                    f"❌ Помилка ініціалізації LLM агента: {str(e)}\n"
                    "Перевірте налаштування LLM в конфігурації."
                )
                return ConversationHandler.END
        
        # Відправляємо повідомлення про початок обробки
        status_message = await update.message.reply_text("🤔 Обробляю запит...")
        
        # Функція для трансляції проміжних результатів
        async def stream_callback(text: str):
            """Транслює проміжні результати користувачу."""
            try:
                # Оновлюємо повідомлення з новим текстом
                current_text = status_message.text or ""
                new_text = current_text + text
                # Обмежуємо довжину повідомлення (Telegram має ліміт 4096 символів)
                if len(new_text) > 4000:
                    new_text = new_text[-4000:] + "\n\n... (текст обрізано)"
                await status_message.edit_text(new_text)
            except Exception:
                pass  # Ігноруємо помилки оновлення повідомлення
        
        try:
            # Створюємо список для зберігання проміжних повідомлень
            intermediate_messages = []
            last_update_time = [time.time()]  # Використовуємо список для nonlocal
            
            # Функція для збору проміжних повідомлень
            # Використовуємо threading.Event для синхронізації
            import threading
            import queue
            update_event = threading.Event()
            update_queue = queue.Queue()
            
            def collect_messages(text: str):
                intermediate_messages.append(text)
                current_time = time.time()
                # Оновлюємо повідомлення кожні 0.5 секунди або кожні 3 повідомлення
                if (current_time - last_update_time[0] > 0.5) or (len(intermediate_messages) % 3 == 0):
                    combined_text = "".join(intermediate_messages)
                    try:
                        update_queue.put_nowait(combined_text[-500:])
                        update_event.set()
                    except queue.Full:
                        pass  # Ігноруємо, якщо черга переповнена
                    last_update_time[0] = current_time
            
            # Створюємо завдання для оновлення повідомлень
            stop_updates = asyncio.Event()
            
            async def update_messages_task():
                """Завдання для оновлення повідомлень з черги."""
                while not stop_updates.is_set():
                    try:
                        # Перевіряємо чергу
                        try:
                            text = update_queue.get_nowait()
                            await stream_callback(text)
                        except queue.Empty:
                            pass
                        await asyncio.sleep(0.1)
                    except Exception:
                        break
            
            # Запускаємо завдання оновлення повідомлень
            update_task = asyncio.create_task(update_messages_task())
            
            # Обробляємо запит в окремому потоці, щоб не блокувати event loop
            import concurrent.futures
            
            def process_in_thread():
                try:
                    return self.llm_agent_service.process_query(
                        user_query,
                        stream_callback=collect_messages
                    )
                finally:
                    # Сигналізуємо про завершення обробки
                    stop_updates.set()
                    update_event.set()
            
            # Виконуємо в thread pool
            try:
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    response = await loop.run_in_executor(executor, process_in_thread)
            finally:
                # Зупиняємо завдання оновлення
                stop_updates.set()
                update_task.cancel()
                try:
                    await update_task
                except asyncio.CancelledError:
                    pass
            
            # Оновлюємо повідомлення з усіма проміжними результатами
            all_messages = "".join(intermediate_messages)
            if all_messages:
                await status_message.edit_text(all_messages[-4000:])
            
            # Відправляємо фінальну відповідь
            if len(response) > 4096:
                # Якщо відповідь занадто довга, розбиваємо на частини
                parts = [response[i:i+4000] for i in range(0, len(response), 4000)]
                for i, part in enumerate(parts):
                    if i == 0 and not all_messages:
                        await status_message.edit_text(part)
                    else:
                        await update.message.reply_text(part)
            else:
                if not all_messages:
                    await status_message.edit_text(response)
                else:
                    await update.message.reply_text(response)
            
            # Логуємо дію
            self.logging_service.log_user_action(
                user_id=user_id,
                action='llm_query',
                message=f"Запит до LLM: {user_query[:100]}...",
                metadata={'query_length': len(user_query), 'response_length': len(response)}
            )
        
        except Exception as e:
            error_msg = f"❌ Помилка обробки запиту: {str(e)}"
            await status_message.edit_text(error_msg)
            
            self.logging_service.log_user_action(
                user_id=user_id,
                action='llm_query',
                message=f"Помилка запиту до LLM: {user_query[:100]}...",
                error=str(e)
            )
        
        # Повертаємо до стану очікування наступного запиту
        await update.message.reply_text(
            "Задайте наступне питання або натисніть '🔙 Повернутись' для виходу.",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("🔙 Повернутись")]], resize_keyboard=True)
        )
        
        return WAITING_LLM_QUERY
    
    async def _back_to_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Повертає до головного меню."""
        context.user_data.clear()
        await self.show_main_menu(update, context)
        return ConversationHandler.END
    
    async def post_init(self, application: Application) -> None:
        """Викликається після ініціалізації бота."""
        await application.bot.set_my_commands([
            BotCommand("start", "Запустити бота")
        ])
    
    def run(self) -> None:
        """Запускає бота."""
        if self._running:
            return
        
        self.application = Application.builder().token(self.settings.telegram_bot_token).post_init(self.post_init).build()
        self.setup_handlers()
        
        self._running = True
        print("Telegram бот запущено")
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)
    
    def stop(self) -> None:
        """Зупиняє бота."""
        if not self._running:
            return
        
        self._running = False
        if self.application:
            self.application.stop()
        print("Telegram бот зупинено")
