import os
import logging
from dotenv import load_dotenv
import asyncio
import logging
import uuid
from datetime import datetime, date
from typing import Dict, List, Optional
import pytz
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton
)
from aiogram.filters import Command, CommandObject
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
import sqlite3
import json

load_dotenv()

API_TOKEN = os.getenv('BOT_TOKEN')
TIMEZONE = os.getenv('TIMEZONE')
DB_FILE = os.getenv('DB_FILE')
LOG_LEVEL = os.getenv('LOG_LEVEL')
LOG_FILE = os.getenv('LOG_FILE')
ADMIN_IDS_STR = os.getenv('ADMIN_IDS')

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Токен бота
API_TOKEN = os.getenv('BOT_TOKEN')
if not API_TOKEN:
    raise ValueError("BOT_TOKEN не найден! Укажите его в .env файле")

# Настройки
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Часовой пояс
tz = pytz.timezone(TIMEZONE)

# Состояния для FSM
class CountdownState(StatesGroup):
    waiting_for_event_name = State()
    waiting_for_target_date = State()
    waiting_for_time = State()
    waiting_for_delete_confirmation = State()

# ========== БАЗА ДАННЫХ ==========

def init_db():
    """Инициализация базы данных SQLite"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Таблица событий
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        chat_id INTEGER,
        user_id INTEGER,
        event_name TEXT NOT NULL,
        target_date TEXT NOT NULL,
        notification_time TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at TEXT,
        chat_type TEXT,
        message_thread_id INTEGER DEFAULT 0
    )
    ''')
    
    # Таблица для отслеживания отправленных уведомлений
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sent_notifications (
        event_id TEXT,
        notification_date TEXT,
        PRIMARY KEY (event_id, notification_date),
        FOREIGN KEY (event_id) REFERENCES events (id)
    )
    ''')
    
    conn.commit()
    conn.close()

init_db()

def save_event(event_data: dict) -> str:
    """Сохранить событие в БД"""
    event_id = str(uuid.uuid4())
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO events 
    (id, chat_id, user_id, event_name, target_date, notification_time, 
     is_active, created_at, chat_type, message_thread_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        event_id,
        event_data['chat_id'],
        event_data['user_id'],
        event_data['event_name'],
        event_data['target_date'].isoformat(),
        event_data['notification_time'],
        1,
        datetime.now().isoformat(),
        event_data.get('chat_type', 'private'),
        event_data.get('message_thread_id', 0)
    ))
    
    conn.commit()
    conn.close()
    return event_id

def get_chat_events(chat_id: int) -> List[dict]:
    """Получить все события для чата"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT id, user_id, event_name, target_date, notification_time, is_active
    FROM events 
    WHERE chat_id = ? AND is_active = 1
    ORDER BY target_date
    ''', (chat_id,))
    
    events = []
    for row in cursor.fetchall():
        events.append({
            'id': row[0],
            'user_id': row[1],
            'event_name': row[2],
            'target_date': datetime.strptime(row[3], '%Y-%m-%d').date(),
            'notification_time': row[4],
            'is_active': bool(row[5])
        })
    
    conn.close()
    return events

def get_user_events_in_chat(chat_id: int, user_id: int) -> List[dict]:
    """Получить события пользователя в конкретном чате"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT id, event_name, target_date, notification_time
    FROM events 
    WHERE chat_id = ? AND user_id = ? AND is_active = 1
    ORDER BY target_date
    ''', (chat_id, user_id))
    
    events = []
    for row in cursor.fetchall():
        events.append({
            'id': row[0],
            'event_name': row[1],
            'target_date': datetime.strptime(row[2], '%Y-%m-%d').date(),
            'notification_time': row[3]
        })
    
    conn.close()
    return events

def delete_event(event_id: str, user_id: int = None):
    """Удалить событие"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    if user_id:
        # Удаляем только если пользователь создавал
        cursor.execute('''
        DELETE FROM events 
        WHERE id = ? AND user_id = ?
        ''', (event_id, user_id))
    else:
        # Удаляем без проверки пользователя (для админов)
        cursor.execute('DELETE FROM events WHERE id = ?', (event_id,))
    
    # Удаляем связанные уведомления
    cursor.execute('DELETE FROM sent_notifications WHERE event_id = ?', (event_id,))
    
    conn.commit()
    conn.close()

def deactivate_event(event_id: str):
    """Деактивировать событие"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
    UPDATE events 
    SET is_active = 0 
    WHERE id = ?
    ''', (event_id,))
    
    conn.commit()
    conn.close()

def get_all_active_events():
    """Получить все активные события"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT id, chat_id, event_name, target_date, notification_time, 
           user_id, chat_type, message_thread_id
    FROM events 
    WHERE is_active = 1
    ''')
    
    events = []
    for row in cursor.fetchall():
        events.append({
            'id': row[0],
            'chat_id': row[1],
            'event_name': row[2],
            'target_date': datetime.strptime(row[3], '%Y-%m-%d').date(),
            'notification_time': row[4],
            'user_id': row[5],
            'chat_type': row[6],
            'message_thread_id': row[7]
        })
    
    conn.close()
    return events

def mark_notification_sent(event_id: str, notification_date: date):
    """Отметить, что уведомление было отправлено"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT OR IGNORE INTO sent_notifications (event_id, notification_date)
    VALUES (?, ?)
    ''', (event_id, notification_date.isoformat()))
    
    conn.commit()
    conn.close()

def was_notification_sent_today(event_id: str) -> bool:
    """Проверить, отправлялось ли уведомление сегодня"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    today = datetime.now(tz).date().isoformat()
    cursor.execute('''
    SELECT 1 FROM sent_notifications 
    WHERE event_id = ? AND notification_date = ?
    ''', (event_id, today))
    
    result = cursor.fetchone() is not None
    conn.close()
    return result

# ========== УТИЛИТЫ ==========

def days_until_target(target_date: date, current_date: Optional[date] = None) -> int:
    """Рассчитать количество дней до даты"""
    if current_date is None:
        current_date = datetime.now(tz).date()
    return (target_date - current_date).days

def format_countdown_message(event_name: str, days_left: int, target_date: date) -> str:
    """Форматировать сообщение с отсчётом"""
    if days_left > 0:
        if days_left == 1:
            day_word = "день"
        elif 2 <= days_left <= 4:
            day_word = "дня"
        else:
            day_word = "дней"
        
        message = f"**{event_name}**\n"
        message += f"До события осталось: **{days_left} {day_word}**\n"
        message += f"Дата: {target_date.strftime('%d.%m.%Y')}"
        
        # Дополнительная информация
        if days_left <= 7:
            if days_left == 1:
                message += f"\n\n Это всего **1 день**!"
            elif 2 <= days_left <= 4:
                message += f"\n\n Это всего **{days_left} дня**!"
            else:
                message += f"\n\n Это всего **{days_left} дней**!"
        elif days_left <= 30:
            weeks = days_left // 7
            if weeks == 1:
                week_word = "неделя"
            elif 2 <= weeks <= 4:
                week_word = "недели"
            else:
                week_word = "недель"
            message += f"\n\n Примерно **{weeks} {week_word}**"
        
        return message
    elif days_left == 0:
        return f" **{event_name}**\n\n**СЕГОДНЯ ДЕНЬ СОБЫТИЯ!** \n{target_date.strftime('%d.%m.%Y')}"
    else:
        # Для прошедших событий
        past_days = abs(days_left)
        if past_days == 1:
            day_word = "день"
        elif 2 <= past_days <= 4:
            day_word = "дня"
        else:
            day_word = "дней"
        return f" **{event_name}**\nСобытие прошло **{past_days} {day_word}** назад\n{target_date.strftime('%d.%m.%Y')}"

def format_events_list(events: List[dict]) -> str:
    """Форматировать список событий"""
    if not events:
        return " Нет активных отсчётов"
    
    today = datetime.now(tz).date()
    message = "**Активные отсчёты:**\n\n"
    
    for i, event in enumerate(events, 1):
        days_left = days_until_target(event['target_date'], today)
        
        if days_left == 1:
            day_word = "день"
        elif 2 <= days_left <= 4:
            day_word = "дня"
        else:
            day_word = "дней"
        
        message += f"{i}. **{event['event_name']}**\n"
        message += f"{event['target_date'].strftime('%d.%m.%Y')}\n"
        message += f"Уведомления в {event['notification_time']}\n"
        message += f"Осталось: {days_left} {day_word}\n"
        
        # Индикатор прогресса
        if days_left > 0 and days_left <= 30:
            progress = '⬜' * max(1, (30 - days_left) // 3) + '⬛' * (days_left // 3)
            message += f"   {progress}\n"
        
        message += f"   ID: `{event['id'][:8]}...`\n\n"
    
    return message

# ========== КОМАНДЫ ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Команда /start"""
    welcome_text = (
        "**Бот для обратного отсчёта**\n\n"
        "Я могу вести обратный отсчёт до важных событий и ежедневно отправлять уведомления.\n\n"
        "**Работает в:**\n"
        "• Личных сообщениях\n"
        "• Групповых чатах\n"
        "• Супергруппах\n"
        "• Каналах (только админ)\n\n"
        "**Основные команды:**\n"
        "• /new - создать новый отсчёт\n"
        "• /list - показать все отсчёты в чате\n"
        "• /my - мои отсчёты в этом чате\n"
        "• /delete - удалить отсчёт\n"
        "• /help - справка\n\n"
        "**Пример:**\n"
        "Создайте отсчёт до дня рождения, отпуска, дедлайна и я буду каждый день напоминать сколько дней осталось!"
    )
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Создать отсчёт"), KeyboardButton(text="📋 Все отсчёты")],
            [KeyboardButton(text="👤 Мои отсчёты"), KeyboardButton(text="❌ Удалить отсчёт")],
            [KeyboardButton(text="ℹ️ Помощь")]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )
    
    await message.answer(welcome_text, reply_markup=keyboard, parse_mode="Markdown")

@dp.message(Command("new"))
@dp.message(F.text == "➕ Создать отсчёт")
async def cmd_new(message: types.Message, state: FSMContext):
    """Создание нового отсчёта"""
    await message.answer(
        "**Создание нового отсчёта**\n\n"
        "Введите название события:\n"
        "Например: Выпить пива в пятницу",
        parse_mode="Markdown"
    )
    await state.set_state(CountdownState.waiting_for_event_name)

@dp.message(CountdownState.waiting_for_event_name)
async def process_event_name(message: types.Message, state: FSMContext):
    """Обработка названия события"""
    if len(message.text) > 100:
        await message.answer("Название слишком длинное. Максимум 100 символов. Введите снова:")
        return
    
    await state.update_data(event_name=message.text)
    
    # Запрашиваем дату
    await message.answer(
        "**Введите дату события**\n\n"
        "Формат: *ДД.ММ.ГГГГ*\n"
        "Пример: *25.12.2024*\n\n"
        "Минимальная дата: завтра\n"
        "Максимальная: 5 лет вперед",
        parse_mode="Markdown"
    )
    await state.set_state(CountdownState.waiting_for_target_date)

@dp.message(CountdownState.waiting_for_target_date)
async def process_target_date(message: types.Message, state: FSMContext):
    """Обработка даты события"""
    try:
        # Парсим дату
        target_date = datetime.strptime(message.text, "%d.%m.%Y").date()
        today = datetime.now(tz).date()
        
        # Проверки
        if target_date <= today:
            await message.answer("Дата должна быть в будущем! Введите другую дату:")
            return
        
        max_date = today.replace(year=today.year + 5)
        if target_date > max_date:
            await message.answer("Дата не может быть больше 5 лет вперед. Введите другую дату:")
            return
        
        await state.update_data(target_date=target_date)
        
        # Запрашиваем время
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="09:00", callback_data="time_09:00"),
                InlineKeyboardButton(text="12:00", callback_data="time_12:00"),
                InlineKeyboardButton(text="15:00", callback_data="time_15:00")
            ],
            [
                InlineKeyboardButton(text="18:00", callback_data="time_18:00"),
                InlineKeyboardButton(text="20:00", callback_data="time_20:00"),
                InlineKeyboardButton(text="Другое", callback_data="time_custom")
            ]
        ])
        
        await message.answer(
            "**Выберите время для ежедневных уведомлений:**\n\n"
            "Сообщения будут приходить каждый день в это время.\n"
            "В групповых чатах бот должен иметь разрешение на отправку сообщений.",
            reply_markup=keyboard
        )
        await state.set_state(CountdownState.waiting_for_time)
        
    except ValueError:
        await message.answer("Неверный формат даты! Используйте ДД.ММ.ГГГГ\nПопробуйте еще раз:")

@dp.callback_query(CountdownState.waiting_for_time)
async def process_time_selection(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработка выбора времени"""
    time_str = callback_query.data.replace("time_", "")
    
    if time_str == "custom":
        await callback_query.message.answer(
            "⌨️ **Введите время вручную**\n\n"
            "Формат: *ЧЧ:ММ*\n"
            "Пример: *09:30*, *14:15*\n\n"
            "Время должно быть от 00:00 до 23:59",
            parse_mode="Markdown"
        )
        await callback_query.answer()
        return
    
    data = await state.get_data()
    
    chat_type = callback_query.message.chat.type
    message_thread_id = 0
    
    # Для топиков в супергруппах
    if hasattr(callback_query.message, 'message_thread_id'):
        message_thread_id = callback_query.message.message_thread_id or 0
    
    event_data = {
        'chat_id': callback_query.message.chat.id,
        'user_id': callback_query.from_user.id,
        'event_name': data['event_name'],
        'target_date': data['target_date'],
        'notification_time': time_str,
        'chat_type': chat_type,
        'message_thread_id': message_thread_id
    }
    
    event_id = save_event(event_data)
    
    today = datetime.now(tz).date()
    days_left = days_until_target(data['target_date'], today)
    
    # Формируем ответ
    success_message = (
        f"**Отсчёт создан успешно!**\n\n"
        f"**Событие:** {data['event_name']}\n"
        f"**Дата:** {data['target_date'].strftime('%d.%m.%Y')}\n"
        f"**Уведомления:** ежедневно в {time_str}\n"
        f"**Осталось дней:** {days_left}\n\n"
        f"ID отсчёта: `{event_id[:8]}...`\n\n"
    )
    
    if chat_type in ['group', 'supergroup']:
        success_message += (
            "*В групповых чатах убедитесь, что у бота есть разрешение на отправку сообщений.*"
        )
    
    await callback_query.message.answer(success_message, parse_mode="Markdown")
    await state.clear()
    await callback_query.answer()

@dp.message(CountdownState.waiting_for_time)
async def process_custom_time(message: types.Message, state: FSMContext):
    """Обработка пользовательского времени"""
    try:
        # Проверяем формат времени
        time_obj = datetime.strptime(message.text, "%H:%M")
        time_str = time_obj.strftime("%H:%M")
        
        # Проверяем диапазон
        if not (0 <= time_obj.hour <= 23 and 0 <= time_obj.minute <= 59):
            raise ValueError
        
        # Сохраняем данные события
        data = await state.get_data()
        
        # Определяем тип чата
        chat_type = message.chat.type
        message_thread_id = 0
        
        # Для топиков в супергрупах
        if hasattr(message, 'message_thread_id'):
            message_thread_id = message.message_thread_id or 0
        
        event_data = {
            'chat_id': message.chat.id,
            'user_id': message.from_user.id,
            'event_name': data['event_name'],
            'target_date': data['target_date'],
            'notification_time': time_str,
            'chat_type': chat_type,
            'message_thread_id': message_thread_id
        }
        
        # Сохраняем в БД
        event_id = save_event(event_data)
        
        # Рассчитываем дни
        today = datetime.now(tz).date()
        days_left = days_until_target(data['target_date'], today)
        
        success_message = (
            f"**Отсчёт создан успешно!**\n\n"
            f"**Событие:** {data['event_name']}\n"
            f"**Дата:** {data['target_date'].strftime('%d.%m.%Y')}\n"
            f"**Уведомления:** ежедневно в {time_str}\n"
            f"**Осталось дней:** {days_left}\n\n"
            f"ID отсчёта: `{event_id[:8]}...`"
        )
        
        await message.answer(success_message, parse_mode="Markdown")
        await state.clear()
        
    except ValueError:
        await message.answer(
            "**Неверный формат времени!**\n\n"
            "Используйте формат *ЧЧ:ММ*\n"
            "Примеры: *09:00*, *14:30*, *23:59*\n\n"
            "Попробуйте еще раз:",
            parse_mode="Markdown"
        )

@dp.message(Command("list"))
@dp.message(F.text == "📋 Все отсчёты")
async def cmd_list(message: types.Message):
    """Показать все отсчёты в чате"""
    chat_events = get_chat_events(message.chat.id)
    
    if not chat_events:
        await message.answer(
            "**В этом чате нет активных отсчётов**\n\n"
            "Создайте первый отсчёт командой /new",
            parse_mode="Markdown"
        )
        return
    
    events_list = format_events_list(chat_events)
    
    events_list += (
        "\n\n**Как управлять:**\n"
        "• Чтобы удалить отсчёт, используйте /delete [ID]\n"
        "• Чтобы посмотреть свои отсчёты - /my\n"
        "• ID отсчёта показан в конце каждого пункта"
    )
    
    await message.answer(events_list, parse_mode="Markdown")

@dp.message(Command("my"))
@dp.message(F.text == "👤 Мои отсчёты")
async def cmd_my(message: types.Message):
    """Показать мои отсчёты в этом чате"""
    user_events = get_user_events_in_chat(message.chat.id, message.from_user.id)
    
    if not user_events:
        await message.answer(
            "**У вас нет отсчётов в этом чате**\n\n"
            "Создайте отсчёт командой /new",
            parse_mode="Markdown"
        )
        return
    
    today = datetime.now(tz).date()
    message_text = "**Ваши отсчёты в этом чате:**\n\n"
    
    for i, event in enumerate(user_events, 1):
        days_left = days_until_target(event['target_date'], today)
        
        if days_left == 1:
            day_word = "день"
        elif 2 <= days_left <= 4:
            day_word = "дня"
        else:
            day_word = "дней"
        
        message_text += f"{i}. **{event['event_name']}**\n"
        message_text += f"{event['target_date'].strftime('%d.%m.%Y')}\n"
        message_text += f"Уведомления в {event['notification_time']}\n"
        message_text += f"Осталось: {days_left} {day_word}\n"
        message_text += f"`{event['id'][:8]}...`\n\n"
    
    message_text += (
        "**Управление:**\n"
        "Чтобы удалить отсчёт, используйте:\n"
        "`/delete ID_отсчёта`\n\n"
        "Пример: `/delete " + user_events[0]['id'][:8] + "`"
    )
    
    await message.answer(message_text, parse_mode="Markdown")

@dp.message(Command("delete"))
@dp.message(F.text == "❌ Удалить отсчёт")
async def cmd_delete(message: types.Message, state: FSMContext, command: CommandObject = None):
    """Удалить отсчёт"""
    # Если передан ID в команде
    if command and command.args:
        event_id_short = command.args.strip()
        
        # Ищем полный ID
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
        SELECT id, event_name FROM events 
        WHERE id LIKE ? AND user_id = ? AND chat_id = ?
        ''', (f"{event_id_short}%", message.from_user.id, message.chat.id))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            event_id, event_name = result
            delete_event(event_id, message.from_user.id)
            await message.answer(f"Отсчёт \"{event_name}\" удалён!")
        else:
            await message.answer(
                "**Отсчёт не найден**\n\n"
                "Возможно:\n"
                "• ID указан неверно\n"
                "• Отсчёт создан другим пользователем\n"
                "• Отсчёт уже удалён\n\n"
                "Посмотрите ID своих отсчётов командой /my",
                parse_mode="Markdown"
            )
        return
    
    # Если ID не передан, показываем список для выбора
    user_events = get_user_events_in_chat(message.chat.id, message.from_user.id)
    
    if not user_events:
        await message.answer(
            "**У вас нет отсчётов для удаления в этом чате**",
            parse_mode="Markdown"
        )
        return
    
    # Создаем клавиатуру с кнопками для удаления
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    
    for event in user_events:
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{event['event_name']} ({event['target_date'].strftime('%d.%m.%Y')})",
                callback_data=f"delete_{event['id']}"
            )
        ])
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Отмена", callback_data="delete_cancel")
    ])
    
    await message.answer(
        "**Выберите отсчёт для удаления:**\n\n"
        "Отсчёт будет удалён без возможности восстановления.",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("delete_"))
async def process_delete(callback_query: types.CallbackQuery):
    """Обработка удаления через inline-кнопку"""
    if callback_query.data == "delete_cancel":
        await callback_query.message.delete()
        await callback_query.answer("Отмена")
        return
    
    event_id = callback_query.data.replace("delete_", "")
    
    # Получаем информацию об отсчёте
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    SELECT event_name FROM events 
    WHERE id = ? AND user_id = ?
    ''', (event_id, callback_query.from_user.id))
    
    result = cursor.fetchone()
    
    if result:
        event_name = result[0]
        delete_event(event_id, callback_query.from_user.id)
        
        await callback_query.message.edit_text(
            f"Отсчёт \"{event_name}\" удалён!",
            reply_markup=None
        )
    else:
        await callback_query.message.edit_text(
            "Не удалось удалить отсчёт.\n"
            "Возможно, он уже удалён или вы не являетесь создателем.",
            reply_markup=None
        )
    
    conn.close()
    await callback_query.answer()

@dp.message(Command("help"))
@dp.message(F.text == "ℹ️ Помощь")
async def cmd_help(message: types.Message):
    """Показать справку"""
    help_text = (
        "**Справка по боту обратного отсчёта**\n\n"
        
        "**Основные команды:**\n"
        "• /start - главное меню\n"
        "• /new - создать новый отсчёт\n"
        "• /list - все отсчёты в чате\n"
        "• /my - мои отсчёты в чате\n"
        "• /delete - удалить отсчёт\n"
        "• /help - эта справка\n\n"
        
        "**Создание отсчёта:**\n"
        "1. Название события (до 100 символов)\n"
        "2. Дата события (ДД.ММ.ГГГГ)\n"
        "3. Время ежедневных уведомлений\n\n"
        
        "**Как это работает:**\n"
        "• Бот отправляет сообщение каждый день в указанное время\n"
        "• Сообщение содержит количество оставшихся дней\n"
        "• Когда событие наступает, отсчёт автоматически прекращается\n\n"
        
        "**Работа в чатах:**\n"
        "• В группах все участники видны все отсчёты\n"
        "• Каждый видит только свои отсчёты для удаления\n"
        "• Для работы в группах боту нужны права на отправку сообщений\n\n"
        
        "**Управление отсчётами:**\n"
        "• Каждый отсчёт имеет уникальный ID\n"
        "• Для удаления используйте ID или кнопки\n"
        "• ID показывается в списках отсчётов\n\n"
        
        "**Примеры использования:**\n"
        "• Отсчёт до дня рождения\n"
        "• Отсчёт до отпуска\n"
        "• Отсчёт до дедлайна проекта\n"
        "• Отсчёт до праздников\n\n"
        
        "**Поддержка:**\n"
        "По вопросам и предложениям обращайтесь к создателю бота."
    )
    
    await message.answer(help_text, parse_mode="Markdown")

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Статистика по чату (только для админов в группах)"""
    chat_events = get_chat_events(message.chat.id)
    
    if not chat_events:
        await message.answer("В этом чате нет активных отсчётов")
        return
    
    # Статистика
    today = datetime.now(tz).date()
    total_events = len(chat_events)
    upcoming_events = sum(1 for e in chat_events if e['target_date'] >= today)
    
    # Самые близкие события
    closest_events = sorted(
        [e for e in chat_events if e['target_date'] >= today],
        key=lambda x: x['target_date']
    )[:3]
    
    stats_text = f"**Статистика чата**\n\n"
    stats_text += f"• Всего отсчётов: {total_events}\n"
    stats_text += f"• Активных: {upcoming_events}\n\n"
    
    if closest_events:
        stats_text += "**Ближайшие события:**\n"
        for event in closest_events:
            days_left = days_until_target(event['target_date'], today)
            if days_left == 1:
                day_word = "день"
            elif 2 <= days_left <= 4:
                day_word = "дня"
            else:
                day_word = "дней"
            stats_text += f"• {event['event_name']}: {days_left} {day_word}\n"
    
    await message.answer(stats_text, parse_mode="Markdown")

async def notification_scheduler():
    """Фоновый планировщик уведомлений"""
    while True:
        try:
            now = datetime.now(tz)
            current_time = now.strftime("%H:%M")
            today = now.date()
            
            # Получаем все активные события
            all_events = get_all_active_events()
            
            for event in all_events:
                # Проверяем время
                if event['notification_time'] == current_time:
                    # Проверяем, не отправляли ли уже сегодня
                    if was_notification_sent_today(event['id']):
                        continue
                    
                    days_left = days_until_target(event['target_date'], today)
                    
                    if days_left < 0:
                        deactivate_event(event['id'])
                        continue
                    
                    # Формируем сообщение
                    message = format_countdown_message(event['event_name'], days_left, event['target_date'])
                    
                    try:
                        # Отправляем сообщение в зависимости от типа чата
                        if event['chat_type'] in ['private', 'group', 'supergroup']:
                            # Для топиков в супергруппах
                            if event['message_thread_id']:
                                await bot.send_message(
                                    chat_id=event['chat_id'],
                                    text=message,
                                    message_thread_id=event['message_thread_id'],
                                    parse_mode="Markdown"
                                )
                            else:
                                await bot.send_message(
                                    chat_id=event['chat_id'],
                                    text=message,
                                    parse_mode="Markdown"
                                )
                        
                        # Отмечаем как отправленное
                        mark_notification_sent(event['id'], today)
                        
                        # Если событие сегодня, деактивируем после отправки
                        if days_left == 0:
                            deactivate_event(event['id'])
                    
                    except Exception as e:
                        logger.error(f"Ошибка отправки сообщения: {e}")
                        # Если бот удален из чата, деактивируем событие
                        if "chat not found" in str(e).lower() or "bot was blocked" in str(e).lower():
                            deactivate_event(event['id'])
            
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Ошибка в планировщике: {e}")
            await asyncio.sleep(60)

async def on_startup():
    """Действия при запуске"""
    logger.info("Бот запущен!")
    
    # Запускаем планировщик уведомлений
    asyncio.create_task(notification_scheduler())

async def main():
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())