import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from datetime import datetime, time, timedelta
import pytz
import os
import json
import asyncio
from typing import Dict, Any, List

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Токен бота из переменных окружения Railway
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7952222222:AAE99unNb3eKLySt8vyj46nI9TEelX-KZZ4')

# Таймзона Москвы
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Сообщение для автоответа
AUTO_REPLY_MESSAGE = """Здравствуйте, вы написали в нерабочее время компании!

Мы отвечаем с понедельника по пятницу | c 10:00 до 19:00 по МСК

**сообщение автоматическое, отвечать на него не нужно**"""

# ID администраторов (теперь несколько)
ADMIN_IDS = {7842709072, 1772492746, 1661202178, 478084322}

# Файлы для сохранения данных
FLAGS_FILE = "auto_reply_flags.json"
WORK_CHAT_FILE = "work_chat.json"
PENDING_MESSAGES_FILE = "pending_messages.json"
FUNNELS_CONFIG_FILE = "funnels_config.json"
EXCLUDED_USERS_FILE = "excluded_users.json"
FUNNELS_STATE_FILE = "funnels_state.json"

# ========== КЛАСС ДЛЯ УПРАВЛЕНИЯ СОСТОЯНИЕМ ВОРОНОК ==========

class FunnelsStateManager:
    def __init__(self):
        self.state = self.load_state()
    
    def load_state(self) -> Dict[str, Any]:
        """Загружает состояние воронок из файла"""
        try:
            if os.path.exists(FUNNELS_STATE_FILE):
                with open(FUNNELS_STATE_FILE, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки состояния воронок: {e}")
        
        # Состояние по умолчанию
        return {
            "last_funnel_1_check": None,
            "last_funnel_2_check": None, 
            "last_funnel_3_check": None,
            "funnel_1_messages_processed": [],
            "funnel_2_messages_processed": [],
            "funnel_3_messages_processed": []
        }
    
    def save_state(self):
        """Сохраняет состояние воронок в файл"""
        try:
            with open(FUNNELS_STATE_FILE, 'w') as f:
                json.dump(self.state, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Ошибка сохранения состояния воронок: {e}")
    
    def update_last_check(self, funnel_number: int):
        """Обновляет время последней проверки для воронки"""
        self.state[f"last_funnel_{funnel_number}_check"] = datetime.now(MOSCOW_TZ).isoformat()
        self.save_state()
    
    def get_last_check(self, funnel_number: int) -> datetime:
        """Возвращает время последней проверки для воронки"""
        timestamp = self.state.get(f"last_funnel_{funnel_number}_check")
        if timestamp:
            return datetime.fromisoformat(timestamp)
        return datetime.now(MOSCOW_TZ) - timedelta(days=1)  # Возвращаем старое время если нет данных
    
    def add_processed_message(self, funnel_number: int, message_key: str):
        """Добавляет сообщение в список обработанных для воронки"""
        key = f"funnel_{funnel_number}_messages_processed"
        if message_key not in self.state[key]:
            self.state[key].append(message_key)
            self.save_state()
    
    def is_message_processed(self, funnel_number: int, message_key: str) -> bool:
        """Проверяет, было ли сообщение уже обработано воронкой"""
        key = f"funnel_{funnel_number}_messages_processed"
        return message_key in self.state[key]
    
    def clear_processed_messages(self, funnel_number: int):
        """Очищает список обработанных сообщений для воронки"""
        key = f"funnel_{funnel_number}_messages_processed"
        self.state[key] = []
        self.save_state()

# ========== КЛАСС ДЛЯ УПРАВЛЕНИЯ ИСКЛЮЧЕНИЯМИ ==========

class ExcludedUsersManager:
    def __init__(self):
        self.excluded_users = self.load_excluded_users()
    
    def load_excluded_users(self) -> Dict[str, Any]:
        """Загружает список исключенных пользователей из файла"""
        try:
            if os.path.exists(EXCLUDED_USERS_FILE):
                with open(EXCLUDED_USERS_FILE, 'r') as f:
                    data = json.load(f)
                    return data
        except Exception as e:
            logger.error(f"Ошибка загрузки исключенных пользователей: {e}")
        
        # Значения по умолчанию
        return {
            "user_ids": [433733509, 1661202178, 478084322, 868325393, 1438860417, 879901619, 6107771545, 253353687, 2113096625, 91047831, 7842709072],
            "usernames": []
        }
    
    def save_excluded_users(self):
        """Сохраняет список исключенных пользователей в файл"""
        try:
            with open(EXCLUDED_USERS_FILE, 'w') as f:
                json.dump(self.excluded_users, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения исключенных пользователей: {e}")
    
    def is_user_excluded(self, user_id: int, username: str = None) -> bool:
        """Проверяет, является ли пользователь исключенным"""
        # Проверка по ID
        if user_id in self.excluded_users["user_ids"]:
            return True
        
        # Проверка по username
        if username and username.lower() in [u.lower() for u in self.excluded_users["usernames"]]:
            return True
        
        return False
    
    def add_user_id(self, user_id: int) -> bool:
        """Добавляет ID пользователя в исключения"""
        if user_id not in self.excluded_users["user_ids"]:
            self.excluded_users["user_ids"].append(user_id)
            self.save_excluded_users()
            logger.info(f"✅ Добавлен ID в исключения: {user_id}")
            return True
        return False
    
    def add_username(self, username: str) -> bool:
        """Добавляет username в исключения"""
        # Убираем @ если есть
        username = username.lstrip('@').lower()
        if username not in [u.lower() for u in self.excluded_users["usernames"]]:
            self.excluded_users["usernames"].append(username)
            self.save_excluded_users()
            logger.info(f"✅ Добавлен username в исключения: @{username}")
            return True
        return False
    
    def remove_user_id(self, user_id: int) -> bool:
        """Удаляет ID пользователя из исключений"""
        if user_id in self.excluded_users["user_ids"]:
            self.excluded_users["user_ids"].remove(user_id)
            self.save_excluded_users()
            logger.info(f"✅ Удален ID из исключений: {user_id}")
            return True
        return False
    
    def remove_username(self, username: str) -> bool:
        """Удаляет username из исключений"""
        username = username.lstrip('@').lower()
        for u in self.excluded_users["usernames"]:
            if u.lower() == username:
                self.excluded_users["usernames"].remove(u)
                self.save_excluded_users()
                logger.info(f"✅ Удален username из исключений: @{username}")
                return True
        return False
    
    def get_all_excluded(self) -> Dict[str, List]:
        """Возвращает всех исключенных пользователей"""
        return self.excluded_users
    
    def clear_all(self):
        """Очищает все исключения"""
        self.excluded_users = {"user_ids": [], "usernames": []}
        self.save_excluded_users()
        logger.info("✅ Все исключения очищены")

# ========== КЛАССЫ ДЛЯ УПРАВЛЕНИЯ ДАННЫМИ ==========

class FunnelsConfig:
    def __init__(self):
        self.funnels = self.load_funnels()
    
    def load_funnels(self) -> Dict[int, int]:
        """Загружает конфигурацию воронок из файла или использует значения по умолчанию"""
        try:
            if os.path.exists(FUNNELS_CONFIG_FILE):
                with open(FUNNELS_CONFIG_FILE, 'r') as f:
                    data = json.load(f)
                    return {int(k): v for k, v in data.items()}
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации воронок: {e}")
        
        # Значения по умолчанию
        return {
            1: 60,    # 1 час
            2: 180,   # 3 часа
            3: 360    # 6 часов
        }
    
    def save_funnels(self):
        """Сохраняет конфигурацию воронок в файл"""
        try:
            with open(FUNNELS_CONFIG_FILE, 'w') as f:
                json.dump(self.funnels, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения конфигурации воронок: {e}")
    
    def get_funnels(self) -> Dict[int, int]:
        """Возвращает текущую конфигурацию воронок"""
        return self.funnels
    
    def set_funnel_interval(self, funnel_number: int, minutes: int) -> bool:
        """Устанавливает интервал для указанной воронки"""
        if funnel_number in [1, 2, 3] and minutes > 0:
            self.funnels[funnel_number] = minutes
            self.save_funnels()
            logger.info(f"Установлен интервал для воронки {funnel_number}: {minutes} минут")
            return True
        return False
    
    def get_funnel_interval(self, funnel_number: int) -> int:
        """Возвращает интервал для указанной воронки"""
        return self.funnels.get(funnel_number, 0)
    
    def reset_to_default(self):
        """Сбрасывает настройки воронок к значениям по умолчанию"""
        self.funnels = {
            1: 60,    # 1 час
            2: 180,   # 3 часа
            3: 360    # 6 часов
        }
        self.save_funnels()
        logger.info("Настройки воронок сброшены к значениям по умолчанию")

class AutoReplyFlags:
    def __init__(self):
        self.flags = self.load_flags()
    
    def load_flags(self) -> Dict[str, bool]:
        try:
            if os.path.exists(FLAGS_FILE):
                with open(FLAGS_FILE, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки флагов: {e}")
        return {}
    
    def save_flags(self):
        try:
            with open(FLAGS_FILE, 'w') as f:
                json.dump(self.flags, f)
        except Exception as e:
            logger.error(f"Ошибка сохранения флагов: {e}")
    
    def has_replied(self, key: str) -> bool:
        return self.flags.get(key, False)
    
    def set_replied(self, key: str):
        self.flags[key] = True
        self.save_flags()
    
    def clear_replied(self, key: str):
        if key in self.flags:
            del self.flags[key]
            self.save_flags()
    
    def clear_all(self):
        self.flags = {}
        self.save_flags()
    
    def count_flags(self):
        return len(self.flags)

class WorkChatManager:
    def __init__(self):
        self.work_chat_id = self.load_work_chat()
    
    def load_work_chat(self):
        try:
            if os.path.exists(WORK_CHAT_FILE):
                with open(WORK_CHAT_FILE, 'r') as f:
                    data = json.load(f)
                    return data.get('work_chat_id')
        except Exception as e:
            logger.error(f"Ошибка загрузки рабочего чата: {e}")
        return None
    
    def save_work_chat(self, chat_id):
        try:
            with open(WORK_CHAT_FILE, 'w') as f:
                json.dump({'work_chat_id': chat_id}, f)
            self.work_chat_id = chat_id
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения рабочего чата: {e}")
            return False
    
    def get_work_chat_id(self):
        return self.work_chat_id
    
    def is_work_chat_set(self):
        return self.work_chat_id is not None

class PendingMessagesManager:
    def __init__(self, funnels_config: FunnelsConfig):
        self.pending_messages = self.load_pending_messages()
        self.funnels_config = funnels_config
    
    def load_pending_messages(self) -> Dict[str, Any]:
        try:
            if os.path.exists(PENDING_MESSAGES_FILE):
                with open(PENDING_MESSAGES_FILE, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки непрочитанных сообщений: {e}")
        return {}
    
    def save_pending_messages(self):
        try:
            with open(PENDING_MESSAGES_FILE, 'w') as f:
                json.dump(self.pending_messages, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения непрочитанных сообщений: {e}")
    
    def add_message(self, chat_id: int, user_id: int, message_text: str, message_id: int, chat_title: str = None, username: str = None, first_name: str = None):
        # Генерируем уникальный ключ для каждого сообщения
        key = f"{chat_id}_{user_id}_{message_id}_{int(datetime.now().timestamp())}"
        
        # Если текст сообщения пустой (например, фото/документ), создаем описание
        if not message_text:
            message_text = "[Сообщение без текста]"
        
        self.pending_messages[key] = {
            'chat_id': chat_id,
            'user_id': user_id,
            'message_text': message_text,
            'message_id': message_id,
            'chat_title': chat_title,
            'username': username,
            'first_name': first_name,
            'timestamp': datetime.now(MOSCOW_TZ).isoformat(),
            'funnels_sent': [],
            'current_funnel': 0,  # 0 = еще не в воронке
            'message_key': key  # Сохраняем ключ для удаления
        }
        self.save_pending_messages()
        logger.info(f"✅ Добавлено непрочитанное сообщение: {key} - '{message_text[:50]}...'")
    
    def remove_message_by_key(self, key: str):
        """Удаляет сообщение по ключу"""
        if key in self.pending_messages:
            del self.pending_messages[key]
            self.save_pending_messages()
            logger.info(f"✅ Удалено непрочитанное сообщение: {key}")
            return True
        return False
    
    def remove_all_chat_messages(self, chat_id: int, user_id: int = None):
        """Удаляет все сообщения из чата (или конкретного пользователя)"""
        keys_to_remove = []
        for key, message in self.pending_messages.items():
            if message['chat_id'] == chat_id:
                if user_id is None or message['user_id'] == user_id:
                    keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.pending_messages[key]
        
        if keys_to_remove:
            self.save_pending_messages()
            logger.info(f"✅ Удалено {len(keys_to_remove)} сообщений из чата {chat_id}")
            return len(keys_to_remove)
        return 0
    
    def get_all_pending_messages(self) -> List[Dict[str, Any]]:
        return list(self.pending_messages.values())
    
    def mark_funnel_sent(self, message_key: str, funnel_number: int):
        """Помечает, что для сообщения отправлена воронка"""
        if message_key in self.pending_messages:
            if funnel_number not in self.pending_messages[message_key]['funnels_sent']:
                self.pending_messages[message_key]['funnels_sent'].append(funnel_number)
                self.pending_messages[message_key]['current_funnel'] = funnel_number
                self.save_pending_messages()
    
    def find_messages_by_chat(self, chat_id: int) -> List[Dict[str, Any]]:
        """Находит все сообщения из указанного чата"""
        result = []
        for message in self.pending_messages.values():
            if message['chat_id'] == chat_id:
                result.append(message)
        return result
    
    def get_messages_for_funnel(self, funnel_number: int, funnels_state: FunnelsStateManager) -> List[Dict[str, Any]]:
        """Возвращает сообщения, готовые для отправки в указанную воронку"""
        result = []
        now = datetime.now(MOSCOW_TZ)
        FUNNELS = self.funnels_config.get_funnels()
        funnel_minutes = FUNNELS[funnel_number]
        
        for message_key, message in self.pending_messages.items():
            # Пропускаем сообщения, уже обработанные этой воронкой
            if funnels_state.is_message_processed(funnel_number, message_key):
                continue
                
            timestamp = datetime.fromisoformat(message['timestamp'])
            time_diff = now - timestamp
            minutes_passed = int(time_diff.total_seconds() / 60)
            
            current_funnel = message.get('current_funnel', 0)
            funnels_sent = message.get('funnels_sent', [])
            
            # Логика для каждой воронки:
            if funnel_number == 1:
                # Первая воронка: сообщение подходит если прошло >= 1 час И еще не было воронок
                if (minutes_passed >= funnel_minutes and 
                    current_funnel == 0 and 
                    funnel_number not in funnels_sent):
                    message['message_key'] = message_key
                    message['minutes_passed'] = minutes_passed
                    result.append(message)
                    
            elif funnel_number == 2:
                # Вторая воронка: сообщение подходит если прошло >= 3 часа И уже была в 1-й воронке И еще не было во 2-й
                if (minutes_passed >= funnel_minutes and 
                    1 in funnels_sent and 
                    funnel_number not in funnels_sent):
                    message['message_key'] = message_key
                    message['minutes_passed'] = minutes_passed
                    result.append(message)
                    
            elif funnel_number == 3:
                # Третья воронка: сообщение подходит если прошло >= 6 часов И уже была во 2-й воронке И еще не было в 3-й
                if (minutes_passed >= funnel_minutes and 
                    2 in funnels_sent and 
                    funnel_number not in funnels_sent):
                    message['message_key'] = message_key
                    message['minutes_passed'] = minutes_passed
                    result.append(message)
        
        return result
    
    def get_all_messages_older_than(self, minutes_threshold: int) -> List[Dict[str, Any]]:
        """Возвращает все сообщения, которые ждут ответа дольше указанного времени в минутах"""
        result = []
        now = datetime.now(MOSCOW_TZ)
        
        for message_key, message in self.pending_messages.items():
            timestamp = datetime.fromisoformat(message['timestamp'])
            time_diff = now - timestamp
            minutes_passed = int(time_diff.total_seconds() / 60)
            
            if minutes_passed >= minutes_threshold:
                message['message_key'] = message_key
                message['minutes_passed'] = minutes_passed
                result.append(message)
        
        return result
    
    def clear_all(self):
        count = len(self.pending_messages)
        self.pending_messages = {}
        self.save_pending_messages()
        logger.info(f"✅ Очищены все непрочитанные сообщения ({count} шт.)")
        return count

# ========== ГЛОБАЛЬНЫЕ ЭКЗЕМПЛЯРЫ ==========

funnels_config = FunnelsConfig()
flags_manager = AutoReplyFlags()
work_chat_manager = WorkChatManager()
pending_messages_manager = PendingMessagesManager(funnels_config)
excluded_users_manager = ExcludedUsersManager()
funnels_state_manager = FunnelsStateManager()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def is_manager(user_id: int, username: str = None) -> bool:
    return excluded_users_manager.is_user_excluded(user_id, username)

def is_excluded_user(user_id: int) -> bool:
    return excluded_users_manager.is_user_excluded(user_id)

def is_working_hours():
    """Проверяет, находится ли текущее время в рабочем интервале (10:00-19:00)"""
    now = datetime.now(MOSCOW_TZ)
    current_time = now.time()
    if current_time >= time(10, 0) and current_time <= time(19, 0):
        return True
    return False

def should_respond_to_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Определяет, нужно ли обрабатывать сообщение"""
    if not update or not update.message:
        return False
    
    if update.message.from_user.id == context.bot.id:
        return False
        
    if is_excluded_user(update.message.from_user.id):
        return False
        
    if update.message.new_chat_members or update.message.left_chat_member:
        return False
        
    if update.message.pinned_message:
        return False
        
    if update.edited_message:
        return False
        
    if update.message.text and update.message.text.startswith('/'):
        return False
        
    # Разрешаем обработку сообщений без текста (фото, документы и т.д.)
    if update.message.text and len(update.message.text.strip()) < 1:
        return False
        
    return True

def get_chat_display_name(chat_data: Dict[str, Any]) -> str:
    """Получает отображаемое название чата из данных сообщения"""
    chat_title = chat_data.get('chat_title')
    username = chat_data.get('username')
    first_name = chat_data.get('first_name')
    
    if chat_title:
        return f"💬 {chat_title}"
    elif username:
        return f"👤 @{username}"
    elif first_name:
        return f"👤 {first_name}"
    else:
        return f"💬 Чат {chat_data['chat_id']}"

def get_funnel_emoji(funnel_number: int) -> str:
    """Возвращает эмодзи для воронки"""
    emojis = {
        1: "🟡",  # Желтый - первая воронка
        2: "🟠",  # Оранжевый - вторая воронка  
        3: "🔴"   # Красный - третья воронка
    }
    return emojis.get(funnel_number, "⚪")

def format_time_ago(timestamp: str) -> str:
    """Форматирует время в читаемый формат"""
    message_time = datetime.fromisoformat(timestamp)
    now = datetime.now(MOSCOW_TZ)
    time_diff = now - message_time
    
    hours = int(time_diff.total_seconds() / 3600)
    minutes = int((time_diff.total_seconds() % 3600) / 60)
    
    if hours > 0:
        return f"{hours}ч {minutes}м"
    else:
        return f"{minutes}м"

def minutes_to_hours_minutes(minutes: int) -> str:
    """Конвертирует минуты в формат 'X ч Y м'"""
    hours = minutes // 60
    mins = minutes % 60
    if hours > 0:
        return f"{hours} ч {mins} м"
    else:
        return f"{mins} м"

# ========== КОМАНДЫ ДЛЯ УПРАВЛЕНИЯ ИСКЛЮЧЕНИЯМИ ==========

async def add_exception_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет пользователя в исключения"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    if not context.args:
        await update.message.reply_text(
            "❌ Использование:\n"
            "Добавить по ID: `/add_exception 123456789`\n"
            "Добавить по username: `/add_exception @username`"
        )
        return
    
    identifier = context.args[0]
    
    # Проверяем, это ID или username
    if identifier.isdigit():
        # Это ID
        user_id = int(identifier)
        if excluded_users_manager.add_user_id(user_id):
            await update.message.reply_text(f"✅ ID `{user_id}` добавлен в исключения")
        else:
            await update.message.reply_text(f"ℹ️ ID `{user_id}` уже в исключениях")
    else:
        # Это username
        if excluded_users_manager.add_username(identifier):
            await update.message.reply_text(f"✅ Username `{identifier}` добавлен в исключения")
        else:
            await update.message.reply_text(f"ℹ️ Username `{identifier}` уже в исключениях")

async def remove_exception_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет пользователя из исключений"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    if not context.args:
        await update.message.reply_text(
            "❌ Использование:\n"
            "Удалить по ID: `/remove_exception 123456789`\n"
            "Удалить по username: `/remove_exception @username`"
        )
        return
    
    identifier = context.args[0]
    
    # Проверяем, это ID или username
    if identifier.isdigit():
        # Это ID
        user_id = int(identifier)
        if excluded_users_manager.remove_user_id(user_id):
            await update.message.reply_text(f"✅ ID `{user_id}` удален из исключений")
        else:
            await update.message.reply_text(f"❌ ID `{user_id}` не найден в исключениях")
    else:
        # Это username
        if excluded_users_manager.remove_username(identifier):
            await update.message.reply_text(f"✅ Username `{identifier}` удален из исключений")
        else:
            await update.message.reply_text(f"❌ Username `{identifier}` не найден в исключениях")

async def list_exceptions_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список всех исключений"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    excluded_users = excluded_users_manager.get_all_excluded()
    
    if not excluded_users["user_ids"] and not excluded_users["usernames"]:
        await update.message.reply_text("📝 Список исключений пуст")
        return
    
    text = "👥 **СПИСОК ИСКЛЮЧЕННЫХ ПОЛЬЗОВАТЕЛЕЙ**\n\n"
    
    if excluded_users["user_ids"]:
        text += "🆔 **По ID:**\n"
        for i, user_id in enumerate(excluded_users["user_ids"], 1):
            text += f"{i}. `{user_id}`\n"
        text += "\n"
    
    if excluded_users["usernames"]:
        text += "👤 **По username:**\n"
        for i, username in enumerate(excluded_users["usernames"], 1):
            text += f"{i}. `@{username}`\n"
    
    text += f"\n📊 Всего: {len(excluded_users['user_ids'])} ID + {len(excluded_users['usernames'])} username"
    
    await update.message.reply_text(text, parse_mode='Markdown')

async def clear_exceptions_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Очищает все исключения"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    excluded_users_manager.clear_all()
    await update.message.reply_text("✅ Все исключения очищены")

# ========== КОМАНДЫ ДЛЯ УПРАВЛЕНИЯ ВОРОНКАМИ ==========

async def funnels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /funnels - показывает текущие настройки воронок"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    FUNNELS = funnels_config.get_funnels()
    
    funnels_text = f"""
⚙️ **ТЕКУЩИЕ НАСТРОЙКИ ВОРОНОК**

🟡 **Воронка 1 (начальное уведомление):**
   - Интервал: {FUNNELS[1]} минут ({minutes_to_hours_minutes(FUNNELS[1])})
   - Команда: `/set_funnel_1 <минуты>`

🟠 **Воронка 2 (повторное уведомление):**
   - Интервал: {FUNNELS[2]} минут ({minutes_to_hours_minutes(FUNNELS[2])})
   - Команда: `/set_funnel_2 <минуты>`

🔴 **Воронка 3 (срочное уведомление):**
   - Интервал: {FUNNELS[3]} минут ({minutes_to_hours_minutes(FUNNELS[3])})
   - Команда: `/set_funnel_3 <минуты>`

🔄 Сбросить настройки: `/reset_funnels`
    """
    
    await update.message.reply_text(funnels_text, parse_mode='Markdown')

async def set_funnel_1_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /set_funnel_1"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Использование: /set_funnel_1 <минуты>")
        return
    
    minutes = int(context.args[0])
    if minutes <= 0:
        await update.message.reply_text("❌ Количество минут должно быть положительным числом")
        return
    
    if funnels_config.set_funnel_interval(1, minutes):
        await update.message.reply_text(f"✅ Воронка 1 установлена на {minutes} минут ({minutes_to_hours_minutes(minutes)})")
    else:
        await update.message.reply_text("❌ Ошибка установки интервала воронки")

async def set_funnel_2_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /set_funnel_2"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Использование: /set_funnel_2 <минуты>")
        return
    
    minutes = int(context.args[0])
    if minutes <= 0:
        await update.message.reply_text("❌ Количество минут должно быть положительным числом")
        return
    
    if funnels_config.set_funnel_interval(2, minutes):
        await update.message.reply_text(f"✅ Воронка 2 установлена на {minutes} минут ({minutes_to_hours_minutes(minutes)})")
    else:
        await update.message.reply_text("❌ Ошибка установки интервала воронки")

async def set_funnel_3_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /set_funnel_3"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("❌ Использование: /set_funnel_3 <минуты>")
        return
    
    minutes = int(context.args[0])
    if minutes <= 0:
        await update.message.reply_text("❌ Количество минут должно быть положительным числом")
        return
    
    if funnels_config.set_funnel_interval(3, minutes):
        await update.message.reply_text(f"✅ Воронка 3 установлена на {minutes} минут ({minutes_to_hours_minutes(minutes)})")
    else:
        await update.message.reply_text("❌ Ошибка установки интервала воронки")

async def reset_funnels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /reset_funnels"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    funnels_config.reset_to_default()
    await update.message.reply_text("✅ Настройки воронок сброшены к значениям по умолчанию")

# ========== ОСНОВНЫЕ КОМАНДЫ ==========

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    if not update or not update.message:
        return
        
    await update.message.reply_text(
        "🤖 Бот-автоответчик запущен!\n\n"
        "📋 Доступные команды:\n"
        "/status - статус системы\n"
        "/funnels - настройки воронок\n"
        "/pending - список непрочитанных\n"
        "/managers - список менеджеров\n"
        "/stats - статистика\n"
        "/help - помощь\n\n"
        "👥 **Управление исключениями:**\n"
        "/add_exception - добавить исключение\n"
        "/remove_exception - удалить исключение\n"
        "/list_exceptions - список исключений\n"
        "/clear_exceptions - очистить все исключения"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    if not update or not update.message:
        return
        
    help_text = """
📖 **СПРАВКА ПО КОМАНДАМ БОТА**

**Основные команды:**
/start - запуск бота
/status - статус системы
/help - эта справка

**Управление воронками:**
/funnels - текущие настройки воронок
/set_funnel_1 <минуты> - установить интервал 1-й воронки
/set_funnel_2 <минуты> - установить интервал 2-й воронки  
/set_funnel_3 <минуты> - установить интервал 3-й воронки
/reset_funnels - сбросить настройки воронок

**Рабочий чат:**
/set_work_chat - установить этот чат как рабочий (для уведомлений)

**Управление сообщениями:**
/pending - список непрочитанных сообщений
/clear_chat - очистить сообщения из текущего чата
/clear_all - очистить все сообщения

**Управление исключениями:**
/add_exception <ID/@username> - добавить менеджера
/remove_exception <ID/@username> - удалить менеджера
/list_exceptions - список всех менеджеров
/clear_exceptions - очистить все исключения

**Ручная проверка воронок:**
/check_voronka_1 - проверить воронку 1
/check_voronka_2 - проверить воронку 2
/check_voronka_3 - проверить воронку 3
/check_all_voronki - проверить все воронки
/force_funnel_check - принудительная проверка

**Статистика:**
/stats - статистика системы
/managers - список менеджеров
    """
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /status"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    FUNNELS = funnels_config.get_funnels()
    now = datetime.now(MOSCOW_TZ)
    excluded_users = excluded_users_manager.get_all_excluded()
    total_excluded = len(excluded_users["user_ids"]) + len(excluded_users["usernames"])
    
    status_text = f"""
📊 **СТАТУС СИСТЕМЫ**

⏰ **Время:** {now.strftime('%d.%m.%Y %H:%M:%S')}
🕐 **Рабочие часы:** {'✅ ДА' if is_working_hours() else '❌ НЕТ'}

📋 **Непрочитанные сообщения:** {len(pending_messages_manager.get_all_pending_messages())}
🚩 **Флаги автоответов:** {flags_manager.count_flags()}
💬 **Рабочий чат:** {'✅ Установлен' if work_chat_manager.is_work_chat_set() else '❌ Не установлен'}

⚙️ **НАСТРОЙКИ ВОРОНОК:**
🟡 Воронка 1: {FUNNELS[1]} мин ({minutes_to_hours_minutes(FUNNELS[1])})
🟠 Воронка 2: {FUNNELS[2]} мин ({minutes_to_hours_minutes(FUNNELS[2])})
🔴 Воронка 3: {FUNNELS[3]} мин ({minutes_to_hours_minutes(FUNNELS[3])})

👥 **Менеджеров в системе:** {total_excluded} ({len(excluded_users["user_ids"])} ID + {len(excluded_users["usernames"])} username)
    """
    
    await update.message.reply_text(status_text, parse_mode='Markdown')

async def set_work_chat_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /set_work_chat"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    chat_id = update.message.chat.id
    if work_chat_manager.save_work_chat(chat_id):
        await update.message.reply_text(f"✅ Этот чат установлен как рабочий (ID: {chat_id})")
    else:
        await update.message.reply_text("❌ Ошибка сохранения рабочего чата")

async def managers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /managers"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    excluded_users = excluded_users_manager.get_all_excluded()
    
    if not excluded_users["user_ids"] and not excluded_users["usernames"]:
        await update.message.reply_text("📝 Список менеджеров пуст")
        return
    
    text = "👥 **СПИСОК МЕНЕДЖЕРОВ**\n\n"
    
    if excluded_users["user_ids"]:
        text += "🆔 **По ID:**\n"
        for i, user_id in enumerate(excluded_users["user_ids"], 1):
            text += f"{i}. `{user_id}`\n"
        text += "\n"
    
    if excluded_users["usernames"]:
        text += "👤 **По username:**\n"
        for i, username in enumerate(excluded_users["usernames"], 1):
            text += f"{i}. `@{username}`\n"
    
    text += f"\n📊 Всего: {len(excluded_users['user_ids'])} ID + {len(excluded_users['usernames'])} username"
    
    await update.message.reply_text(text, parse_mode='Markdown')

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /stats"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    all_pending = pending_messages_manager.get_all_pending_messages()
    excluded_users = excluded_users_manager.get_all_excluded()
    total_excluded = len(excluded_users["user_ids"]) + len(excluded_users["usernames"])
    
    # Анализируем сообщения по времени
    now = datetime.now(MOSCOW_TZ)
    time_stats = {
        "менее 1 часа": 0,
        "1-3 часа": 0,
        "3-6 часов": 0,
        "более 6 часов": 0
    }
    
    for message in all_pending:
        timestamp = datetime.fromisoformat(message['timestamp'])
        time_diff = now - timestamp
        hours_passed = time_diff.total_seconds() / 3600
        
        if hours_passed < 1:
            time_stats["менее 1 часа"] += 1
        elif hours_passed < 3:
            time_stats["1-3 часа"] += 1
        elif hours_passed < 6:
            time_stats["3-6 часов"] += 1
        else:
            time_stats["более 6 часов"] += 1
    
    stats_text = f"""
📈 **СТАТИСТИКА СИСТЕМЫ**

📊 **Общая статистика:**
   - Непрочитанных сообщений: {len(all_pending)}
   - Флагов автоответов: {flags_manager.count_flags()}
   - Менеджеров в системе: {total_excluded} ({len(excluded_users["user_ids"])} ID + {len(excluded_users["usernames"])} username)

⏱ **Время ожидания ответа:**
   - Менее 1 часа: {time_stats['менее 1 часа']}
   - 1-3 часа: {time_stats['1-3 часа']}
   - 3-6 часов: {time_stats['3-6 часов']}
   - Более 6 часов: {time_stats['более 6 часов']}

⚙️ **Рабочий чат:** {'✅ Установлен' if work_chat_manager.is_work_chat_set() else '❌ Не установлен'}
🕐 **Текущее время:** {now.strftime('%H:%M:%S')}
    """
    
    await update.message.reply_text(stats_text, parse_mode='Markdown')

# ========== УЛУЧШЕННАЯ СИСТЕМА ВОРОНОК ==========

async def send_funnel_notification(context: ContextTypes.DEFAULT_TYPE, funnel_number: int, messages: List[Dict[str, Any]]):
    """Отправляет уведомление воронки в рабочий чат"""
    work_chat_id = work_chat_manager.get_work_chat_id()
    if not work_chat_id:
        logger.error("❌ Не могу отправить уведомление воронки: рабочий чат не установлен")
        return
    
    FUNNELS = funnels_config.get_funnels()
    funnel_emoji = get_funnel_emoji(funnel_number)
    funnel_minutes = FUNNELS[funnel_number]
    
    # Определяем текст в зависимости от времени
    time_text = minutes_to_hours_minutes(funnel_minutes).upper()
    
    # Заголовок уведомления в зависимости от воронки
    if funnel_number == 1:
        header = f"{funnel_emoji} <b>ВОРОНКА 1: СООБЩЕНИЯ ЖДУТ ОТВЕТА {time_text}</b>"
        description = f"📊 Новых сообщений: <b>{len(messages)}</b>"
    elif funnel_number == 2:
        header = f"{funnel_emoji} <b>ВОРОНКА 2: СООБЩЕНИЯ ЖДУТ ОТВЕТА {time_text}</b>"
        description = f"📊 Сообщений требует внимания: <b>{len(messages)}</b>"
    else:  # funnel_number == 3
        header = f"{funnel_emoji} <b>ВОРОНКА 3: СРОЧНЫЕ СООБЩЕНИЯ ЖДУТ ОТВЕТА {time_text}</b>"
        description = f"📊 Срочных сообщений: <b>{len(messages)}</b>"
    
    # Группируем сообщения по чатам для лучшего отображения
    chats_messages = {}
    for message in messages:
        chat_id = message['chat_id']
        if chat_id not in chats_messages:
            chats_messages[chat_id] = []
        chats_messages[chat_id].append(message)
    
    notification_text = f"""
{header}

{description}

📋 <b>Список чатов с непрочитанными сообщениями:</b>
"""
    
    # Добавляем информацию по каждому чату
    chat_number = 1
    for chat_id, chat_messages in chats_messages.items():
        first_message = chat_messages[0]
        chat_display = get_chat_display_name(first_message)
        
        notification_text += f"\n{chat_number}. {chat_display}"
        notification_text += f"\n   📝 Сообщений: {len(chat_messages)}"
        
        # Показываем время самого старого сообщения в чате
        oldest_timestamp = min(msg['timestamp'] for msg in chat_messages)
        time_ago = format_time_ago(oldest_timestamp)
        notification_text += f"\n   ⏰ Самое старое: {time_ago} назад"
        
        # Показываем все сообщения из этого чата (первые 5)
        for i, msg in enumerate(chat_messages[:5]):
            user_info = f"@{msg['username']}" if msg.get('username') else f"ID:{msg['user_id']}"
            message_preview = msg['message_text'][:80] + "..." if len(msg['message_text']) > 80 else msg['message_text']
            notification_text += f"\n      {i+1}. {user_info}: {message_preview}"
        
        if len(chat_messages) > 5:
            notification_text += f"\n      ... и еще {len(chat_messages) - 5} сообщ."
        
        notification_text += f"\n"
        chat_number += 1
    
    notification_text += f"\n💡 <i>Сообщения остаются в системе до ручного удаления</i>"
    
    try:
        await context.bot.send_message(chat_id=work_chat_id, text=notification_text, parse_mode='HTML')
        
        # Помечаем сообщения как обработанные в этой воронке
        for message_data in messages:
            pending_messages_manager.mark_funnel_sent(message_data['message_key'], funnel_number)
            funnels_state_manager.add_processed_message(funnel_number, message_data['message_key'])
        
        logger.info(f"✅ Уведомление воронки {funnel_number} отправлено в рабочий чат. Чатов: {len(chats_messages)}, Сообщений: {len(messages)}")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления воронки {funnel_number}: {e}")

async def check_funnel_messages(context: ContextTypes.DEFAULT_TYPE, funnel_number: int):
    """Проверяет и отправляет уведомления для конкретной воронки"""
    
    if not work_chat_manager.is_work_chat_set():
        logger.warning("❌ Рабочий чат не установлен!")
        return
    
    if not is_working_hours():
        logger.warning("❌ Сейчас нерабочее время - уведомления не отправляются")
        return
    
    FUNNELS = funnels_config.get_funnels()
    funnel_minutes = FUNNELS[funnel_number]
    
    # Получаем время последней проверки для этой воронки
    last_check = funnels_state_manager.get_last_check(funnel_number)
    now = datetime.now(MOSCOW_TZ)
    time_since_last_check = now - last_check
    
    # Проверяем, прошло ли достаточно времени с последней проверки
    check_interval = timedelta(minutes=min(60, funnel_minutes))
    
    if time_since_last_check < check_interval:
        logger.info(f"⏰ Воронка {funnel_number}: проверка пропущена, прошло только {int(time_since_last_check.total_seconds() / 60)} мин.")
        return
    
    logger.info(f"🔍 Проверка воронки {funnel_number} (интервал: {funnel_minutes} мин)")
    
    # Получаем сообщения для этой воронки
    messages_for_funnel = pending_messages_manager.get_messages_for_funnel(funnel_number, funnels_state_manager)
    
    if messages_for_funnel:
        # Группируем сообщения по чатам для логирования
        chats_in_funnel = {}
        for msg in messages_for_funnel:
            chat_id = msg['chat_id']
            if chat_id not in chats_in_funnel:
                chats_in_funnel[chat_id] = []
            chats_in_funnel[chat_id].append(msg)
        
        logger.info(f"🚨 Воронка {funnel_number}: {len(chats_in_funnel)} чатов, {len(messages_for_funnel)} сообщений")
        
        for chat_id, chat_messages in chats_in_funnel.items():
            chat_display = get_chat_display_name(chat_messages[0])
            logger.info(f"  📝 {chat_display}: {len(chat_messages)} сообщ.")
        
        await send_funnel_notification(context, funnel_number, messages_for_funnel)
        
        # Обновляем время последней проверки
        funnels_state_manager.update_last_check(funnel_number)
    else:
        logger.info(f"✅ Воронка {funnel_number}: нет новых сообщений для уведомления")

async def check_all_funnels(context: ContextTypes.DEFAULT_TYPE):
    """Проверяет все воронки по очереди"""
    logger.info("🔄 Запуск проверки всех воронок...")
    
    # Проверяем каждую воронку
    for funnel_number in [1, 2, 3]:
        await check_funnel_messages(context, funnel_number)
        await asyncio.sleep(1)  # Небольшая пауза между проверками

# ========== КОМАНДЫ ДЛЯ РУЧНОЙ ПРОВЕРКИ ВОРОНОК ==========

async def check_voronka_1_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручная проверка воронки 1"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    await update.message.reply_text("🔍 Запускаю ручную проверку воронки 1...")
    await check_funnel_messages(context, 1)
    await update.message.reply_text("✅ Проверка воронки 1 завершена")

async def check_voronka_2_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручная проверка воронки 2"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    await update.message.reply_text("🔍 Запускаю ручную проверку воронки 2...")
    await check_funnel_messages(context, 2)
    await update.message.reply_text("✅ Проверка воронки 2 завершена")

async def check_voronka_3_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручная проверка воронки 3"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    await update.message.reply_text("🔍 Запускаю ручную проверку воронки 3...")
    await check_funnel_messages(context, 3)
    await update.message.reply_text("✅ Проверка воронки 3 завершена")

async def check_all_voronki_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручная проверка всех воронок"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    await update.message.reply_text("🔍 Запускаю ручную проверку всех воронок...")
    await check_all_funnels(context)
    await update.message.reply_text("✅ Проверка всех воронок завершена")

async def force_funnel_check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принудительная проверка воронок (игнорирует временные ограничения)"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    FUNNELS = funnels_config.get_funnels()
    
    response_text = "🔍 **ПРИНУДИТЕЛЬНАЯ ПРОВЕРКА ВОРОНОК**\n\n"
    
    for funnel_num in [1, 2, 3]:
        threshold = FUNNELS[funnel_num]
        messages = pending_messages_manager.get_all_messages_older_than(threshold)
        
        if funnel_num == 1:
            filtered_messages = [msg for msg in messages if msg.get('current_funnel', 0) == 0]
        elif funnel_num == 2:
            filtered_messages = [msg for msg in messages if 1 in msg.get('funnels_sent', [])]
        else:  # funnel_num == 3
            filtered_messages = [msg for msg in messages if 2 in msg.get('funnels_sent', [])]
        
        # Группируем по чатам
        chats_count = len(set(msg['chat_id'] for msg in filtered_messages))
        
        response_text += f"{get_funnel_emoji(funnel_num)} **Воронка {funnel_num}** ({minutes_to_hours_minutes(threshold)}):\n"
        response_text += f"   📝 Сообщений: {len(filtered_messages)}\n"
        response_text += f"   💬 Чатов: {chats_count}\n\n"
    
    await update.message.reply_text(response_text, parse_mode='Markdown')
    
    # Запускаем фактическую отправку уведомлений
    await check_all_funnels(context)

# ========== КОМАНДЫ ДЛЯ РУЧНОГО УПРАВЛЕНИЯ СООБЩЕНИЯМИ ==========

async def clear_chat_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручное удаление всех сообщений из текущего чата"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    chat_id = update.message.chat.id
    removed_count = pending_messages_manager.remove_all_chat_messages(chat_id)
    
    if removed_count > 0:
        await update.message.reply_text(f"✅ Удалено {removed_count} сообщений из этого чата")
    else:
        await update.message.reply_text("✅ В этом чате нет непрочитанных сообщений")

async def clear_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручное удаление всех сообщений из всех чатов"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    removed_count = pending_messages_manager.clear_all()
    await update.message.reply_text(f"✅ Удалены все непрочитанные сообщения ({removed_count} шт.)")

async def pending_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает все непрочитанные сообщения"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    all_pending = pending_messages_manager.get_all_pending_messages()
    
    if not all_pending:
        await update.message.reply_text("✅ Нет непрочитанных сообщений")
        return
    
    # Группируем по чатам
    chats_messages = {}
    for message in all_pending:
        chat_id = message['chat_id']
        if chat_id not in chats_messages:
            chats_messages[chat_id] = []
        chats_messages[chat_id].append(message)
    
    pending_text = f"📋 **НЕПРОЧИТАННЫЕ СООБЩЕНИЯ**\n\nВсего сообщений: {len(all_pending)}\nЧатов: {len(chats_messages)}\n\n"
    
    for i, (chat_id, messages) in enumerate(chats_messages.items(), 1):
        chat_display = get_chat_display_name(messages[0])
        oldest = min(msg['timestamp'] for msg in messages)
        time_ago = format_time_ago(oldest)
        
        pending_text += f"{i}. {chat_display}\n"
        pending_text += f"   📝 Сообщений: {len(messages)}\n"
        pending_text += f"   ⏰ Самое старое: {time_ago} назад\n\n"
    
    if len(pending_text) > 4000:
        pending_text = pending_text[:4000] + "\n\n... (сообщение обрезано)"
    
    await update.message.reply_text(pending_text, parse_mode='Markdown')

# ========== ОБРАБОТЧИКИ СООБЩЕНИЙ (БЕЗ АВТОМАТИЧЕСКОГО УДАЛЕНИЯ) ==========

async def handle_manager_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ответов менеджеров - БЕЗ удаления сообщений"""
    if not update or not update.message:
        return
        
    username = update.message.from_user.username
    if not is_manager(update.message.from_user.id, username):
        return
        
    if update.message.text and update.message.text.startswith('/'):
        return
    
    chat_id = update.message.chat.id
    logger.info(f"🔍 Менеджер ответил в чате {chat_id}, но сообщения НЕ удаляются")

async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка сообщений в группах"""
    if not update or not update.message:
        return
        
    logger.info(f"📨 Получено групповое сообщение: {update.message.chat.title} - {update.message.text[:50] if update.message.text else '[без текста]'}...")
    
    username = update.message.from_user.username
    if is_manager(update.message.from_user.id, username):
        await handle_manager_reply(update, context)
        return
    
    if not should_respond_to_message(update, context):
        logger.info("❌ Сообщение не требует обработки")
        return
    
    if update.message.chat.type in ['group', 'supergroup']:
        if not is_working_hours():
            chat_id = update.message.chat.id
            replied_key = f'chat_{chat_id}'
            if not flags_manager.has_replied(replied_key):
                await update.message.reply_text(AUTO_REPLY_MESSAGE)
                flags_manager.set_replied(replied_key)
                logger.info(f"✅ Автоответ отправлен в чат {chat_id}")
        else:
            chat_id = update.message.chat.id
            replied_key = f'chat_{chat_id}'
            if flags_manager.has_replied(replied_key):
                flags_manager.clear_replied(replied_key)
            
            # Сохраняем информацию о чате и пользователе
            chat_title = update.message.chat.title
            username = update.message.from_user.username
            first_name = update.message.from_user.first_name
            message_text = update.message.text or update.message.caption or "[Сообщение без текста]"
            
            pending_messages_manager.add_message(
                chat_id=update.message.chat.id,
                user_id=update.message.from_user.id,
                message_text=message_text,
                message_id=update.message.message_id,
                chat_title=chat_title,
                username=username,
                first_name=first_name
            )
            logger.info(f"✅ Добавлено в непрочитанные: чат '{chat_title}', пользователь {update.message.from_user.id}")

async def handle_private_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка личных сообщений"""
    if not update or not update.message:
        return
        
    logger.info(f"📨 Получено личное сообщение от {update.message.from_user.id}: {update.message.text[:50] if update.message.text else '[без текста]'}...")
    
    username = update.message.from_user.username
    if is_manager(update.message.from_user.id, username):
        await handle_manager_reply(update, context)
        return
    
    if not should_respond_to_message(update, context):
        logger.info("❌ Сообщение не требует обработки")
        return
    
    if not is_working_hours():
        user_id = update.message.from_user.id
        replied_key = f'user_{user_id}'
        if not flags_manager.has_replied(replied_key):
            await update.message.reply_text(AUTO_REPLY_MESSAGE)
            flags_manager.set_replied(replied_key)
            logger.info(f"✅ Автоответ отправлен пользователю {user_id}")
    else:
        user_id = update.message.from_user.id
        replied_key = f'user_{user_id}'
        if flags_manager.has_replied(replied_key):
            flags_manager.clear_replied(replied_key)
        
        # Сохраняем информацию о пользователе
        username = update.message.from_user.username
        first_name = update.message.from_user.first_name
        message_text = update.message.text or update.message.caption or "[Сообщение без текста]"
        
        pending_messages_manager.add_message(
            chat_id=update.message.chat.id,
            user_id=update.message.from_user.id,
            message_text=message_text,
            message_id=update.message.message_id,
            username=username,
            first_name=first_name
        )
        logger.info(f"✅ Добавлено в непрочитанные: пользователь {first_name or username or user_id}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"💥 Ошибка при обработке сообщения: {context.error}")
    
    # Добавляем дополнительную информацию об ошибке
    if update:
        logger.error(f"💥 Update object: {update}")
        if update.message:
            logger.error(f"💥 Message info: chat_id={update.message.chat.id}, user_id={update.message.from_user.id if update.message.from_user else 'None'}")
    
    # Можно добавить отправку уведомления администратору об ошибке
    try:
        for admin_id in ADMIN_IDS:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"💥 Произошла ошибка в боте:\n\n{context.error}\n\nUpdate: {update}"
            )
    except Exception as e:
        logger.error(f"❌ Не удалось отправить уведомление об ошибке: {e}")

# ========== ЗАПУСК БОТА ==========

def main():
    try:
        # Выводим информацию о запуске
        print("=" * 50)
        print("🤖 ЗАПУСК БОТА-АВТООТВЕТЧИКА (СООБЩЕНИЯ НЕ УДАЛЯЮТСЯ АВТОМАТИЧЕСКИ)")
        print("=" * 50)
        
        application = Application.builder().token(BOT_TOKEN).build()
        
        # Команды для управления воронками
        application.add_handler(CommandHandler("funnels", funnels_command))
        application.add_handler(CommandHandler("set_funnel_1", set_funnel_1_command))
        application.add_handler(CommandHandler("set_funnel_2", set_funnel_2_command))
        application.add_handler(CommandHandler("set_funnel_3", set_funnel_3_command))
        application.add_handler(CommandHandler("reset_funnels", reset_funnels_command))
        
        # Команды для ручной проверки воронок
        application.add_handler(CommandHandler("check_voronka_1", check_voronka_1_command))
        application.add_handler(CommandHandler("check_voronka_2", check_voronka_2_command))
        application.add_handler(CommandHandler("check_voronka_3", check_voronka_3_command))
        application.add_handler(CommandHandler("check_all_voronki", check_all_voronki_command))
        application.add_handler(CommandHandler("force_funnel_check", force_funnel_check_command))
        
        # Команды для управления исключениями
        application.add_handler(CommandHandler("add_exception", add_exception_command))
        application.add_handler(CommandHandler("remove_exception", remove_exception_command))
        application.add_handler(CommandHandler("list_exceptions", list_exceptions_command))
        application.add_handler(CommandHandler("clear_exceptions", clear_exceptions_command))
        
        # Команды для ручного управления сообщениями
        application.add_handler(CommandHandler("clear_chat", clear_chat_command))
        application.add_handler(CommandHandler("clear_all", clear_all_command))
        application.add_handler(CommandHandler("pending", pending_command))
        
        # Основные команды
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("status", status_command))
        application.add_handler(CommandHandler("set_work_chat", set_work_chat_command))
        application.add_handler(CommandHandler("managers", managers_command))
        application.add_handler(CommandHandler("stats", stats_command))
        
        # Обработчики сообщений - РАСШИРЕННЫЕ ФИЛЬТРЫ
        application.add_handler(MessageHandler(
            filters.TEXT | filters.CAPTION | filters.PHOTO | filters.Document.ALL, 
            handle_group_message,
            block=False
        ))
        application.add_handler(MessageHandler(
            filters.TEXT | filters.CAPTION | filters.PHOTO | filters.Document.ALL,
            handle_private_message, 
            block=False
        ))
        
        # Обработчик ошибок
        application.add_error_handler(error_handler)
        
        # Периодическая проверка воронок (каждые 5 минут)
        job_queue = application.job_queue
        if job_queue:
            job_queue.run_repeating(check_all_funnels, interval=300, first=10)  # 5 минут
            print("✅ Планировщик задач запущен (интервал: 5 минут)")
        else:
            print("❌ Планировщик задач недоступен")
        
        # Запуск
        FUNNELS = funnels_config.get_funnels()
        excluded_users = excluded_users_manager.get_all_excluded()
        total_excluded = len(excluded_users["user_ids"]) + len(excluded_users["usernames"])
        
        print("🚀 Бот запускается...")
        print(f"📊 Загружено флагов: {flags_manager.count_flags()}")
        print(f"📋 Непрочитанных сообщений: {len(pending_messages_manager.get_all_pending_messages())}")
        print(f"👥 Менеджеров в системе: {total_excluded} ({len(excluded_users['user_ids'])} ID + {len(excluded_users['usernames'])} username)")
        print(f"⚙️ Воронки уведомлений:")
        for funnel_num, minutes in FUNNELS.items():
            print(f"   - Воронка {funnel_num}: {minutes} мин ({minutes_to_hours_minutes(minutes)})")
        
        if work_chat_manager.is_work_chat_set():
            print(f"💬 Рабочий чат установлен: {work_chat_manager.get_work_chat_id()}")
        else:
            print("⚠️ Рабочий чат не установлен! Используйте /set_work_chat")
        
        print("⏰ Ожидание сообщений...")
        print("=" * 50)
        
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False
        )
        
    except Exception as e:
        print(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logger.error(f"💥 Критическая ошибка при запуске бота: {e}")

if __name__ == "__main__":
    main()
