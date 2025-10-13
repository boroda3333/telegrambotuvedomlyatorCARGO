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
BOT_TOKEN = os.environ.get('BOT_TOKEN', '7952222222:AAHNNBA5OnoQrblwY4BO0BoETb-9jZg_z_g')

# Таймзона Москвы
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Сообщение для автоответа
AUTO_REPLY_MESSAGE = """Здравствуйте, вы написали в нерабочее время компании!

Мы отвечаем с понедельника по пятницу | c 10:00 до 19:00 по МСК

**сообщение автоматическое, отвечать на него не нужно**"""

# ID администраторов
ADMIN_IDS = {7842709072, 1772492746, 1661202178, 478084322}

# Файлы для сохранения данных
FLAGS_FILE = "auto_reply_flags.json"
WORK_CHAT_FILE = "work_chat.json"
PENDING_MESSAGES_FILE = "pending_messages.json"
FUNNELS_CONFIG_FILE = "funnels_config.json"
EXCLUDED_USERS_FILE = "excluded_users.json"
FUNNELS_STATE_FILE = "funnels_state.json"
MASTER_NOTIFICATION_FILE = "master_notification.json"

# ========== КЛАСС ДЛЯ УПРАВЛЕНИЯ ГЛАВНЫМ УВЕДОМЛЕНИЕМ ==========

class MasterNotificationManager:
    def __init__(self):
        self.data = self.load_data()
        self.last_notification_time = None
        self.notification_cooldown = 900  # 15 минут в секундах
    
    def load_data(self) -> Dict[str, Any]:
        """Загружает данные главного уведомления из файла"""
        try:
            if os.path.exists(MASTER_NOTIFICATION_FILE):
                with open(MASTER_NOTIFICATION_FILE, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки главного уведомления: {e}")
        return {"message_ids": [], "last_update": None}
    
    def save_data(self):
        """Сохраняет данные главного уведомления в файл"""
        try:
            with open(MASTER_NOTIFICATION_FILE, 'w') as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения главного уведомления: {e}")
    
    def add_message_id(self, message_id: int):
        """Добавляет ID сообщения в список"""
        if "message_ids" not in self.data:
            self.data["message_ids"] = []
        
        self.data["message_ids"].append(message_id)
        self.data["last_update"] = datetime.now(MOSCOW_TZ).isoformat()
        self.save_data()
        logger.info(f"✅ Добавлен ID уведомления: {message_id}")
    
    def get_message_ids(self) -> List[int]:
        """Возвращает список ID сообщений уведомлений"""
        return self.data.get("message_ids", [])
    
    def clear_old_messages(self, keep_last: int = 3):
        """Очищает старые сообщения, оставляя только последние"""
        if "message_ids" in self.data and len(self.data["message_ids"]) > keep_last:
            # Оставляем только последние keep_last сообщений
            self.data["message_ids"] = self.data["message_ids"][-keep_last:]
            self.save_data()
    
    def should_update(self) -> bool:
        """Проверяет, нужно ли обновлять уведомление (каждые 15 минут)"""
        # Если никогда не отправляли - отправляем
        if not self.last_notification_time:
            return True
        
        now = datetime.now(MOSCOW_TZ)
        time_diff = now - self.last_notification_time
        
        return time_diff.total_seconds() >= self.notification_cooldown
    
    def update_notification_time(self):
        """Обновляет время последней отправки уведомления"""
        self.last_notification_time = datetime.now(MOSCOW_TZ)
        logger.info(f"🕐 Обновлено время уведомления: {self.last_notification_time.strftime('%H:%M:%S')}")

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
        return datetime.now(MOSCOW_TZ) - timedelta(days=1)
    
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
        if user_id in self.excluded_users["user_ids"]:
            return True
        
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
        """Загружает конфигурацию воронок из файла или использует значения по умолчания"""
        try:
            if os.path.exists(FUNNELS_CONFIG_FILE):
                with open(FUNNELS_CONFIG_FILE, 'r') as f:
                    data = json.load(f)
                    return {int(k): v for k, v in data.items()}
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации воронок: {e}")
        
        return {
            1: 60,    # 1 час
            2: 180,   # 3 часа  
            3: 300    # 5 часов
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
        self.funnels = {1: 60, 2: 180, 3: 300}
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
        key = f"{chat_id}_{user_id}_{message_id}_{int(datetime.now().timestamp())}"
        
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
            'current_funnel': 0,
            'message_key': key
        }
        self.save_pending_messages()
        logger.info(f"✅ Добавлено непрочитанное сообщение: {key}")
    
    def remove_message_by_key(self, key: str):
        if key in self.pending_messages:
            del self.pending_messages[key]
            self.save_pending_messages()
            logger.info(f"✅ Удалено непрочитанное сообщение: {key}")
            return True
        return False
    
    def remove_all_chat_messages(self, chat_id: int, user_id: int = None):
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
        if message_key in self.pending_messages:
            if funnel_number not in self.pending_messages[message_key]['funnels_sent']:
                self.pending_messages[message_key]['funnels_sent'].append(funnel_number)
                self.pending_messages[message_key]['current_funnel'] = funnel_number
                self.save_pending_messages()
    
    def find_messages_by_chat(self, chat_id: int) -> List[Dict[str, Any]]:
        result = []
        for message in self.pending_messages.values():
            if message['chat_id'] == chat_id:
                result.append(message)
        return result
    
    def get_messages_for_funnel(self, funnel_number: int, funnels_state: FunnelsStateManager) -> List[Dict[str, Any]]:
        """Получает сообщения для указанной воронки - СТРОГАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ"""
        result = []
        now = datetime.now(MOSCOW_TZ)
        FUNNELS = self.funnels_config.get_funnels()
        funnel_minutes = FUNNELS[funnel_number]
        
        for message_key, message in self.pending_messages.items():
            if funnels_state.is_message_processed(funnel_number, message_key):
                continue
                
            timestamp = datetime.fromisoformat(message['timestamp'])
            time_diff = now - timestamp
            minutes_passed = int(time_diff.total_seconds() / 60)
            
            current_funnel = message.get('current_funnel', 0)
            funnels_sent = message.get('funnels_sent', [])
            
            # СТРОГАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ ВОРОНОК
            if funnel_number == 1:
                # Воронка 1: должно пройти больше 1 часа и не быть в других воронках
                if (minutes_passed >= funnel_minutes and 
                    current_funnel == 0 and 
                    funnel_number not in funnels_sent):
                    message['message_key'] = message_key
                    message['minutes_passed'] = minutes_passed
                    result.append(message)
                    
            elif funnel_number == 2:
                # Воронка 2: должно пройти больше 3 часов И воронка 1 уже была отправлена
                if (minutes_passed >= funnel_minutes and 
                    1 in funnels_sent and 
                    current_funnel == 1 and
                    funnel_number not in funnels_sent):
                    message['message_key'] = message_key
                    message['minutes_passed'] = minutes_passed
                    result.append(message)
                    
            elif funnel_number == 3:
                # Воронка 3: должно пройти больше 5 часов И воронка 2 уже была отправлена
                if (minutes_passed >= funnel_minutes and 
                    2 in funnels_sent and 
                    current_funnel == 2 and
                    funnel_number not in funnels_sent):
                    message['message_key'] = message_key
                    message['minutes_passed'] = minutes_passed
                    result.append(message)
        
        return result
    
    def update_funnel_statuses(self):
        """Автоматически обновляет статусы воронок на основе времени - СТРОГАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ"""
        updated_count = 0
        now = datetime.now(MOSCOW_TZ)
        FUNNELS = self.funnels_config.get_funnels()
        
        for message_key, message in self.pending_messages.items():
            timestamp = datetime.fromisoformat(message['timestamp'])
            time_diff = now - timestamp
            minutes_passed = int(time_diff.total_seconds() / 60)
            
            current_funnel = message.get('current_funnel', 0)
            funnels_sent = message.get('funnels_sent', [])
            
            # Определяем следующую воронку на основе времени и предыдущих воронок
            next_funnel = current_funnel
            
            if current_funnel == 0 and minutes_passed >= FUNNELS[1]:
                next_funnel = 1
            elif current_funnel == 1 and minutes_passed >= FUNNELS[2]:
                next_funnel = 2
            elif current_funnel == 2 and minutes_passed >= FUNNELS[3]:
                next_funnel = 3
            
            # Обновляем статус только если воронка изменилась
            if next_funnel != current_funnel:
                # Добавляем предыдущие воронки в список отправленных
                for funnel_num in range(1, next_funnel):
                    if funnel_num not in funnels_sent:
                        funnels_sent.append(funnel_num)
                
                self.pending_messages[message_key]['current_funnel'] = next_funnel
                self.pending_messages[message_key]['funnels_sent'] = funnels_sent
                updated_count += 1
                logger.info(f"🔄 Сообщение {message_key} перешло из воронки {current_funnel} в воронку {next_funnel} ({minutes_passed} минут прошло)")
        
        if updated_count > 0:
            self.save_pending_messages()
            logger.info(f"✅ Обновлено статусов воронок: {updated_count} сообщений")
        
        return updated_count
    
    def get_all_messages_older_than(self, minutes_threshold: int) -> List[Dict[str, Any]]:
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
master_notification_manager = MasterNotificationManager()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def is_manager(user_id: int, username: str = None) -> bool:
    return excluded_users_manager.is_user_excluded(user_id, username)

def is_excluded_user(user_id: int) -> bool:
    return excluded_users_manager.is_user_excluded(user_id)

def is_working_hours():
    now = datetime.now(MOSCOW_TZ)
    current_time = now.time()
    if current_time >= time(10, 0) and current_time <= time(19, 0):
        return True
    return False

def should_respond_to_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
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
        
    if update.message.text and len(update.message.text.strip()) < 1:
        return False
        
    return True

def get_chat_display_name(chat_data: Dict[str, Any]) -> str:
    chat_title = chat_data.get('chat_title')
    if chat_title:
        return chat_title
    else:
        return f"Чат {chat_data['chat_id']}"

def get_funnel_emoji(funnel_number: int) -> str:
    emojis = {1: "🟡", 2: "🟠", 3: "🔴"}
    return emojis.get(funnel_number, "⚪")

def format_time_ago(timestamp: str) -> str:
    message_time = datetime.fromisoformat(timestamp)
    now = datetime.now(MOSCOW_TZ)
    time_diff = now - message_time
    
    total_minutes = int(time_diff.total_seconds() / 60)
    hours = total_minutes // 60
    minutes = total_minutes % 60
    
    if hours > 0:
        return f"{hours}ч {minutes}м"
    else:
        return f"{minutes}м"

def minutes_to_hours_text(minutes: int) -> str:
    hours = minutes // 60
    if hours == 1:
        return "1 ЧАС"
    elif hours == 3:
        return "3 ЧАСА"
    elif hours == 5:
        return "5 ЧАСОВ"
    else:
        return f"{hours} ЧАСОВ"

# ========== ФУНКЦИИ АВТОМАТИЧЕСКОГО ОБНОВЛЕНИЯ ВОРОНОК ==========

async def update_message_funnel_statuses():
    """Автоматически обновляет статусы воронок для всех сообщений - СТРОГАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ"""
    logger.info("🔄 Автоматическое обновление статусов воронок...")
    return pending_messages_manager.update_funnel_statuses()

async def mark_messages_as_processed():
    """Помечает все сообщения в текущих воронках как обработанные"""
    processed_count = 0
    
    # Помечаем сообщения для каждой воронки как обработанные
    for funnel_number in [1, 2, 3]:
        messages = pending_messages_manager.get_messages_for_funnel(funnel_number, funnels_state_manager)
        for message in messages:
            message_key = message.get('message_key')
            if message_key:
                # Помечаем воронку как отправленную
                pending_messages_manager.mark_funnel_sent(message_key, funnel_number)
                # Помечаем сообщение как обработанное для этой воронки
                funnels_state_manager.add_processed_message(funnel_number, message_key)
                processed_count += 1
                logger.info(f"✅ Сообщение {message_key} помечено как обработанное в воронке {funnel_number}")
    
    if processed_count > 0:
        logger.info(f"✅ Помечено {processed_count} сообщений как обработанные")
    
    return processed_count

# ========== СИСТЕМА ЕДИНОГО УВЕДОМЛЕНИЯ ==========

def create_master_notification_text() -> str:
    """Создает текст единого уведомления со всеми воронками"""
    FUNNELS = funnels_config.get_funnels()
    
    # Получаем сообщения для каждой воронки
    funnel_1_messages = pending_messages_manager.get_messages_for_funnel(1, funnels_state_manager)
    funnel_2_messages = pending_messages_manager.get_messages_for_funnel(2, funnels_state_manager)
    funnel_3_messages = pending_messages_manager.get_messages_for_funnel(3, funnels_state_manager)
    
    # Группируем сообщения по чатам для каждой воронки
    funnel_1_chats = {}
    funnel_2_chats = {}
    funnel_3_chats = {}
    
    for msg in funnel_1_messages:
        chat_id = msg['chat_id']
        if chat_id not in funnel_1_chats:
            funnel_1_chats[chat_id] = {
                'chat_info': msg,
                'message_count': 0,
                'oldest_time': msg['timestamp']
            }
        funnel_1_chats[chat_id]['message_count'] += 1
        # Обновляем самое старое время
        if msg['timestamp'] < funnel_1_chats[chat_id]['oldest_time']:
            funnel_1_chats[chat_id]['oldest_time'] = msg['timestamp']
    
    for msg in funnel_2_messages:
        chat_id = msg['chat_id']
        if chat_id not in funnel_2_chats:
            funnel_2_chats[chat_id] = {
                'chat_info': msg,
                'message_count': 0,
                'oldest_time': msg['timestamp']
            }
        funnel_2_chats[chat_id]['message_count'] += 1
        # Обновляем самое старое время
        if msg['timestamp'] < funnel_2_chats[chat_id]['oldest_time']:
            funnel_2_chats[chat_id]['oldest_time'] = msg['timestamp']
    
    for msg in funnel_3_messages:
        chat_id = msg['chat_id']
        if chat_id not in funnel_3_chats:
            funnel_3_chats[chat_id] = {
                'chat_info': msg,
                'message_count': 0,
                'oldest_time': msg['timestamp']
            }
        funnel_3_chats[chat_id]['message_count'] += 1
        # Обновляем самое старое время
        if msg['timestamp'] < funnel_3_chats[chat_id]['oldest_time']:
            funnel_3_chats[chat_id]['oldest_time'] = msg['timestamp']
    
    # Создаем текст уведомления
    notification_text = "📊 **ОБЗОР НЕОТВЕЧЕННЫХ СООБЩЕНИЙ**\n\n"
    
    # Воронка 1
    notification_text += f"🟡 {minutes_to_hours_text(FUNNELS[1])} без ответа\n"
    if funnel_1_chats:
        for chat_id, chat_data in funnel_1_chats.items():
            chat_display = get_chat_display_name(chat_data['chat_info'])
            message_count = chat_data['message_count']
            time_ago = format_time_ago(chat_data['oldest_time'])
            notification_text += f"  • {chat_display} ({message_count} сообщ., {time_ago} назад)\n"
    else:
        notification_text += "  Таких нет\n"
    notification_text += "\n"
    
    # Воронка 2
    notification_text += f"🟠 {minutes_to_hours_text(FUNNELS[2])} без ответа\n"
    if funnel_2_chats:
        for chat_id, chat_data in funnel_2_chats.items():
            chat_display = get_chat_display_name(chat_data['chat_info'])
            message_count = chat_data['message_count']
            time_ago = format_time_ago(chat_data['oldest_time'])
            notification_text += f"  • {chat_display} ({message_count} сообщ., {time_ago} назад)\n"
    else:
        notification_text += "  Таких нет\n"
    notification_text += "\n"
    
    # Воронка 3
    notification_text += f"🔴 БОЛЕЕ {minutes_to_hours_text(FUNNELS[3])} без ответа\n"
    if funnel_3_chats:
        for chat_id, chat_data in funnel_3_chats.items():
            chat_display = get_chat_display_name(chat_data['chat_info'])
            message_count = chat_data['message_count']
            time_ago = format_time_ago(chat_data['oldest_time'])
            notification_text += f"  • {chat_display} ({message_count} сообщ., {time_ago} назад)\n"
    else:
        notification_text += "  Таких нет\n"
    
    # Добавляем общую статистику
    total_messages = len(pending_messages_manager.get_all_pending_messages())
    total_chats = len(set(msg['chat_id'] for msg in pending_messages_manager.get_all_pending_messages()))
    
    notification_text += f"\n📈 **ИТОГО:** {total_messages} сообщений в {total_chats} чатах"
    notification_text += f"\n⏰ Обновлено: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}"
    
    return notification_text

async def delete_old_notifications(context: ContextTypes.DEFAULT_TYPE):
    """Удаляет старые уведомления"""
    work_chat_id = work_chat_manager.get_work_chat_id()
    if not work_chat_id:
        return
    
    try:
        message_ids = master_notification_manager.get_message_ids()
        for message_id in message_ids:
            try:
                await context.bot.delete_message(
                    chat_id=work_chat_id,
                    message_id=message_id
                )
                logger.info(f"✅ Удалено старое уведомление: {message_id}")
            except Exception as e:
                logger.warning(f"❌ Не удалось удалить сообщение {message_id}: {e}")
        
        # Очищаем список сообщений после удаления
        master_notification_manager.data["message_ids"] = []
        master_notification_manager.save_data()
        
    except Exception as e:
        logger.error(f"❌ Ошибка при удалении старых уведомлений: {e}")

async def send_new_master_notification(context: ContextTypes.DEFAULT_TYPE, force: bool = False):
    """Отправляет новое уведомление (удаляет старые и отправляет новое)"""
    work_chat_id = work_chat_manager.get_work_chat_id()
    if not work_chat_id:
        logger.error("❌ Не могу отправить уведомление: рабочий чат не установлен")
        return False
    
    # Проверяем cooldown, если не форсированная отправка
    if not force and not master_notification_manager.should_update():
        logger.info("⏳ Cooldown: уведомление не отправляется (еще не прошло 15 минут)")
        return False
    
    try:
        # Сначала удаляем старые уведомления
        await delete_old_notifications(context)
        
        # Затем отправляем новое
        notification_text = create_master_notification_text()
        
        sent_message = await context.bot.send_message(
            chat_id=work_chat_id,
            text=notification_text,
            parse_mode='Markdown'
        )
        
        # Сохраняем ID нового сообщения
        master_notification_manager.add_message_id(sent_message.message_id)
        
        # ПОМЕЧАЕМ СООБЩЕНИЯ КАК ОБРАБОТАННЫЕ ПОСЛЕ ОТПРАВКИ УВЕДОМЛЕНИЯ
        processed_count = await mark_messages_as_processed()
        logger.info(f"📝 После отправки уведомления помечено {processed_count} сообщений как обработанные")
        
        # Обновляем время последней отправки
        master_notification_manager.update_notification_time()
        
        # Очищаем старые сообщения (оставляем только последние 3)
        master_notification_manager.clear_old_messages(keep_last=3)
        
        logger.info("✅ Отправлено новое единое уведомление")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки нового уведомления: {e}")
        return False

async def check_and_send_new_notification(context: ContextTypes.DEFAULT_TYPE):
    """Проверяет и отправляет новое уведомление каждые 15 минут с автоматическим обновлением статусов"""
    logger.info("🔄 Проверка необходимости отправки уведомления...")
    
    # СНАЧАЛА ОБНОВЛЯЕМ СТАТУСЫ ВСЕХ СООБЩЕНИЙ
    updated_count = await update_message_funnel_statuses()
    if updated_count > 0:
        logger.info(f"🔄 Обновлено {updated_count} статусов воронок перед отправкой уведомления")
    
    # ПОТОМ ОТПРАВЛЯЕМ УВЕДОМЛЕНИЕ
    await send_new_master_notification(context)

# ========== ОБРАБОТЧИК ОТВЕТОВ МЕНЕДЖЕРА ==========

async def handle_manager_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ответы менеджеров и обновляет уведомление"""
    if not update or not update.message:
        return
        
    username = update.message.from_user.username
    if not is_manager(update.message.from_user.id, username):
        return
        
    if update.message.text and update.message.text.startswith('/'):
        return
    
    chat_id = update.message.chat.id
    logger.info(f"🔍 Менеджер ответил в чате {chat_id}")
    
    # Удаляем сообщения из pending для этого чата
    removed_count = pending_messages_manager.remove_all_chat_messages(chat_id)
    
    if removed_count > 0:
        logger.info(f"✅ Удалено {removed_count} сообщений из чата {chat_id} после ответа менеджера")
        
        # Немедленно отправляем новое уведомление (форсированно)
        await send_new_master_notification(context, force=True)

# ========== КОМАНДЫ БОТА ==========

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        "/help - помощь\n"
        "/update_notification - обновить уведомление\n"
        "/force_update_funnels - принудительно обновить воронки\n"
        "/debug_funnels - отладка воронок\n\n"
        "👥 **Управление исключениями:**\n"
        "/add_exception - добавить исключение\n"
        "/remove_exception - удалить исключение\n"
        "/list_exceptions - список исключений\n"
        "/clear_exceptions - очистить все исключения"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
/force_update_funnels - принудительно обновить статусы воронок
/debug_funnels - отладка воронок

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

**Обновление уведомления:**
/update_notification - обновить единое уведомление

**Статистика:**
/stats - статистика системы
/managers - список менеджеров

📝 **Логика работы воронок:**
🟡 Воронка 1: через 1 час без ответа
🟠 Воронка 2: через 3 часа без ответа (только после воронки 1)
🔴 Воронка 3: через 5 часов без ответа (только после воронки 2)
**СТРОГАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ** - сообщения проходят все воронки по очереди
    """
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    FUNNELS = funnels_config.get_funnels()
    now = datetime.now(MOSCOW_TZ)
    excluded_users = excluded_users_manager.get_all_excluded()
    total_excluded = len(excluded_users["user_ids"]) + len(excluded_users["usernames"])
    
    # Получаем статистику по воронкам
    funnel_1_count = len(pending_messages_manager.get_messages_for_funnel(1, funnels_state_manager))
    funnel_2_count = len(pending_messages_manager.get_messages_for_funnel(2, funnels_state_manager))
    funnel_3_count = len(pending_messages_manager.get_messages_for_funnel(3, funnels_state_manager))
    
    # Время последнего уведомления
    last_notification = master_notification_manager.last_notification_time
    last_notification_str = last_notification.strftime('%H:%M:%S') if last_notification else "Никогда"
    
    status_text = f"""
📊 **СТАТУС СИСТЕМЫ**

⏰ **Время:** {now.strftime('%d.%m.%Y %H:%M:%S')}
🕐 **Рабочие часы:** {'✅ ДА' if is_working_hours() else '❌ НЕТ'}

📋 **Непрочитанные сообщения:** {len(pending_messages_manager.get_all_pending_messages())}
🚩 **Флаги автоответов:** {flags_manager.count_flags()}
💬 **Рабочий чат:** {'✅ Установлен' if work_chat_manager.is_work_chat_set() else '❌ Не установлен'}
📢 **Последнее уведомление:** {last_notification_str}

⚙️ **НАСТРОЙКИ ВОРОНОК:**
🟡 Воронка 1: {FUNNELS[1]} мин ({minutes_to_hours_text(FUNNELS[1])}) - {funnel_1_count} сообщ.
🟠 Воронка 2: {FUNNELS[2]} мин ({minutes_to_hours_text(FUNNELS[2])}) - {funnel_2_count} сообщ.
🔴 Воронка 3: {FUNNELS[3]} мин ({minutes_to_hours_text(FUNNELS[3])}) - {funnel_3_count} сообщ.

👥 **Менеджеров в системе:** {total_excluded} ({len(excluded_users["user_ids"])} ID + {len(excluded_users["usernames"])} username)

🔄 **Логика уведомлений:** Удаление старого + отправка нового каждые 15 минут
⏳ **Cooldown:** {'✅ Активен' if not master_notification_manager.should_update() else '❌ Можно отправлять'}
🔒 **Последовательность:** ✅ Строгая (1→2→3)
    """
    
    await update.message.reply_text(status_text, parse_mode='Markdown')

async def funnels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    FUNNELS = funnels_config.get_funnels()
    
    funnels_text = f"""
⚙️ **ТЕКУЩИЕ НАСТРОЙКИ ВОРОНОК**

🟡 **Воронка 1 (начальное уведомление):**
   - Интервал: {FUNNELS[1]} минут ({minutes_to_hours_text(FUNNELS[1])})
   - Команда: `/set_funnel_1 <минуты>`

🟠 **Воронка 2 (повторное уведомление):**
   - Интервал: {FUNNELS[2]} минут ({minutes_to_hours_text(FUNNELS[2])})
   - Команда: `/set_funnel_2 <минуты>`

🔴 **Воронка 3 (срочное уведомление):**
   - Интервал: {FUNNELS[3]} минут ({minutes_to_hours_text(FUNNELS[3])})
   - Команда: `/set_funnel_3 <минуты>`

🔄 Сбросить настройки: `/reset_funnels`
🚀 Принудительное обновление: `/force_update_funnels`
🐛 Отладка: `/debug_funnels`

📝 **Логика работы:**
Единое уведомление обновляется каждые 15 минут
**СТАРОЕ УДАЛЯЕТСЯ, ОТПРАВЛЯЕТСЯ НОВОЕ**
**COOLDOWN 15 МИНУТ** - защита от частых отправок
**СТРОГАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ** - сообщения проходят все воронки по очереди
    """
    
    await update.message.reply_text(funnels_text, parse_mode='Markdown')

async def set_funnel_1_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await update.message.reply_text(f"✅ Воронка 1 установлена на {minutes} минут ({minutes_to_hours_text(minutes)})")
        logger.info("✅ Настройки воронки 1 обновлены")
    else:
        await update.message.reply_text("❌ Ошибка установки интервала воронки")

async def set_funnel_2_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await update.message.reply_text(f"✅ Воронка 2 установлена на {minutes} минут ({minutes_to_hours_text(minutes)})")
        logger.info("✅ Настройки воронки 2 обновлены")
    else:
        await update.message.reply_text("❌ Ошибка установки интервала воронки")

async def set_funnel_3_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await update.message.reply_text(f"✅ Воронка 3 установлена на {minutes} минут ({minutes_to_hours_text(minutes)})")
        logger.info("✅ Настройки воронки 3 обновлены")
    else:
        await update.message.reply_text("❌ Ошибка установки интервала воронки")

async def reset_funnels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    funnels_config.reset_to_default()
    await update.message.reply_text("✅ Настройки воронок сброшены к значениям по умолчанию")
    logger.info("✅ Настройки воронок сброшены")

async def force_update_funnels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принудительно обновляет статусы воронок для всех сообщений"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    await update.message.reply_text("🔄 Принудительное обновление статусов воронок...")
    
    updated_count = await update_message_funnel_statuses()
    
    if updated_count > 0:
        await update.message.reply_text(f"✅ Обновлено статусов воронок: {updated_count} сообщений")
        # Сразу отправляем обновленное уведомление
        await send_new_master_notification(context, force=True)
    else:
        await update.message.reply_text("ℹ️ Не требуется обновление статусов воронок")

async def debug_funnels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для отладки воронок"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    # Получаем все сообщения для каждой воронки
    FUNNELS = funnels_config.get_funnels()
    funnel_1_messages = pending_messages_manager.get_messages_for_funnel(1, funnels_state_manager)
    funnel_2_messages = pending_messages_manager.get_messages_for_funnel(2, funnels_state_manager)
    funnel_3_messages = pending_messages_manager.get_messages_for_funnel(3, funnels_state_manager)
    
    debug_text = "🐛 **ОТЛАДКА ВОРОНОК**\n\n"
    
    debug_text += f"🟡 Воронка 1 ({FUNNELS[1]} мин): {len(funnel_1_messages)} сообщ.\n"
    for msg in funnel_1_messages:
        chat_display = get_chat_display_name(msg)
        time_ago = format_time_ago(msg['timestamp'])
        debug_text += f"   - {chat_display} ({time_ago} назад)\n"
        debug_text += f"     Текст: {msg['message_text'][:50]}...\n"
        debug_text += f"     current_funnel: {msg.get('current_funnel', 0)}, funnels_sent: {msg.get('funnels_sent', [])}\n"
    
    debug_text += f"\n🟠 Воронка 2 ({FUNNELS[2]} мин): {len(funnel_2_messages)} сообщ.\n"
    for msg in funnel_2_messages:
        chat_display = get_chat_display_name(msg)
        time_ago = format_time_ago(msg['timestamp'])
        debug_text += f"   - {chat_display} ({time_ago} назад)\n"
        debug_text += f"     Текст: {msg['message_text'][:50]}...\n"
        debug_text += f"     current_funnel: {msg.get('current_funnel', 0)}, funnels_sent: {msg.get('funnels_sent', [])}\n"
    
    debug_text += f"\n🔴 Воронка 3 ({FUNNELS[3]} мин): {len(funnel_3_messages)} сообщ.\n"
    for msg in funnel_3_messages:
        chat_display = get_chat_display_name(msg)
        time_ago = format_time_ago(msg['timestamp'])
        debug_text += f"   - {chat_display} ({time_ago} назад)\n"
        debug_text += f"     Текст: {msg['message_text'][:50]}...\n"
        debug_text += f"     current_funnel: {msg.get('current_funnel', 0)}, funnels_sent: {msg.get('funnels_sent', [])}\n"
    
    # Показываем статусы обработки
    debug_text += f"\n📊 Статусы обработки:\n"
    for funnel_num in [1, 2, 3]:
        processed_count = len(funnels_state_manager.state.get(f"funnel_{funnel_num}_messages_processed", []))
        debug_text += f"   Воронка {funnel_num}: {processed_count} обработано\n"
    
    # Показываем все pending сообщения
    all_pending = pending_messages_manager.get_all_pending_messages()
    debug_text += f"\n📋 Всего в pending: {len(all_pending)} сообщений\n"
    for msg in all_pending:
        chat_display = get_chat_display_name(msg)
        time_ago = format_time_ago(msg['timestamp'])
        debug_text += f"   - {chat_display} ({time_ago} назад): current_funnel={msg.get('current_funnel', 0)}, funnels_sent={msg.get('funnels_sent', [])}\n"
    
    if len(debug_text) > 4000:
        debug_text = debug_text[:4000] + "\n\n... (сообщение обрезано)"
    
    await update.message.reply_text(debug_text, parse_mode='Markdown')

async def update_notification_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для ручного обновления уведомления"""
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    await update.message.reply_text("🔄 Обновляю единое уведомление...")
    success = await send_new_master_notification(context, force=True)
    
    if success:
        await update.message.reply_text("✅ Единое уведомление обновлено")
    else:
        await update.message.reply_text("❌ Ошибка обновления уведомления")

async def set_work_chat_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    chat_id = update.message.chat.id
    if work_chat_manager.save_work_chat(chat_id):
        await update.message.reply_text(f"✅ Этот чат установлен как рабочий (ID: {chat_id})")
        # Сразу отправляем уведомление в новый рабочий чат (форсированно)
        await send_new_master_notification(context, force=True)
    else:
        await update.message.reply_text("❌ Ошибка сохранения рабочего чата")

async def managers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    all_pending = pending_messages_manager.get_all_pending_messages()
    excluded_users = excluded_users_manager.get_all_excluded()
    total_excluded = len(excluded_users["user_ids"]) + len(excluded_users["usernames"])
    
    now = datetime.now(MOSCOW_TZ)
    time_stats = {"менее 1 часа": 0, "1-3 часа": 0, "3-6 часов": 0, "более 6 часов": 0}
    
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
    
    # Статистика по воронкам
    funnel_1_count = len(pending_messages_manager.get_messages_for_funnel(1, funnels_state_manager))
    funnel_2_count = len(pending_messages_manager.get_messages_for_funnel(2, funnels_state_manager))
    funnel_3_count = len(pending_messages_manager.get_messages_for_funnel(3, funnels_state_manager))
    
    # Время последнего уведомления
    last_notification = master_notification_manager.last_notification_time
    last_notification_str = last_notification.strftime('%H:%M:%S') if last_notification else "Никогда"
    
    stats_text = f"""
📈 **СТАТИСТИКА СИСТЕМЫ**

📊 **Общая статистика:**
   - Непрочитанных сообщений: {len(all_pending)}
   - Флагов автоответов: {flags_manager.count_flags()}
   - Менеджеров в системе: {total_excluded} ({len(excluded_users["user_ids"])} ID + {len(excluded_users["usernames"])} username)
   - Последнее уведомление: {last_notification_str}

⚙️ **Статистика воронок:**
   - 🟡 Воронка 1: {funnel_1_count} сообщений
   - 🟠 Воронка 2: {funnel_2_count} сообщений  
   - 🔴 Воронка 3: {funnel_3_count} сообщений

⏱ **Время ожидания ответа:**
   - Менее 1 часа: {time_stats['менее 1 часа']}
   - 1-3 часа: {time_stats['1-3 часа']}
   - 3-6 часов: {time_stats['3-6 часов']}
   - Более 6 часов: {time_stats['более 6 часов']}

💬 **Рабочий чат:** {'✅ Установлен' if work_chat_manager.is_work_chat_set() else '❌ Не установлен'}
🔄 **Логика уведомлений:** Удаление старого + отправка нового каждые 15 минут
⏳ **Cooldown:** {'✅ Активен' if not master_notification_manager.should_update() else '❌ Можно отправлять'}
🔒 **Последовательность:** ✅ Строгая (1→2→3)
🕐 **Текущее время:** {now.strftime('%H:%M:%S')}
    """
    
    await update.message.reply_text(stats_text, parse_mode='Markdown')

async def pending_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    all_pending = pending_messages_manager.get_all_pending_messages()
    
    if not all_pending:
        await update.message.reply_text("✅ Нет непрочитанных сообщений")
        return
    
    # ОТЛАДОЧНАЯ ИНФОРМАЦИЯ - показываем статусы воронок
    FUNNELS = funnels_config.get_funnels()
    funnel_1_count = len(pending_messages_manager.get_messages_for_funnel(1, funnels_state_manager))
    funnel_2_count = len(pending_messages_manager.get_messages_for_funnel(2, funnels_state_manager))
    funnel_3_count = len(pending_messages_manager.get_messages_for_funnel(3, funnels_state_manager))
    
    debug_info = f"\n🔍 **ОТЛАДКА ВОРОНОК:**\n"
    debug_info += f"🟡 Воронка 1 ({FUNNELS[1]} мин): {funnel_1_count} сообщ.\n"
    debug_info += f"🟠 Воронка 2 ({FUNNELS[2]} мин): {funnel_2_count} сообщ.\n"  
    debug_info += f"🔴 Воронка 3 ({FUNNELS[3]} мин): {funnel_3_count} сообщ.\n\n"
    
    chats_messages = {}
    for message in all_pending:
        chat_id = message['chat_id']
        if chat_id not in chats_messages:
            chats_messages[chat_id] = []
        chats_messages[chat_id].append(message)
    
    pending_text = f"📋 **НЕПРОЧИТАННЫЕ СООБЩЕНИЯ**\n\nВсего сообщений: {len(all_pending)}\nЧатов: {len(chats_messages)}\n"
    pending_text += debug_info
    
    for i, (chat_id, messages) in enumerate(chats_messages.items(), 1):
        chat_display = get_chat_display_name(messages[0])
        oldest = min(msg['timestamp'] for msg in messages)
        time_ago = format_time_ago(oldest)
        
        # Определяем текущую воронку для чата
        current_funnel = max([msg.get('current_funnel', 0) for msg in messages])
        funnel_emoji = get_funnel_emoji(current_funnel) if current_funnel > 0 else "⚪"
        
        # Показываем детальную информацию о воронках для этого чата
        funnel_details = []
        for msg in messages:
            funnel_num = msg.get('current_funnel', 0)
            funnels_sent = msg.get('funnels_sent', [])
            funnel_details.append(f"{funnel_num}({','.join(map(str, funnels_sent))})")
        
        pending_text += f"{i}. {chat_display} {funnel_emoji}\n"
        pending_text += f"   📝 Сообщений: {len(messages)}\n"
        pending_text += f"   ⏰ Самое старое: {time_ago} назад\n"
        pending_text += f"   🚀 Текущая воронка: {current_funnel}\n"
        pending_text += f"   🔧 Статусы: {', '.join(funnel_details)}\n\n"
    
    if len(pending_text) > 4000:
        pending_text = pending_text[:4000] + "\n\n... (сообщение обрезано)"
    
    await update.message.reply_text(pending_text, parse_mode='Markdown')

async def clear_chat_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    chat_id = update.message.chat.id
    removed_count = pending_messages_manager.remove_all_chat_messages(chat_id)
    
    if removed_count > 0:
        await update.message.reply_text(f"✅ Удалено {removed_count} сообщений из этого чата")
        logger.info(f"✅ Удалены сообщения из чата {chat_id}")
    else:
        await update.message.reply_text("✅ В этом чате нет непрочитанных сообщений")

async def clear_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    removed_count = pending_messages_manager.clear_all()
    await update.message.reply_text(f"✅ Удалены все непрочитанные сообщения ({removed_count} шт.)")
    logger.info("✅ Все сообщения очищены")

# ========== КОМАНДЫ УПРАВЛЕНИЯ ИСКЛЮЧЕНИЯМИ ==========

async def add_exception_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    if not context.args:
        await update.message.reply_text("❌ Использование: /add_exception <ID или @username>")
        return
    
    identifier = context.args[0]
    
    if identifier.isdigit():
        user_id = int(identifier)
        if excluded_users_manager.add_user_id(user_id):
            await update.message.reply_text(f"✅ ID `{user_id}` добавлен в исключения")
        else:
            await update.message.reply_text(f"ℹ️ ID `{user_id}` уже в исключениях")
    else:
        if excluded_users_manager.add_username(identifier):
            await update.message.reply_text(f"✅ Username `{identifier}` добавлен в исключения")
        else:
            await update.message.reply_text(f"ℹ️ Username `{identifier}` уже в исключениях")

async def remove_exception_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    if not context.args:
        await update.message.reply_text("❌ Использование: /remove_exception <ID или @username>")
        return
    
    identifier = context.args[0]
    
    if identifier.isdigit():
        user_id = int(identifier)
        if excluded_users_manager.remove_user_id(user_id):
            await update.message.reply_text(f"✅ ID `{user_id}` удален из исключений")
        else:
            await update.message.reply_text(f"❌ ID `{user_id}` не найден в исключениях")
    else:
        if excluded_users_manager.remove_username(identifier):
            await update.message.reply_text(f"✅ Username `{identifier}` удален из исключений")
        else:
            await update.message.reply_text(f"❌ Username `{identifier}` не найден в исключениях")

async def list_exceptions_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    if not update or not update.message:
        return
        
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ У вас нет прав для выполнения этой команды")
        return
    
    excluded_users_manager.clear_all()
    await update.message.reply_text("✅ Все исключения очищены")

# ========== ОБРАБОТЧИКИ СООБЩЕНИЙ ==========

async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            
            # НЕ отправляем уведомление автоматически при новом сообщении - только по расписанию
            logger.info("📝 Новое сообщение добавлено, уведомление будет отправлено по расписанию")

async def handle_private_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        
        # НЕ отправляем уведомление автоматически при новом сообщении - только по расписанию
        logger.info("📝 Новое сообщение добавлено, уведомление будет отправлено по расписанию")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"💥 Ошибка при обработке сообщения: {context.error}")
    
    if update:
        logger.error(f"💥 Update object: {update}")
        if update.message:
            logger.error(f"💥 Message info: chat_id={update.message.chat.id}, user_id={update.message.from_user.id if update.message.from_user else 'None'}")
    
    try:
        for admin_id in ADMIN_IDS:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"💥 Произошла ошибка в боте:\n\n{context.error}"
            )
    except Exception as e:
        logger.error(f"❌ Не удалось отправить уведомление об ошибке: {e}")

# ========== ЗАПУСК БОТА ==========

def main():
    try:
        print("=" * 50)
        print("🤖 ЗАПУСК БОТА-АВТООТВЕТЧИКА")
        print("=" * 50)
        
        application = Application.builder().token(BOT_TOKEN).build()
        
        # Команды для управления воронками
        application.add_handler(CommandHandler("funnels", funnels_command))
        application.add_handler(CommandHandler("set_funnel_1", set_funnel_1_command))
        application.add_handler(CommandHandler("set_funnel_2", set_funnel_2_command))
        application.add_handler(CommandHandler("set_funnel_3", set_funnel_3_command))
        application.add_handler(CommandHandler("reset_funnels", reset_funnels_command))
        application.add_handler(CommandHandler("force_update_funnels", force_update_funnels_command))
        application.add_handler(CommandHandler("debug_funnels", debug_funnels_command))
        
        # Команды для обновления уведомления
        application.add_handler(CommandHandler("update_notification", update_notification_command))
        
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
        
        # Обработчики сообщений
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
        
        # Периодическая проверка и отправка нового уведомления (каждые 15 минут)
        job_queue = application.job_queue
        if job_queue:
            job_queue.run_repeating(check_and_send_new_notification, interval=900, first=10)  # 15 минут
            print("✅ Планировщик задач запущен (удаление старого + отправка нового каждые 15 минут)")
            print("🛡️  COOLDOWN АКТИВИРОВАН - защита от частых отправок")
            print("🔒 СТРОГАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ - сообщения проходят воронки 1→2→3")
            print("✅ АВТОМАТИЧЕСКАЯ ОТМЕТКА - сообщения помечаются как обработанные после отправки")
        else:
            print("❌ Планировщик задач недоступен")
        
        # Запуск
        FUNNELS = funnels_config.get_funnels()
        excluded_users = excluded_users_manager.get_all_excluded()
        total_excluded = len(excluded_users["user_ids"]) + len(excluded_users["usernames"])
        
        print("🚀 Бот запускается...")
        print(f"📊 Загружено флагов: {flags_manager.count_flags()}")
        print(f"📋 Непрочитанных сообщений: {len(pending_messages_manager.get_all_pending_messages())}")
        print(f"👥 Менеджеров в системе: {total_excluded}")
        print(f"⚙️ Воронки уведомлений: {FUNNELS}")
        
        if work_chat_manager.is_work_chat_set():
            print(f"💬 Рабочий чат установлен: {work_chat_manager.get_work_chat_id()}")
        else:
            print("⚠️ Рабочий чат не установлен! Используйте /set_work_chat")
        
        print("🔄 Логика уведомлений: УДАЛЕНИЕ СТАРОГО + ОТПРАВКА НОВОГО каждые 15 минут")
        print("⏳ COOLDOWN: 15 минут между отправками")
        print("🔒 ПОСЛЕДОВАТЕЛЬНОСТЬ: строгая 1→2→3")
        print("✅ ОТМЕТКА: сообщения помечаются как обработанные после отправки")
        print("⏰ Ожидание сообщений...")
        print("=" * 50)
        
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=False,
            close_loop=False
        )
        
    except Exception as e:
        print(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logger.error(f"💥 Критическая ошибка при запуске бота: {e}")

if __name__ == "__main__":
    main()
