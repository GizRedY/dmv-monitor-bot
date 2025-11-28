"""
DMV Monitor Server - VPS Version with Browser Push Notifications
FIXED: Улучшенная стабильность для работы на сервере
"""

import asyncio
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, field
from pathlib import Path
import json
import time
import re
import sys
import os
import fcntl

try:
    from playwright.async_api import async_playwright, Page, Browser
except ImportError:
    print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)

try:
    from pywebpush import webpush, WebPushException
except ImportError:
    print("ERROR: pywebpush not installed. Run: pip install pywebpush")
    sys.exit(1)

# Полный список локаций NC
ALL_NC_LOCATIONS = [
    'Aberdeen', 'Ahoskie', 'Albemarle', 'Andrews', 'Asheboro',
    'Asheville', 'Boone', 'Brevard', 'Bryson City', 'Burgaw',
    'Burnsville', 'Carrboro', 'Cary', 'Charlotte East', 'Charlotte North',
    'Charlotte South', 'Charlotte West', 'Clayton', 'Clinton', 'Clyde',
    'Concord', 'Durham East', 'Durham South', 'Elizabeth City', 'Elizabethtown',
    'Elkin', 'Erwin', 'Fayetteville South', 'Fayetteville West', 'Forest City',
    'Franklin', 'Fuquay-Varina', 'Garner', 'Gastonia', 'Goldsboro',
    'Graham', 'Greensboro East', 'Greensboro West', 'Greenville', 'Hamlet',
    'Havelock', 'Henderson', 'Hendersonville', 'Hickory', 'High Point',
    'Hillsborough', 'Hudson', 'Huntersville', 'Jacksonville', 'Jefferson',
    'Kernersville', 'Kinston', 'Lexington', 'Lincolnton', 'Louisburg',
    'Lumberton', 'Marion', 'Marshall', 'Mocksville', 'Monroe',
    'Mooresville', 'Morehead City', 'Morganton', 'Mount Airy', 'Mount Holly',
    'Nags Head', 'New Bern', 'Newton', 'Oxford', 'Polkton',
    'Raleigh North', 'Raleigh West', 'Roanoke Rapids', 'Rocky Mount', 'Roxboro',
    'Salisbury', 'Sanford', 'Shallotte', 'Shelby', 'Siler City',
    'Smithfield', 'Statesville', 'Stedman', 'Sylva', 'Tarboro',
    'Taylorsville', 'Thomasville', 'Troy', 'Washington', 'Wendell',
    'Wentworth', 'Whiteville', 'Wilkesboro', 'Williamston', 'Wilmington North',
    'Wilmington South', 'Wilson', 'Winston Salem North', 'Winston Salem South', 'Yadkinville'
]

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class Config:
    """Server configuration"""
    # DMV Settings
    dmv_url: str = "https://skiptheline.ncdot.gov/Webapp/Appointment/Index/a7ade79b-996d-4971-8766-97feb75254de"
    check_interval_sec: int = 150  # 🔧 УВЕЛИЧЕНО: 5 минут между проверками
    base_city: str = "Raleigh"
    base_coords: Tuple[float, float] = (35.787743, -78.644257)

    # Browser settings - 🔧 КРИТИЧЕСКИ ВАЖНО ДЛЯ СТАБИЛЬНОСТИ
    headless: bool = True
    page_timeout: int = 30000
    navigation_timeout: int = 45000

    # 🔧 НОВОЕ: Перезапуск браузера после N категорий
    browser_restart_after_categories: int = 3

    # 🔧 НОВОЕ: Максимум попыток при ошибке
    max_retries_on_error: int = 2

    location_click_timeout: int = 60000

    # Database/Storage
    data_dir: Path = Path("./data")
    subscriptions_file: Path = Path("./data/subscriptions.json")
    last_check_file: Path = Path("./public_data/last_check.json")

    # Cleanup settings
    subscription_max_age_days: int = 3

    # Logging
    log_file: Path = Path("./logs/dmv_monitor.log")
    log_level: str = "INFO"  # 🔧 Изменено на INFO /  WARNING для лучшей диагностики

    # VAPID keys
    vapid_private_key: str = "9stDm8G4-lI5xMFXLSQDiAWL0dIelrKAImhagQw2Gj0"
    vapid_public_key: str = "BFAncJsXiE0c_4N-hvqQOESc8_CLk3p0H0LopSKAwPq9tEMnnbREZ2vhLLTMijDy9yBwaLMnSKbeziGHmqyrrLw"
    vapid_claims: dict = field(default_factory=lambda: {
        "sub": "mailto:activation.service.mailbox@gmail.com"
    })


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class TimeSlot:
    """Represents an available appointment time slot"""
    date: date
    time: str

    def __str__(self):
        return f"{self.date.isoformat()} {self.time}"

    def to_dict(self):
        return {
            "date": self.date.isoformat(),
            "time": self.time
        }


@dataclass
class LocationAvailability:
    """Availability for a specific location"""
    location_name: str
    category: str
    slots: List[TimeSlot] = field(default_factory=list)
    last_checked: datetime = field(default_factory=datetime.now)
    available: bool = True  # Можно ли зайти в эту локацию


@dataclass
class UserSubscription:
    """User subscription to specific categories and locations"""
    user_id: str
    push_subscription: Optional[str] = None
    categories: Set[str] = field(default_factory=set)
    locations: Set[str] = field(default_factory=set)
    date_range_days: int = 30
    created_at: datetime = field(default_factory=datetime.now)
    last_notification_sent: Optional[datetime] = None
    failed_attempts: int = 0

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "push_subscription": self.push_subscription,
            "categories": list(self.categories),
            "locations": list(self.locations),
            "date_range_days": self.date_range_days,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            "last_notification_sent": self.last_notification_sent.isoformat() if self.last_notification_sent else None,
            "failed_attempts": self.failed_attempts
        }


# ============================================================================
# DMV CATEGORIES
# ============================================================================

DMV_CATEGORIES = {
    "driver_license_first_time": {
        "name": "Driver License - First Time",
        "description": "New driver over 18, new N.C. resident, REAL ID"
    },
    "driver_license_duplicate": {
        "name": "Driver License Duplicate",
        "description": "Replace lost or stolen license, change name or address, REAL ID"
    },
    "driver_license_renewal": {
        "name": "Driver License Renewal",
        "description": "Renew an existing license without any changes, REAL ID"
    },
    "fees": {
        "name": "Fees",
        "description": "License reinstatement appointment, administrative hearings, and medical certifications"
    },
    "id_card": {
        "name": "ID Card",
        "description": "State ID card, REAL ID"
    },
    "knowledge_computer_test": {
        "name": "Knowledge/Computer Test",
        "description": "Written, traffic signs, vision"
    },
    "legal_presence": {
        "name": "Legal Presence",
        "description": "For non-citizens to prove they are legally authorized to be in the U.S."
    },
    "motorcycle_skills_test": {
        "name": "Motorcycle Skills Test",
        "description": "Schedule a motorcycle driving skills test"
    },
    "non_cdl_road_test": {
        "name": "Non-CDL Road Test",
        "description": "Schedule a driving skills test"
    },
    "permits": {
        "name": "Permits",
        "description": "Adult permit, CDL"
    },
    "teen_driver_level_1": {
        "name": "Teen Driver Level 1",
        "description": "Limited learner permit - ages 15-17"
    },
    "teen_driver_level_2": {
        "name": "Teen Driver Level 2",
        "description": "Limited provisional license - ages 16-17; Level 1 permit"
    },
    "teen_driver_level_3": {
        "name": "Teen Driver Level 3",
        "description": "Full provisional license - ages 16-17; Level 2 license"
    }
}


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(config: Config):
    """Setup logging configuration"""
    config.log_file.parent.mkdir(parents=True, exist_ok=True)

    # 🔧 ИСПРАВЛЕНИЕ: Поддержка UTF-8 для эмодзи в Windows
    import sys

    # Настройка обработчиков с правильной кодировкой
    file_handler = logging.FileHandler(config.log_file, encoding='utf-8')

    # Для Windows консоли - используем UTF-8 или убираем эмодзи
    if sys.platform == 'win32':
        try:
            # Попытка включить UTF-8 в консоли Windows
            import codecs
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
            sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
            console_handler = logging.StreamHandler(sys.stdout)
        except:
            # Если не получилось - используем обычный вывод
            console_handler = logging.StreamHandler()
    else:
        console_handler = logging.StreamHandler()

    # Установка форматтера
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Настройка root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, config.log_level.upper()))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# ============================================================================
# NOTIFICATION SERVICE
# ============================================================================

class NotificationService:
    """Handles sending notifications to users via browser push"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("NotificationService")

    # 🔧 УЛУЧШЕНИЕ: NotificationService с более детальным логированием
    # Заменить метод send_push_notification в классе NotificationService (строки ~286-338)

    def send_push_notification(self, subscription: UserSubscription, title: str, body: str, url: str = "/") -> tuple[
        bool, Optional[str]]:
        """Send browser push notification with improved error handling"""
        try:
            if not subscription.push_subscription:
                self.logger.warning(f"No push subscription for user {subscription.user_id}")
                return False, 'no_subscription'

            push_sub = json.loads(subscription.push_subscription)
            endpoint = push_sub.get('endpoint', '')

            # Determine audience based on endpoint
            if 'apple.com' in endpoint:
                aud = 'https://web.push.apple.com'
            elif 'fcm.googleapis.com' in endpoint:
                aud = 'https://fcm.googleapis.com'
            elif 'mozilla.com' in endpoint:
                aud = 'https://updates.push.services.mozilla.com'
            else:
                from urllib.parse import urlparse
                parsed = urlparse(endpoint)
                aud = f"{parsed.scheme}://{parsed.netloc}"

            vapid_claims = {
                "sub": "mailto:activation.service.mailbox@gmail.com",
                "aud": aud
            }

            notification_data = {
                "title": title,
                "body": body,
                "icon": "/icon-192.png",
                "badge": "/icon-192.png",
                "tag": "dmv-appointment",
                "requireInteraction": True,
                "data": {
                    "url": url
                }
            }

            # 🔥 НОВОЕ: Детальное логирование перед отправкой
            self.logger.info(f"📤 Attempting push to user {subscription.user_id}, endpoint: {endpoint[:50]}...")

            webpush(
                subscription_info=push_sub,
                data=json.dumps(notification_data),
                vapid_private_key=self.config.vapid_private_key,
                vapid_claims=vapid_claims
            )

            self.logger.info(f"✅ Push notification sent successfully to user {subscription.user_id}")
            return True, None

        except WebPushException as e:
            # 🔥 УЛУЧШЕНО: Более детальное логирование ошибок
            self.logger.error(f"❌ WebPush error for user {subscription.user_id}: {e}")

            if e.response:
                status_code = e.response.status_code
                self.logger.error(f"   Status code: {status_code}, Response: {e.response.text[:200]}")

                # Только 404/410 = реально мёртвая подписка
                if status_code in [404, 410]:
                    self.logger.warning(f"💀 Subscription truly dead (404/410) for user {subscription.user_id}")
                    return False, 'invalid_subscription'

                # 400 = плохой запрос (возможно, временная проблема)
                elif status_code == 400:
                    self.logger.warning(f"⚠️ Bad request (400) for user {subscription.user_id} - may be temporary")
                    return False, 'bad_request'

                # 401/403 = проблемы с авторизацией
                elif status_code in [401, 403]:
                    self.logger.warning(f"🔒 Auth error ({status_code}) for user {subscription.user_id}")
                    return False, 'auth_error'

                # Любые другие коды
                else:
                    self.logger.warning(f"❓ Unknown error code {status_code} for user {subscription.user_id}")
                    return False, 'unknown_error'
            else:
                self.logger.error(f"❌ WebPush exception without response for user {subscription.user_id}")
                return False, 'network_error'

        except Exception as e:
            self.logger.error(f"💥 Unexpected error sending push notification to {subscription.user_id}: {e}",
                              exc_info=True)
            return False, 'exception'
        """Send browser push notification"""
        try:
            if not subscription.push_subscription:
                self.logger.warning(f"No push subscription for user {subscription.user_id}")
                return False, None

            push_sub = json.loads(subscription.push_subscription)
            endpoint = push_sub.get('endpoint', '')

            # Determine audience based on endpoint
            if 'apple.com' in endpoint:
                aud = 'https://web.push.apple.com'
            elif 'fcm.googleapis.com' in endpoint:
                aud = 'https://fcm.googleapis.com'
            elif 'mozilla.com' in endpoint:
                aud = 'https://updates.push.services.mozilla.com'
            else:
                from urllib.parse import urlparse
                parsed = urlparse(endpoint)
                aud = f"{parsed.scheme}://{parsed.netloc}"

            vapid_claims = {
                "sub": "mailto:activation.service.mailbox@gmail.com",
                "aud": aud
            }

            notification_data = {
                "title": title,
                "body": body,
                "icon": "/icon-192.png",
                "badge": "/icon-192.png",
                "tag": "dmv-appointment",
                "requireInteraction": True,
                "data": {
                    "url": url
                }
            }

            webpush(
                subscription_info=push_sub,
                data=json.dumps(notification_data),
                vapid_private_key=self.config.vapid_private_key,
                vapid_claims=vapid_claims
            )

            self.logger.info(f"Push notification sent to user {subscription.user_id}")
            return True, None

        except WebPushException as e:
            self.logger.error(f"WebPush error for user {subscription.user_id}: {e}")
            if e.response and e.response.status_code in [404, 410]:
                self.logger.warning(f"Subscription for user {subscription.user_id} is no longer valid")
                return False, 'invalid_subscription'
            return False, 'other'
        except Exception as e:
            self.logger.error(f"Error sending push notification: {e}")
            return False, 'other'

    def notify_user(self, subscription: UserSubscription, availability: LocationAvailability) -> tuple[bool, Optional[str]]:
        """Notify user about new availability"""
        category_name = DMV_CATEGORIES.get(availability.category, {}).get('name', availability.category)

        title = "🚗 DMV Appointment Available!"

        body_lines = [
            f"📋 {category_name}",
            f"📍 {availability.location_name}",
        ]

        if availability.slots:
            body_lines.append(f"\n📅 Available: {availability.slots[0].date.strftime('%b %d')} at {availability.slots[0].time}")
            if len(availability.slots) > 1:
                body_lines.append(f"+ {len(availability.slots) - 1} more slots")

        body = "\n".join(body_lines)

        return self.send_push_notification(subscription, title, body, url="https://skiptheline.ncdot.gov/Webapp/Appointment/Index/a7ade79b-996d-4971-8766-97feb75254de")


# ============================================================================
# SUBSCRIPTION MANAGER
# ============================================================================

class SubscriptionManager:
    """Manages user subscriptions"""

    def __init__(self, config: Config):
        self.config = config
        self.subscriptions: Dict[str, UserSubscription] = {}
        self.logger = logging.getLogger("SubscriptionManager")
        self.load_subscriptions()

    def increment_failed_attempts(self, user_id: str):
        """Increment failed notification attempts counter"""
        if user_id in self.subscriptions:
            self.subscriptions[user_id].failed_attempts += 1
            self.save_subscriptions()
            self.logger.warning(f"Failed attempts for {user_id}: {self.subscriptions[user_id].failed_attempts}")

    def reset_failed_attempts(self, user_id: str):
        """Reset failed attempts counter after successful notification - with merge protection"""
        # 🔥 НОВОЕ: Перезагружаем подписки перед обновлением
        self.load_subscriptions()

        if user_id in self.subscriptions:
            self.subscriptions[user_id].failed_attempts = 0
            self.save_subscriptions()
            self.logger.debug(f"✅ Reset failed attempts for {user_id}")
        else:
            self.logger.warning(f"⚠️ User {user_id} not found when resetting failed attempts")

    def load_subscriptions(self):
        """Load subscriptions from file"""
        try:
            self.subscriptions = {}

            if not self.config.subscriptions_file.exists():
                self.logger.info("No subscriptions file found")
                return

            # 🔒 Lock before reading
            lock_path = self.config.data_dir / "subscriptions.lock"

            with open(lock_path, 'w') as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)

                try:
                    with open(self.config.subscriptions_file, 'r') as f:
                        data = json.load(f)
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

            loaded_count = 0

            for user_data in data:
                try:
                    created_at = user_data.get('created_at')
                    if isinstance(created_at, str):
                        created_at = datetime.fromisoformat(created_at)
                    elif created_at is None:
                        created_at = datetime.now()

                    last_notification_sent = user_data.get('last_notification_sent')
                    if isinstance(last_notification_sent, str):
                        last_notification_sent = datetime.fromisoformat(last_notification_sent)

                    sub = UserSubscription(
                        user_id=user_data['user_id'],
                        push_subscription=user_data.get('push_subscription'),
                        categories=set(user_data.get('categories', [])),
                        locations=set(user_data.get('locations', [])),
                        date_range_days=user_data.get('date_range_days', 30),
                        created_at=created_at,
                        last_notification_sent=last_notification_sent,
                        failed_attempts=user_data.get('failed_attempts', 0)
                    )
                    self.subscriptions[sub.user_id] = sub
                    loaded_count += 1
                except Exception as e:
                    self.logger.error(f"Skipping invalid subscription entry: {e}")

            self.logger.info(f"Loaded {loaded_count} subscriptions")
        except Exception as e:
            self.logger.error(f"Error loading subscriptions: {e}")

    def save_subscriptions(self):
        """Save subscriptions to file (atomic write with file lock)"""
        try:
            self.config.data_dir.mkdir(parents=True, exist_ok=True)

            # 🔥 КРИТИЧНО: Lock file BEFORE any operations
            lock_path = self.config.data_dir / "subscriptions.lock"

            with open(lock_path, 'w') as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)

                try:
                    # 🔥 НОВОЕ: Перезагружаем данные из файла перед сохранением
                    # чтобы не потерять подписки, добавленные через API
                    existing_data = {}
                    if self.config.subscriptions_file.exists():
                        try:
                            with open(self.config.subscriptions_file, 'r') as f:
                                file_subs = json.load(f)
                                for sub_data in file_subs:
                                    existing_data[sub_data['user_id']] = sub_data
                            self.logger.debug(f"📖 Loaded {len(existing_data)} existing subscriptions from file")
                        except Exception as e:
                            self.logger.warning(f"⚠️ Could not load existing subscriptions: {e}")

                    # 🔥 НОВОЕ: Мёрджим наши изменения с тем, что в файле
                    # Приоритет у данных из памяти (self.subscriptions)
                    for user_id, sub in self.subscriptions.items():
                        existing_data[user_id] = sub.to_dict()

                    data = list(existing_data.values())

                    # Атомарная запись через временный файл
                    tmp_path = self.config.subscriptions_file.with_suffix(
                        self.config.subscriptions_file.suffix + ".tmp"
                    )

                    with open(tmp_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2)

                    os.replace(tmp_path, self.config.subscriptions_file)

                    self.logger.debug(f"✅ Saved {len(data)} subscriptions (locked, merged)")
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

        except Exception as e:
            self.logger.error(f"Error saving subscriptions: {e}", exc_info=True)

    def remove_subscription(self, user_id: str):
        """Remove a subscription"""
        if user_id in self.subscriptions:
            del self.subscriptions[user_id]
            self.save_subscriptions()
            self.logger.info(f"Removed subscription for user {user_id}")

    def cleanup_old_subscriptions(self):
        """Remove subscriptions older than max_age_days"""
        cutoff_date = datetime.now() - timedelta(days=self.config.subscription_max_age_days)
        removed = []

        for user_id, sub in list(self.subscriptions.items()):
            if sub.created_at < cutoff_date:
                removed.append(user_id)
                del self.subscriptions[user_id]

        if removed:
            self.save_subscriptions()
            self.logger.info(f"Cleaned up {len(removed)} old subscriptions")

        return len(removed)

    def update_last_notification(self, user_id: str):
        """Update last notification timestamp - with merge protection"""
        # 🔥 НОВОЕ: Перезагружаем подписки перед обновлением
        self.load_subscriptions()

        if user_id in self.subscriptions:
            self.subscriptions[user_id].last_notification_sent = datetime.now()
            self.save_subscriptions()
            self.logger.debug(f"📝 Updated last notification for {user_id}")
        else:
            self.logger.warning(f"⚠️ User {user_id} not found when updating notification timestamp")

    def increment_failed_attempts(self, user_id: str):
        """Increment failed notification attempts counter - with merge protection"""
        # 🔥 НОВОЕ: Перезагружаем подписки перед обновлением
        self.load_subscriptions()

        if user_id in self.subscriptions:
            self.subscriptions[user_id].failed_attempts += 1
            self.save_subscriptions()
            self.logger.warning(f"Failed attempts for {user_id}: {self.subscriptions[user_id].failed_attempts}")
        else:
            self.logger.warning(f"⚠️ User {user_id} not found when incrementing failed attempts")

    def get_interested_users(self, category: str, location: str) -> List[UserSubscription]:
        """Get users interested in this category/location combination"""
        interested = []
        for sub in self.subscriptions.values():
            if category in sub.categories or not sub.categories:
                if location in sub.locations or not sub.locations:
                    interested.append(sub)
        return interested


# ============================================================================
# DMV SCRAPER - 🔧 ИСПРАВЛЕННАЯ ВЕРСИЯ
# ============================================================================

class DMVScraper:
    """Scrapes DMV appointment availability - FIXED for server stability"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("DMVScraper")
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.playwright = None
        self.context = None

    async def get_current_page_type(self) -> str:
        """Determine what page we are currently on"""
        try:
            page_info = await self.page.evaluate("""
                () => {
                    const bodyText = document.body.innerText.toLowerCase();

                    // Check for location list page
                    const hasLocationTiles = document.querySelectorAll('.QflowObjectItem').length > 0;
                    const hasLocationText = bodyText.includes('select a location');

                    // Check for category selection page
                    const hasCategoryText = bodyText.includes('select a service') || 
                                           bodyText.includes('what would you like to do');

                    // Check for appointment calendar page
                    const hasCalendar = document.querySelector('.ui-datepicker') !== null;
                    const hasTimeSlots = document.querySelectorAll('select option').length > 5;

                    if (hasCalendar || hasTimeSlots) {
                        return 'appointment_page';
                    } else if (hasLocationTiles || hasLocationText) {
                        return 'location_list';
                    } else if (hasCategoryText) {
                        return 'category_page';
                    } else {
                        return 'unknown';
                    }
                }
            """)
            return page_info
        except Exception as e:
            self.logger.warning(f"Could not determine page type: {e}")
            return 'unknown'

    async def ensure_on_location_list(self) -> bool:
        """Make sure we are on the location list page"""
        try:
            page_type = await self.get_current_page_type()
            self.logger.info(f"Current page type: {page_type}")

            if page_type == 'location_list':
                self.logger.info("Already on location list page")
                return True

            if page_type == 'appointment_page':
                self.logger.info("On appointment page, going back to location list")
                try:
                    back_btn = self.page.locator('button:has-text("Back")').first
                    if await back_btn.is_visible(timeout=3000):
                        await self.safe_click(back_btn, "Back button")
                    else:
                        await self.page.go_back()

                    await asyncio.sleep(2)
                    await self.page.wait_for_load_state("networkidle", timeout=10000)

                    # Verify we are back
                    page_type = await self.get_current_page_type()
                    if page_type == 'location_list':
                        self.logger.info("Successfully returned to location list")
                        return True
                except Exception as e:
                    self.logger.warning(f"Error going back from appointment page: {e}")

            if page_type == 'category_page':
                self.logger.warning("On category page - need to re-navigate")
                return False

            if page_type == 'unknown':
                self.logger.warning("Unknown page - attempting to find location tiles")
                try:
                    tiles_count = await self.page.locator('.QflowObjectItem').count()
                    if tiles_count > 0:
                        self.logger.info(f"Found {tiles_count} location tiles")
                        return True
                except:
                    pass
                return False

            return False

        except Exception as e:
            self.logger.error(f"Error in ensure_on_location_list: {e}")
            return False


    async def initialize(self):
        """Initialize browser with better error handling"""
        try:
            self.logger.info("🔧 Initializing browser with server-optimized settings...")

            self.playwright = await async_playwright().start()

            # 🔧 АДАПТИВНЫЕ АРГУМЕНТЫ: разные для Windows и Linux
            import platform
            is_windows = platform.system() == 'Windows'

            if is_windows:
                # Windows: минимальные аргументы
                browser_args = [
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security"
                ]
                self.logger.info("🪟 Using Windows-optimized browser arguments")
            else:
                # Linux/VPS: полный набор для стабильности
                browser_args = [
                    "--disable-dev-shm-usage",
                    "--disable-web-security",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-gpu",
                    "--no-first-run",
                    "--no-zygote",
                    "--single-process",
                    "--disable-background-networking",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-breakpad",
                    "--disable-component-extensions-with-background-pages",
                    "--disable-features=TranslateUI,BlinkGenPropertyTrees",
                    "--disable-ipc-flooding-protection",
                    "--disable-renderer-backgrounding",
                    "--enable-features=NetworkService,NetworkServiceInProcess",
                    "--force-color-profile=srgb",
                    "--metrics-recording-only",
                    "--mute-audio"
                ]
                self.logger.info("🐧 Using Linux/VPS-optimized browser arguments")

            self.browser = await self.playwright.chromium.launch(
                headless=self.config.headless,
                args=browser_args
            )

            self.context = await self.browser.new_context(
                geolocation={"latitude": self.config.base_coords[0], "longitude": self.config.base_coords[1]},
                permissions=["geolocation"],
                ignore_https_errors=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                viewport={"width": 1280, "height": 720}  # 🔧 Фиксированный размер
            )

            self.page = await self.context.new_page()
            self.page.set_default_timeout(self.config.page_timeout)
            self.page.set_default_navigation_timeout(self.config.navigation_timeout)

            # 🔧 НОВОЕ: Игнорировать диалоги автоматически
            self.page.on("dialog", lambda d: asyncio.create_task(d.accept()))

            # 🔧 НОВОЕ: Отслеживание краха страницы
            self.page.on("crash", lambda: self.logger.error("❌ PAGE CRASHED!"))

            self.logger.info("✅ Browser initialized successfully")

        except Exception as e:
            self.logger.error(f"❌ Error initializing browser: {e}")
            raise

    async def restart_browser(self):
        """🔧 НОВОЕ: Перезапуск браузера для предотвращения утечек памяти"""
        self.logger.info("🔄 Restarting browser to free resources...")
        try:
            await self.close()
            await asyncio.sleep(1.5)  # Дать время системе освободить ресурсы
            await self.initialize()
            self.logger.info("✅ Browser restarted successfully")
        except Exception as e:
            self.logger.error(f"❌ Error restarting browser: {e}")
            raise

    async def safe_navigate(self, url: str, wait_until: str = "domcontentloaded") -> bool:
        """🔧 НОВОЕ: Безопасная навигация с повторными попытками"""
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                self.logger.info(f"🌐 Navigating to {url} (attempt {attempt + 1}/{max_attempts})")
                await self.page.goto(url, wait_until=wait_until, timeout=self.config.navigation_timeout)
                await asyncio.sleep(3)  # Увеличенная пауза
                return True
            except Exception as e:
                self.logger.warning(f"⚠️ Navigation attempt {attempt + 1} failed: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(5)  # Пауза перед повтором
                else:
                    self.logger.error(f"❌ All navigation attempts failed for {url}")
                    return False
        return False

    async def wait_for_element_ready(self, locator, timeout=8000):
        """Ждёт, пока элемент станет видимым и кликабельным"""
        try:
            await locator.wait_for(state="visible", timeout=timeout)
            await asyncio.sleep(1)  # Увеличенная пауза
            return True
        except Exception as e:
            self.logger.warning(f"⚠️ Element not ready: {e}")
            return False

    async def safe_click(self, locator, element_name="element", max_retries=3):
        """Безопасный клик с повторными попытками"""
        for attempt in range(max_retries):
            try:
                if await self.wait_for_element_ready(locator, timeout=15000):  # Увеличил до 15 сек
                    await locator.click(timeout=10000)  # Увеличил таймаут клика до 10 сек
                    self.logger.info(f"✅ Successfully clicked on {element_name}")
                    return True
                else:
                    self.logger.warning(f"⚠️ Attempt {attempt + 1}: {element_name} not ready")
            except Exception as e:
                self.logger.warning(f"⚠️ Attempt {attempt + 1} to click {element_name} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)  # 🔥 Сократил паузу с 3 до 2 сек

        self.logger.error(f"❌ Failed to click on {element_name} after {max_retries} attempts")
        return False

    async def navigate_to_category(self, category_key: str) -> bool:
        """Navigate to a specific category"""
        try:
            category_info = DMV_CATEGORIES.get(category_key)
            if not category_info:
                self.logger.error(f"Unknown category: {category_key}")
                return False

            category_name = category_info["name"]
            self.logger.info(f"📂 Navigating to category: {category_name}")

            # 🔧 Безопасная навигация
            if not await self.safe_navigate(self.config.dmv_url):
                return False

            # Первая кнопка "Make an Appointment"
            make_btn = self.page.locator("#cmdMakeAppt")
            if await make_btn.count() == 0:
                make_btn = self.page.locator("text=Make an Appointment").first

            if not await self.safe_click(make_btn, "Make an Appointment button"):
                return False

            await self.page.wait_for_load_state("networkidle", timeout=40000)
            await asyncio.sleep(1.5)

            # Вторая кнопка "Make an Appointment" (если есть)
            second_make = self.page.locator("input.next-button[value='Make an Appointment']")
            if await second_make.is_visible():
                if not await self.safe_click(second_make, "Second Make an Appointment button"):
                    self.logger.warning("⚠️ Could not click second button, continuing...")
                await self.page.wait_for_load_state("networkidle", timeout=40000)
                await asyncio.sleep(1.5)

            # OK button
            ok_btn = self.page.get_by_role("button", name=re.compile(r"^ok$", re.I))
            if await ok_btn.is_visible():
                await self.safe_click(ok_btn, "OK button")
                await asyncio.sleep(2)

            self.logger.info(f"🔍 Selecting category: {category_name}")
            await asyncio.sleep(1.5)

            # Поиск и клик на категорию
            candidates = [
                self.page.locator(f"text={category_name}").first,
                self.page.locator(f"button:has-text('{category_name}')").first,
                self.page.locator(f"a:has-text('{category_name}')").first,
            ]

            clicked = False
            for candidate in candidates:
                try:
                    if await candidate.count() > 0:
                        if await self.safe_click(candidate, f"Category: {category_name}"):
                            clicked = True
                            break
                except Exception:
                    continue

            if not clicked:
                self.logger.error(f"❌ Could not find category: {category_name}")
                return False

            await self.page.wait_for_load_state("networkidle", timeout=40000)
            await asyncio.sleep(2)

            # 🔧 УЛУЧШЕННАЯ проверка страницы выбора локации с защитой от null
            try:
                # Ждем появления любого из признаков страницы локаций
                await self.page.wait_for_function("""
                            () => {
                                // Защита от null/undefined
                                if (!document.body) return false;

                                const text = (document.body.innerText || '').toLowerCase();
                                const hasLocationText = text.includes('select a location') || 
                                                       text.includes('choose a location');
                                const hasLocationTiles = document.querySelectorAll('.QflowObjectItem').length > 0;
                                const hasLocationDropdown = document.querySelector('select[name*="location"]') !== null;

                                return hasLocationText || hasLocationTiles || hasLocationDropdown;
                            }""", timeout=35000)
                self.logger.info("✅ Reached location selection page")
                return True
            except Exception as e:
                # Дополнительная проверка: есть ли вообще элементы локаций?
                try:
                    tiles_count = await self.page.locator('.QflowObjectItem').count()
                    if tiles_count > 0:
                        self.logger.info(f"✅ Found {tiles_count} location tiles, proceeding")
                        return True
                except:
                    pass

                self.logger.warning(f"⚠️ Could not verify location page, but continuing... ({str(e)[:100]})")
                return True

        except Exception as e:
            self.logger.error(f"❌ Error navigating to category: {e}")
            return False

    async def get_available_locations(self) -> List[str]:
        """Get list of available locations"""
        try:
            await asyncio.sleep(2)

            available_locations = []
            active_tiles = self.page.locator(".QflowObjectItem.ui-selectable.Active-Unit:not(.disabled-unit)")

            count = await active_tiles.count()
            self.logger.info(f"📍 Found {count} active location tiles")

            for i in range(count):
                try:
                    tile = active_tiles.nth(i)
                    await tile.wait_for(state="visible", timeout=7000)

                    text = await tile.inner_text()
                    lines = [line.strip() for line in text.splitlines() if line.strip()]

                    if lines:
                        location_name = lines[0]
                        if "sorry" not in location_name.lower() and "don't have" not in location_name.lower():
                            available_locations.append(location_name)

                except Exception as e:
                    self.logger.warning(f"⚠️ Error processing tile {i}: {e}")
                    continue

            self.logger.info(f"✅ Found {len(available_locations)} available locations")
            return available_locations

        except Exception as e:
            self.logger.error(f"❌ Error getting available locations: {e}")
            return []

    async def get_appointment_slots(self, location_name: str, category_key: str) -> List[TimeSlot]:
        """Get available appointment slots for a location"""
        slots = []

        try:
            self.logger.info(f"Checking slots for: {location_name}")

            # CRITICAL: Make sure we are on location list page with smart recovery
            max_recovery_attempts = 2
            for recovery_attempt in range(max_recovery_attempts):
                page_check = await self.ensure_on_location_list()

                if page_check:
                    # ✅ Мы на правильной странице
                    self.logger.info(f"✅ Confirmed on location list page")
                    break

                # ❌ Не на той странице - проверяем где мы
                current_page_type = await self.get_current_page_type()
                self.logger.warning(
                    f"⚠️ Wrong page type: {current_page_type}, attempting recovery (attempt {recovery_attempt + 1}/{max_recovery_attempts})...")

                if current_page_type == 'category_page':
                    # Мы вернулись к выбору категорий - нужно заново войти в категорию!
                    self.logger.info(
                        f"🔄 Accidentally returned to category page, re-navigating to category {category_key}...")
                    if await self.navigate_to_category(category_key):
                        self.logger.info(f"✅ Successfully re-entered category {category_key}")
                        continue
                    else:
                        self.logger.error(f"❌ Failed to re-enter category {category_key}")
                        return slots

                elif current_page_type == 'appointment_page':
                    # Все еще на странице календаря - пытаемся вернуться
                    self.logger.info(f"🔙 Still on appointment page, going back...")
                    await self.page.go_back()
                    await asyncio.sleep(2)

                else:
                    # Неизвестная страница - пробуем вернуться
                    self.logger.warning(f"❓ Unknown page, attempting to go back...")
                    await self.page.go_back()
                    await asyncio.sleep(2)
            else:
                # Не получилось вернуться после всех попыток
                self.logger.error(
                    f"❌ Failed to return to location list after {max_recovery_attempts} attempts! Skipping {location_name}")
                return slots

            clicked = False
            selectors = [
                f"div:has-text('{location_name}')",
                f".QFlowObjectItem:has-text('{location_name}')",
            ]

            for selector in selectors:
                try:
                    elements = self.page.locator(selector)
                    count = await elements.count()

                    if count > 0:
                        for i in range(count):
                            element = elements.nth(i)
                            if await element.is_visible():
                                text = await element.inner_text()
                                if location_name.lower() in text.lower():
                                    if "sorry" not in text.lower():
                                        if await self.safe_click(element, f"Location: {location_name}"):
                                            clicked = True
                                            # Wait for navigation after click
                                            try:
                                                await self.page.wait_for_load_state("networkidle", timeout=15000)
                                            except Exception as e:
                                                self.logger.warning(f"Load state warning after clicking location: {e}")
                                            break
                        if clicked:
                            break
                except Exception:
                    continue

            if not clicked:
                self.logger.warning(f"Could not click on location: {location_name}")
                return slots

            await asyncio.sleep(3)
            try:
                await self.page.wait_for_load_state("domcontentloaded", timeout=10000)
            except:
                pass

            # Extract appointment data
            appointment_data = await self.page.evaluate("""
                () => {
                    const results = [];

                    let currentMonth = null;
                    let currentYear = null;

                    const monthEl = document.querySelector('.ui-datepicker-month, span.ui-datepicker-month');
                    const yearEl = document.querySelector('.ui-datepicker-year, span.ui-datepicker-year');

                    if (monthEl && yearEl) {
                        const monthText = monthEl.textContent.trim().toLowerCase();
                        const monthMap = {
                            'january': 1, 'february': 2, 'march': 3, 'april': 4,
                            'may': 5, 'june': 6, 'july': 7, 'august': 8,
                            'september': 9, 'october': 10, 'november': 11, 'december': 12
                        };
                        currentMonth = monthMap[monthText];
                        currentYear = parseInt(yearEl.textContent.trim());
                    }

                    if (!currentMonth || !currentYear) {
                        const now = new Date();
                        currentMonth = now.getMonth() + 1;
                        currentYear = now.getFullYear();
                    }

                    const availableDays = [];
                    const datepickerCells = document.querySelectorAll('.ui-datepicker-calendar td a:not(.ui-state-disabled)');
                    for (const cell of datepickerCells) {
                        const dayNum = parseInt(cell.textContent.trim());
                        if (dayNum >= 1 && dayNum <= 31) {
                            availableDays.push(dayNum);
                        }
                    }

                    const timeSlots = [];
                    const selects = document.querySelectorAll('select');
                    for (const select of selects) {
                        const options = Array.from(select.options);
                        for (const opt of options) {
                            if (opt.value && opt.value.trim() !== '' && !opt.disabled) {
                                const text = opt.textContent.trim();
                                if (/\\d{1,2}:\\d{2}\\s*(AM|PM)?/i.test(text)) {
                                    timeSlots.push(text);
                                }
                            }
                        }
                    }

                    return {
                        currentMonth: currentMonth,
                        currentYear: currentYear,
                        availableDays: [...new Set(availableDays)].sort((a, b) => a - b),
                        timeSlots: [...new Set(timeSlots)]
                    };
                }
            """)

            # Combine dates and times
            if appointment_data['availableDays'] and appointment_data['timeSlots']:
                for day in appointment_data['availableDays'][:10]:
                    try:
                        slot_date = date(
                            appointment_data['currentYear'],
                            appointment_data['currentMonth'],
                            day
                        )

                        for time_str in appointment_data['timeSlots'][:5]:
                            slots.append(TimeSlot(date=slot_date, time=time_str))

                    except ValueError:
                        continue

            self.logger.info(f"✅ Found {len(slots)} slots for {location_name}")

            await asyncio.sleep(2)

            # Go back to location list
            try:
                page_type = await self.get_current_page_type()

                if page_type == 'appointment_page':
                    self.logger.info("Going back from appointment page")
                    # 🔥 УПРОЩЕНО: Сразу используем browser back, без попытки найти кнопку
                    await self.page.go_back()

                    await asyncio.sleep(2)
                    try:
                        await self.page.wait_for_load_state("networkidle", timeout=10000)

                        # Verify we returned to location list
                        final_page_type = await self.get_current_page_type()
                        if final_page_type == 'location_list':
                            self.logger.info("✅ Successfully returned to location list")
                        else:
                            self.logger.warning(f"⚠️ After going back, page type is: {final_page_type}")
                            if final_page_type != 'location_list':
                                await self.page.go_back()
                                await asyncio.sleep(2)

                    except Exception as e:
                        self.logger.warning(f"Timeout waiting for location list: {e}")
                        await self.page.go_back()
                        await asyncio.sleep(2)
                else:
                    self.logger.warning(f"Expected appointment page but got: {page_type}")

            except Exception as e:
                self.logger.error(f"Error going back: {e}")
                try:
                    await self.page.go_back()
                    await asyncio.sleep(2)
                except:
                    pass

        except Exception as e:
            self.logger.error(f"❌ Error getting slots for {location_name}: {e}")

        return slots

    async def close(self):
        """Close browser"""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            self.logger.info("🔒 Browser closed")
        except Exception as e:
            self.logger.error(f"❌ Error closing browser: {e}")


# ============================================================================
# MAIN MONITOR SERVICE
# ============================================================================

class DMVMonitorService:
    """Main monitoring service"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("DMVMonitorService")
        self.scraper = DMVScraper(config)
        self.subscription_manager = SubscriptionManager(config)
        self.notification_service = NotificationService(config)
        self.last_seen_slots: Dict[str, Set[str]] = {}
        self.current_availability: Dict[str, dict] = {}

    async def initialize(self):
        """Initialize the service"""
        self.logger.info("🚀 Initializing DMV Monitor Service")
        await self.scraper.initialize()
        self.logger.info("✅ Service initialized successfully")

    def _save_current_availability(self):
        """Persist current availability to JSON"""
        try:
            self.config.data_dir.mkdir(parents=True, exist_ok=True)

            existing_data = {}
            if self.config.last_check_file.exists():
                with open(self.config.last_check_file, "r") as f:
                    existing_list = json.load(f)
                    for item in existing_list:
                        key = f"{item['category']}:{item['location_name']}"
                        existing_data[key] = item

            for key, new_item in self.current_availability.items():
                existing_data[key] = new_item

            availability_list = list(existing_data.values())

            self.logger.info(f"💾 Saving {len(availability_list)} availability entries")

            with open(self.config.last_check_file, "w") as f:
                json.dump(availability_list, f, indent=2)

            self.logger.debug(f"✅ Successfully saved availability data")
        except Exception as e:
            self.logger.error(f"❌ Error saving current availability: {e}", exc_info=True)

    def _update_availability_entry(self, availability: LocationAvailability):
        """Update availability record for given category/location"""
        key = f"{availability.category}:{availability.location_name}"

        self.logger.debug(f"📝 Updating availability entry: {key}")

        # Определяем статус: available (можно зайти) или has_slots (есть слоты)
        has_slots = len(availability.slots) > 0
        is_available = getattr(availability, 'available', has_slots)

        self.current_availability[key] = {
            "category": availability.category,
            "location_name": availability.location_name,
            "available": is_available,  # Можно ли зайти в локацию
            "has_slots": has_slots,  # Есть ли реальные слоты (только если заходили)
            "last_checked": availability.last_checked.isoformat()
        }

    async def monitor_category(self, category_key: str) -> bool:
        """Monitor a single category with better error recovery"""
        max_retries = 3  # 🔥 Максимум попыток при ошибке

        for attempt in range(max_retries):
            try:
                self.logger.info(f"{'=' * 60}")
                self.logger.info(f"📂 Monitoring category: {category_key} (attempt {attempt + 1}/{max_retries})")
                self.logger.info(f"{'=' * 60}")

                if not await self.scraper.navigate_to_category(category_key):
                    self.logger.error(f"❌ Failed to navigate to category: {category_key}")

                    # 🔥 При провале навигации - перезапускаем браузер
                    if attempt < max_retries - 1:
                        self.logger.warning(f"🔄 Restarting browser after navigation failure...")
                        await self.scraper.restart_browser()
                        await asyncio.sleep(5)
                        continue
                    else:
                        return False

                available_locations = await self.scraper.get_available_locations()

                if not available_locations:
                    self.logger.info(f"🔭 No available locations for category: {category_key}")

                    # Записываем ВСЕ локации NC как недоступные (без указания количества слотов)
                    for location in ALL_NC_LOCATIONS:
                        availability = LocationAvailability(
                            location_name=location,
                            category=category_key,
                            slots=[],
                            available=False  # Явно указываем что локация недоступна
                        )
                        self._update_availability_entry(availability)

                    self.logger.info(f"📝 Recorded all {len(ALL_NC_LOCATIONS)} NC locations as unavailable")
                    self._save_current_availability()
                    return True

                # 🔥 НОВАЯ ЛОГИКА: Проверяем только локации, на которые есть подписчики
                # 🔥 НОВАЯ ЛОГИКА: Проверяем только локации, на которые есть подписчики
                interested_locations = set()
                for sub in self.subscription_manager.subscriptions.values():
                    if category_key in sub.categories or not sub.categories:
                        interested_locations.update(sub.locations if sub.locations else [])

                self.logger.info(f"✅ Found {len(available_locations)} available locations for {category_key}")
                self.logger.info(f"👥 Found {len(interested_locations)} locations with subscribers")

                # Сначала записываем ВСЕ локации
                for location in ALL_NC_LOCATIONS:
                    # Проверяем доступна ли локация
                    is_available = location in available_locations

                    availability = LocationAvailability(
                        location_name=location,
                        category=category_key,
                        slots=[],
                        available=is_available  # True если локация в списке доступных
                    )
                    self._update_availability_entry(availability)

                # Теперь проверяем ТОЛЬКО локации с подписчиками И которые доступны
                locations_to_check = interested_locations.intersection(available_locations)

                if not locations_to_check:
                    self.logger.info(f"ℹ️ No locations with subscribers are available for {category_key}")
                    self._save_current_availability()
                    return True

                self.logger.info(f"🔍 Will check {len(locations_to_check)} locations with active subscriptions")

                locations_checked = 0
                for location in locations_to_check:
                    try:
                        self.logger.info(f"🔍 Checking slots for {location} in {category_key}")
                        slots = await self.scraper.get_appointment_slots(location, category_key)
                        locations_checked += 1

                        availability = LocationAvailability(
                            location_name=location,
                            category=category_key,
                            slots=slots
                        )

                        self._update_availability_entry(availability)
                        self.logger.info(f"✅ Updated availability for {location}: {len(slots)} slots")

                        if slots:
                            key = f"{category_key}:{location}"
                            current_slots_set = {str(slot) for slot in slots}

                            if key not in self.last_seen_slots:
                                self.last_seen_slots[key] = set()

                            new_slots = current_slots_set - self.last_seen_slots[key]

                            if slots:
                                self.logger.info(f"🎉 NEW SLOTS FOUND for {location}: {len(new_slots)} new slots!")

                                interested_users = self.subscription_manager.get_interested_users(
                                    category_key, location
                                )

                                self.logger.info(f"👥 Found {len(interested_users)} interested users")

                                # 🔧 ИСПРАВЛЕНИЕ: Заменить блок обработки уведомлений в dmv_monitor.py
                                # Найти строки ~946-960 и заменить на этот код:

                                for user in interested_users:
                                    success, error_type = self.notification_service.notify_user(user, availability)

                                    if success:
                                        self.logger.info(f"✅ Successfully notified user {user.user_id}")
                                        self.subscription_manager.update_last_notification(user.user_id)
                                        self.subscription_manager.reset_failed_attempts(user.user_id)

                                    elif error_type == 'invalid_subscription':
                                        # 🔥 КРИТИЧНО: Не удаляем сразу! Даём 3 попытки на все ошибки
                                        self.logger.warning(
                                            f"⚠️ Invalid subscription for user {user.user_id}, incrementing failed attempts")
                                        self.subscription_manager.increment_failed_attempts(user.user_id)

                                        # Удаляем только после 5 неудачных попыток (было 3)
                                        if user.failed_attempts >= 5:
                                            self.logger.info(
                                                f"🗑️ Removing subscription after {user.failed_attempts} failed attempts: {user.user_id}")
                                            self.subscription_manager.remove_subscription(user.user_id)

                                    else:
                                        # Любая другая ошибка - тоже инкрементируем счётчик
                                        self.logger.warning(f"⚠️ Failed to notify user {user.user_id}: {error_type}")
                                        self.subscription_manager.increment_failed_attempts(user.user_id)

                                        # Удаляем только после 5 неудачных попыток
                                        if user.failed_attempts >= 5:
                                            self.logger.info(
                                                f"🗑️ Removing subscription after {user.failed_attempts} failed attempts: {user.user_id}")
                                            self.subscription_manager.remove_subscription(user.user_id)

                                self.last_seen_slots[key] = current_slots_set
                            else:
                                self.logger.info(
                                    f"ℹ️ No new slots for {location} (already seen all {len(slots)} slots)")
                        else:
                            self.logger.info(f"🔭 No available slots for {location}")

                    except Exception as e:
                        self.logger.error(f"❌ Error checking location {location}: {e}", exc_info=True)

                        # 🔥 КРИТИЧНО: Если словили "context destroyed" - сразу выходим
                        if "context was destroyed" in str(e).lower():
                            self.logger.error(f"💥 Browser context destroyed! Need restart.")
                            raise  # Пробрасываем ошибку наверх для перезапуска

                        continue

                self.logger.info(
                    f"✅ Finished checking category {category_key} ({locations_checked} locations), saving results...")
                self._save_current_availability()
                return True

            except Exception as e:
                self.logger.error(f"❌ Error monitoring category {category_key} (attempt {attempt + 1}): {e}",
                                  exc_info=True)

                # 🔥 При любой серьёзной ошибке - перезапускаем браузер
                if attempt < max_retries - 1:
                    self.logger.warning(f"🔄 Restarting browser after error (attempt {attempt + 1})...")
                    try:
                        await self.scraper.restart_browser()
                        await asyncio.sleep(10)  # Даём браузеру время восстановиться
                    except Exception as restart_error:
                        self.logger.error(f"💥 Failed to restart browser: {restart_error}")
                        # Если даже перезапуск не помог - ждём дольше
                        await asyncio.sleep(30)
                    continue
                else:
                    self.logger.error(f"❌ All {max_retries} attempts failed for category {category_key}")
                    return False

        return False

    async def run(self):
        """Main monitoring loop with browser restarts"""
        self.logger.info("🚀 Starting monitoring loop")

        try:
            await self.initialize()

            cycle_count = 0
            categories_checked_since_restart = 0

            while True:
                start_time = time.time()
                cycle_count += 1

                self.logger.info(f"\n{'='*70}")
                self.logger.info(f"🔄 CYCLE {cycle_count} STARTING")
                self.logger.info(f"{'='*70}\n")

                # Reload subscriptions
                self.subscription_manager.load_subscriptions()
                self.logger.info(f"👥 Loaded {len(self.subscription_manager.subscriptions)} active subscriptions")

                # Cleanup old subscriptions every 10 cycles
                if cycle_count % 10 == 0:
                    removed = self.subscription_manager.cleanup_old_subscriptions()
                    if removed > 0:
                        self.logger.info(f"🗑️ Cleanup: Removed {removed} old subscriptions")

                # Monitor all categories
                for category_key in DMV_CATEGORIES.keys():
                    try:
                        # 🔧 КРИТИЧЕСКИ ВАЖНО: Перезапуск браузера после N категорий
                        if categories_checked_since_restart >= self.config.browser_restart_after_categories:
                            self.logger.info(f"🔄 Restarting browser after {categories_checked_since_restart} categories...")
                            await self.scraper.restart_browser()
                            categories_checked_since_restart = 0

                        success = await self.monitor_category(category_key)

                        if success:
                            categories_checked_since_restart += 1
                        else:
                            # Если категория не удалась, попробуем перезапустить браузер
                            self.logger.warning(f"⚠️ Category failed, attempting browser restart...")
                            await self.scraper.restart_browser()
                            categories_checked_since_restart = 0

                    except Exception as e:
                        self.logger.error(f"❌ Error in category {category_key}: {e}")
                        # При любой ошибке - перезапускаем браузер
                        try:
                            await self.scraper.restart_browser()
                            categories_checked_since_restart = 0
                        except:
                            pass
                        continue

                elapsed = time.time() - start_time
                sleep_time = max(0, self.config.check_interval_sec - elapsed)

                self.logger.info(f"\n{'='*70}")
                self.logger.info(f"✅ CYCLE {cycle_count} COMPLETED in {elapsed:.1f}s")
                self.logger.info(f"😴 Sleeping for {sleep_time:.1f}s")
                self.logger.info(f"{'='*70}\n")

                await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            self.logger.info("⛔ Monitoring stopped by user")
        except Exception as e:
            self.logger.error(f"💥 Fatal error in monitoring loop: {e}")
            raise
        finally:
            await self.scraper.close()


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    """Main entry point"""
    config = Config()

    setup_logging(config)

    config.data_dir.mkdir(parents=True, exist_ok=True)

    # Check VAPID keys
    if config.vapid_private_key == "YOUR_PRIVATE_KEY_HERE":
        print("=" * 80)
        print("ERROR: VAPID KEYS NOT CONFIGURED!")
        print("Generate keys with: python -c \"from pywebpush import webpush; print(webpush.generate_vapid_keys())\"")
        print("=" * 80)
        return

    service = DMVMonitorService(config)
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())