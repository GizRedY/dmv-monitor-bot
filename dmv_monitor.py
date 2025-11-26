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
    check_interval_sec: int = 300  # 🔧 УВЕЛИЧЕНО: 5 минут между проверками
    base_city: str = "Raleigh"
    base_coords: Tuple[float, float] = (35.787743, -78.644257)

    # Browser settings - 🔧 КРИТИЧЕСКИ ВАЖНО ДЛЯ СТАБИЛЬНОСТИ
    headless: bool = True
    page_timeout: int = 30000
    navigation_timeout: int = 25000

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
    log_level: str = "INFO"  # 🔧 Изменено на INFO /  WARNINGдля лучшей диагностики

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

    def to_dict(self):
        return {
            "location_name": self.location_name,
            "category": self.category,
            "slots": [slot.to_dict() for slot in self.slots],
            "last_checked": self.last_checked.isoformat()
        }


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

# 🔧 НОВОЕ: Разделяем категории на две группы для параллельной работы
DMV_CATEGORIES_GROUP_1 = {
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
    }
}

DMV_CATEGORIES_GROUP_2 = {
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

# Объединённый словарь для совместимости с остальным кодом
DMV_CATEGORIES = {**DMV_CATEGORIES_GROUP_1, **DMV_CATEGORIES_GROUP_2}


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(config: Config):
    """Setup logging configuration with separate files for each browser"""
    config.log_file.parent.mkdir(parents=True, exist_ok=True)

    import sys

    # 🔧 НАСТРОЙКА: Основной root logger (для общих событий)
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.log_level.upper()))
    root_logger.handlers.clear()  # Очищаем старые обработчики

    # 📁 Общий лог-файл (main events)
    main_handler = logging.FileHandler(config.log_file, encoding='utf-8')
    main_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    main_handler.setFormatter(main_formatter)
    main_handler.addFilter(lambda record: not record.name.startswith('Browser'))  # 👈 Исключаем браузеры
    root_logger.addHandler(main_handler)

    # 📁 Лог для Browser 1
    browser1_logger = logging.getLogger("Browser1")
    browser1_logger.setLevel(getattr(logging, config.log_level.upper()))
    browser1_logger.propagate = False  # Не передавать в root
    browser1_handler = logging.FileHandler(config.log_file.parent / "browser1.log", encoding='utf-8')
    browser1_handler.setFormatter(main_formatter)
    browser1_logger.addHandler(browser1_handler)

    # 📁 Лог для Browser 2
    browser2_logger = logging.getLogger("Browser2")
    browser2_logger.setLevel(getattr(logging, config.log_level.upper()))
    browser2_logger.propagate = False  # Не передавать в root
    browser2_handler = logging.FileHandler(config.log_file.parent / "browser2.log", encoding='utf-8')
    browser2_handler.setFormatter(main_formatter)
    browser2_logger.addHandler(browser2_handler)

    # 🖥️ Консольный вывод (для всех логов)
    if sys.platform == 'win32':
        try:
            import codecs
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
            sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
            console_handler = logging.StreamHandler(sys.stdout)
        except:
            console_handler = logging.StreamHandler()
    else:
        console_handler = logging.StreamHandler()

    console_handler.setFormatter(main_formatter)
    root_logger.addHandler(console_handler)
    browser1_logger.addHandler(console_handler)
    browser2_logger.addHandler(console_handler)

    logging.info("✅ Logging configured: dmv_monitor.log, browser1.log, browser2.log")


# ============================================================================
# NOTIFICATION SERVICE
# ============================================================================

class NotificationService:
    """Handles sending notifications to users via browser push"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("NotificationService")

    def send_push_notification(self, subscription: UserSubscription, title: str, body: str, url: str = "/") -> tuple[
        bool, Optional[str]]:
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
        """Reset failed attempts counter after successful notification"""
        if user_id in self.subscriptions:
            self.subscriptions[user_id].failed_attempts = 0
            self.save_subscriptions()

    def load_subscriptions(self):
        """Load subscriptions from file"""
        try:
            self.subscriptions = {}

            if not self.config.subscriptions_file.exists():
                self.logger.info("No subscriptions file found")
                return

            with open(self.config.subscriptions_file, 'r') as f:
                data = json.load(f)

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
        """Save subscriptions to file (atomic write)"""
        try:
            # Гарантируем, что папка существует
            self.config.data_dir.mkdir(parents=True, exist_ok=True)

            # Данные для записи — как и раньше: список словарей
            data = [sub.to_dict() for sub in self.subscriptions.values()]

            # Временный файл рядом с основным
            tmp_path = self.config.subscriptions_file.with_suffix(
                self.config.subscriptions_file.suffix + ".tmp"
            )

            # 1) Пишем во временный файл
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            # 2) Атомарно заменяем старый файл новым
            os.replace(tmp_path, self.config.subscriptions_file)

            self.logger.debug(f"Saved {len(self.subscriptions)} subscriptions (atomic)")
        except Exception as e:
            # ВАЖНО: если что-то пошло не так, старый subscriptions.json останется нетронутым
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
        """Update last notification timestamp"""
        if user_id in self.subscriptions:
            self.subscriptions[user_id].last_notification_sent = datetime.now()
            self.save_subscriptions()

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

    def __init__(self, config: Config, browser_id: int = 1):  # 👈 Добавили browser_id
        self.config = config
        self.browser_id = browser_id
        self.logger = logging.getLogger(f"Browser{browser_id}")
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.playwright = None
        self.context = None

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
                await asyncio.sleep(2)  # Увеличенная пауза
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

    async def safe_click(self, locator, element_name="element", max_retries=2):
        """Безопасный клик с повторными попытками - УСКОРЕННАЯ ВЕРСИЯ"""
        for attempt in range(max_retries):
            try:
                if await self.wait_for_element_ready(locator, timeout=4000):  # 🚀 4 сек вместо 5
                    await locator.click(timeout=3000)  # 🚀 3 сек вместо 5
                    self.logger.info(f"✅ Successfully clicked on {element_name}")
                    return True
                else:
                    self.logger.warning(f"⚠️ Attempt {attempt + 1}: {element_name} not ready")
            except Exception as e:
                self.logger.warning(f"⚠️ Attempt {attempt + 1} to click {element_name} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)  # 🚀 1 сек вместо 2

        self.logger.error(f"❌ Failed to click on {element_name} after {max_retries} attempts")
        return False

    async def navigate_to_category(self, category_key: str) -> bool:
        """Navigate to a specific category - IMPROVED with better waiting"""
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

            # 🔥 НОВОЕ: Ждём стабилизации страницы после загрузки
            await asyncio.sleep(2)

            # 🔥 УЛУЧШЕНО: Первая кнопка "Make an Appointment" - с увеличенным ожиданием
            try:
                # Пробуем разные селекторы
                make_btn_selectors = [
                    "#cmdMakeAppt",
                    "text=Make an Appointment",
                    "button:has-text('Make an Appointment')",
                    "input[value='Make an Appointment']"
                ]

                make_btn = None
                for selector in make_btn_selectors:
                    try:
                        candidate = self.page.locator(selector).first
                        if await candidate.count() > 0:
                            # 🔥 Ждём дольше - до 10 секунд
                            await candidate.wait_for(state="visible", timeout=10000)
                            make_btn = candidate
                            self.logger.debug(f"✅ Found button with selector: {selector}")
                            break
                    except:
                        continue

                if not make_btn:
                    self.logger.error("❌ Could not find Make an Appointment button")
                    return False

                # 🔥 НОВОЕ: Увеличенный таймаут клика до 5 попыток
                clicked = False
                for attempt in range(5):  # Было 2, стало 5
                    try:
                        await make_btn.click(timeout=5000)
                        clicked = True
                        self.logger.info(f"✅ Successfully clicked Make an Appointment button (attempt {attempt + 1})")
                        break
                    except Exception as e:
                        if attempt < 4:
                            self.logger.warning(f"⚠️ Click attempt {attempt + 1} failed, retrying...")
                            await asyncio.sleep(2)
                        else:
                            self.logger.error(f"❌ All click attempts failed: {e}")
                            return False

                if not clicked:
                    return False

            except Exception as e:
                self.logger.error(f"❌ Error with first Make an Appointment button: {e}")
                return False

            # Ждём загрузки
            await self.page.wait_for_load_state("networkidle", timeout=40000)
            await asyncio.sleep(2)

            # Вторая кнопка "Make an Appointment" (если есть)
            try:
                second_make = self.page.locator("input.next-button[value='Make an Appointment']")
                if await second_make.is_visible(timeout=5000):
                    await second_make.click(timeout=5000)
                    self.logger.info("✅ Clicked second Make an Appointment button")
                    await self.page.wait_for_load_state("networkidle", timeout=40000)
                    await asyncio.sleep(2)
            except Exception as e:
                self.logger.debug(f"No second button or error: {e}")

            # OK button
            try:
                ok_btn = self.page.get_by_role("button", name=re.compile(r"^ok$", re.I))
                if await ok_btn.is_visible(timeout=5000):
                    await ok_btn.click(timeout=5000)
                    self.logger.info("✅ Clicked OK button")
                    await asyncio.sleep(2)
            except Exception as e:
                self.logger.debug(f"No OK button or error: {e}")

            self.logger.info(f"🔍 Selecting category: {category_name}")
            await asyncio.sleep(2)

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
                        await candidate.wait_for(state="visible", timeout=8000)
                        await candidate.click(timeout=5000)
                        clicked = True
                        self.logger.info(f"✅ Successfully clicked category: {category_name}")
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
                await self.page.wait_for_function("""
                    () => {
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

    async def get_appointment_slots(self, location_name: str) -> List[TimeSlot]:
        """Get available appointment slots for a location - FIXED VERSION with navigation protection"""
        slots = []

        try:
            self.logger.info(f"🔍 Checking slots for: {location_name}")

            # 🔧 УЛУЧШЕНО: Больше попыток клика (до 4 раз)
            clicked = False
            max_click_attempts = 4

            for attempt in range(max_click_attempts):
                try:
                    self.logger.debug(f"🎯 Click attempt {attempt + 1}/{max_click_attempts} for {location_name}")

                    # Ищем элемент с названием локации
                    selectors = [
                        f".QflowObjectItem:has-text('{location_name}')",
                        f"div:has-text('{location_name}')",
                        f"[title*='{location_name}']",
                    ]

                    for selector in selectors:
                        try:
                            elements = self.page.locator(selector)
                            count = await elements.count()

                            if count > 0:
                                for i in range(count):
                                    element = elements.nth(i)

                                    try:
                                        await element.wait_for(state="visible", timeout=3000)
                                    except:
                                        continue

                                    text = await element.inner_text()

                                    if location_name.lower() in text.lower() and "sorry" not in text.lower():
                                        await element.scroll_into_view_if_needed()
                                        await asyncio.sleep(0.5)

                                        # 🔥 КРИТИЧНО: Слушаем навигацию ПЕРЕД кликом
                                        async with self.page.expect_navigation(timeout=15000,
                                                                               wait_until="domcontentloaded"):
                                            await element.click(timeout=5000)

                                        clicked = True
                                        self.logger.info(f"✅ Successfully clicked on {location_name}")
                                        break

                            if clicked:
                                break
                        except Exception as e:
                            self.logger.debug(f"Selector {selector} failed: {e}")
                            continue

                    if clicked:
                        break

                    if attempt < max_click_attempts - 1:
                        self.logger.warning(f"⚠️ Attempt {attempt + 1} failed for {location_name}, retrying...")
                        await asyncio.sleep(2)

                except Exception as e:
                    self.logger.warning(f"⚠️ Click attempt {attempt + 1} failed: {e}")
                    if attempt < max_click_attempts - 1:
                        await asyncio.sleep(2)

            if not clicked:
                self.logger.warning(
                    f"❌ Could not click on location after {max_click_attempts} attempts: {location_name}")
                return slots

            # 🔥 НОВОЕ: Дополнительное ожидание стабилизации страницы
            self.logger.debug(f"⏳ Waiting for page to stabilize after clicking {location_name}...")
            await asyncio.sleep(3)

            # 🔥 НОВОЕ: Проверяем, что страница не закрылась
            try:
                await self.page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception as e:
                self.logger.error(f"❌ Page load state error for {location_name}: {e}")
                return slots

            # 🔥 НОВОЕ: Защита от "context destroyed" - проверяем доступность страницы
            try:
                # Простой тест - можем ли мы получить title
                _ = await self.page.title()
            except Exception as e:
                self.logger.error(f"❌ Page context lost for {location_name}: {e}")
                return slots

            # Extract appointment data
            try:
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
            except Exception as eval_error:
                self.logger.error(f"❌ Error evaluating appointment data for {location_name}: {eval_error}")
                # Пробуем вернуться назад, так как страница может быть битой
                try:
                    await self.page.go_back(wait_until="domcontentloaded", timeout=10000)
                    await asyncio.sleep(2)
                except:
                    pass
                return slots

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

            # 🔧 КРИТИЧНО: Возврат назад с проверкой
            try:
                self.logger.debug(f"🔙 Returning to location list from {location_name}")

                # Пробуем найти кнопку Back
                back_btn = self.page.locator('button:has-text("Back"), input[value="Back"]').first

                if await back_btn.is_visible(timeout=3000):
                    # 🔥 НОВОЕ: Ожидаем навигацию при клике на Back
                    async with self.page.expect_navigation(timeout=15000, wait_until="domcontentloaded"):
                        await back_btn.click(timeout=5000)
                    self.logger.debug("✅ Clicked Back button")
                else:
                    self.logger.debug("⚠️ Back button not found, using browser back")
                    await self.page.go_back(wait_until="domcontentloaded", timeout=15000)

                # 🔥 НОВОЕ: Ждём возврата на страницу со списком локаций
                await self.page.wait_for_function("""
                    () => {
                        const tiles = document.querySelectorAll('.QflowObjectItem');
                        return tiles.length > 5;
                    }
                """, timeout=15000)

                await asyncio.sleep(2)
                self.logger.debug("✅ Successfully returned to location list")

            except Exception as back_error:
                self.logger.warning(f"⚠️ Error going back from {location_name}: {back_error}")
                # Если не получилось вернуться - пробуем жёсткий вариант
                try:
                    await self.page.go_back(wait_until="domcontentloaded", timeout=10000)
                    await asyncio.sleep(2)
                except:
                    self.logger.error(f"❌ Could not return to location list, may cause issues")

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
        self.subscription_manager = SubscriptionManager(config)
        self.notification_service = NotificationService(config)
        self.last_seen_slots: Dict[str, Set[str]] = {}
        self.current_availability: Dict[str, dict] = {}


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

        self.logger.debug(f"📝 Updating availability entry: {key} with {len(availability.slots)} slots")

        self.current_availability[key] = {
            "category": availability.category,
            "location_name": availability.location_name,
            "slots_count": len(availability.slots),
            "last_checked": availability.last_checked.isoformat()
        }

    async def monitor_category(self, category_key: str, scraper: DMVScraper) -> bool:
        """Monitor a single category using PROVIDED scraper"""
        max_retries = 3  # 🔥 Максимум попыток при ошибке

        for attempt in range(max_retries):
            try:
                self.logger.info(f"{'=' * 60}")
                self.logger.info(f"📂 Monitoring category: {category_key} (attempt {attempt + 1}/{max_retries})")
                self.logger.info(f"{'=' * 60}")

                if not await scraper.navigate_to_category(category_key):
                    self.logger.error(f"❌ Failed to navigate to category: {category_key}")

                    # 🔥 При провале навигации - перезапускаем браузер
                    if attempt < max_retries - 1:
                        self.logger.warning(f"🔄 Restarting browser after navigation failure...")
                        await scraper.restart_browser()
                        await asyncio.sleep(5)
                        continue
                    else:
                        return False

                available_locations = await scraper.get_available_locations()

                if not available_locations:
                    self.logger.info(f"🔭 No available locations for category: {category_key}")

                    # Записываем ВСЕ локации NC с 0 слотами
                    for location in ALL_NC_LOCATIONS:
                        availability = LocationAvailability(
                            location_name=location,
                            category=category_key,
                            slots=[]
                        )
                        self._update_availability_entry(availability)

                    self.logger.info(f"📝 Recorded all {len(ALL_NC_LOCATIONS)} NC locations with 0 slots")
                    self._save_current_availability()
                    return True

                self.logger.info(f"✅ Found {len(available_locations)} available locations for {category_key}")

                # Сначала записываем ВСЕ локации с 0 слотами
                for location in ALL_NC_LOCATIONS:
                    availability = LocationAvailability(
                        location_name=location,
                        category=category_key,
                        slots=[]
                    )
                    self._update_availability_entry(availability)

                # Теперь проверяем доступные локации
                locations_checked = 0
                for location in available_locations:
                    try:
                        self.logger.info(f"🔍 Checking slots for {location} in {category_key}")
                        slots = await scraper.get_appointment_slots(location)
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

                            if new_slots:
                                self.logger.info(f"🎉 NEW SLOTS FOUND for {location}: {len(new_slots)} new slots!")

                                interested_users = self.subscription_manager.get_interested_users(
                                    category_key, location
                                )

                                self.logger.info(f"👥 Found {len(interested_users)} interested users")

                                for user in interested_users:
                                    success, error_type = self.notification_service.notify_user(user, availability)

                                    if success:
                                        self.logger.info(f"✅ Successfully notified user {user.user_id}")
                                        self.subscription_manager.update_last_notification(user.user_id)
                                        self.subscription_manager.reset_failed_attempts(user.user_id)
                                    elif error_type == 'invalid_subscription':
                                        self.logger.info(f"🗑️ Removing invalid subscription for user {user.user_id}")
                                        self.subscription_manager.remove_subscription(user.user_id)
                                    else:
                                        self.logger.warning(f"⚠️ Failed to notify user {user.user_id}")
                                        self.subscription_manager.increment_failed_attempts(user.user_id)

                                        if user.failed_attempts >= 3:
                                            self.logger.info(
                                                f"🗑️ Removing subscription after 3 failed attempts: {user.user_id}")
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
                        await scraper.restart_browser()
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
        """Main monitoring loop with TWO INDEPENDENT browsers"""
        self.logger.info("🚀 Starting DUAL-BROWSER monitoring loop")

        # 🔥 Убираем старый self.scraper, если он есть
        if hasattr(self, 'scraper'):
            try:
                await self.scraper.close()
            except:
                pass
            delattr(self, 'scraper')

        # 🔥 СОЗДАЁМ ДВА ПОЛНОСТЬЮ НЕЗАВИСИМЫХ SCRAPER'А
        scraper1 = DMVScraper(self.config, browser_id=1)
        scraper2 = DMVScraper(self.config, browser_id=2)

        try:
            # Инициализируем оба браузера
            await scraper1.initialize()
            await scraper2.initialize()
            self.logger.info("✅ Both INDEPENDENT browsers initialized")

            cycle_count = 0

            while True:
                start_time = time.time()
                cycle_count += 1

                self.logger.info(f"\n{'=' * 70}")
                self.logger.info(f"🔄 DUAL CYCLE {cycle_count} STARTING")
                self.logger.info(f"{'=' * 70}\n")

                # Reload subscriptions
                self.subscription_manager.load_subscriptions()
                self.logger.info(f"👥 Loaded {len(self.subscription_manager.subscriptions)} subscriptions")

                # Cleanup every 10 cycles
                if cycle_count % 10 == 0:
                    removed = self.subscription_manager.cleanup_old_subscriptions()
                    if removed > 0:
                        self.logger.info(f"🗑️ Cleanup: Removed {removed} old subscriptions")

                # 🔥 ДВЕ ПОЛНОСТЬЮ НЕЗАВИСИМЫЕ ФУНКЦИИ
                async def process_group_1():
                    """Browser 1: первая половина категорий - НЕЗАВИСИМЫЙ"""
                    categories_checked = 0
                    for category_key in DMV_CATEGORIES_GROUP_1.keys():
                        try:
                            scraper1.logger.info(f"Starting category: {category_key}")

                            # Перезапуск после 3 категорий
                            if categories_checked >= 3:
                                scraper1.logger.info("🔄 Restarting browser after 3 categories")
                                await scraper1.restart_browser()
                                categories_checked = 0

                            # 👇 ПЕРЕДАЁМ scraper1 явно!
                            success = await self.monitor_category(category_key, scraper1)

                            if success:
                                categories_checked += 1
                            else:
                                scraper1.logger.warning(f"❌ Category {category_key} failed, restarting browser")
                                await scraper1.restart_browser()
                                categories_checked = 0

                        except Exception as e:
                            scraper1.logger.error(f"❌ Error in {category_key}: {e}", exc_info=True)
                            try:
                                await scraper1.restart_browser()
                                categories_checked = 0
                            except:
                                pass

                async def process_group_2():
                    """Browser 2: вторая половина категорий - НЕЗАВИСИМЫЙ"""
                    categories_checked = 0
                    for category_key in DMV_CATEGORIES_GROUP_2.keys():
                        try:
                            scraper2.logger.info(f"Starting category: {category_key}")

                            # Перезапуск после 3 категорий
                            if categories_checked >= 3:
                                scraper2.logger.info("🔄 Restarting browser after 3 categories")
                                await scraper2.restart_browser()
                                categories_checked = 0

                            # 👇 ПЕРЕДАЁМ scraper2 явно!
                            success = await self.monitor_category(category_key, scraper2)

                            if success:
                                categories_checked += 1
                            else:
                                scraper2.logger.warning(f"❌ Category {category_key} failed, restarting browser")
                                await scraper2.restart_browser()
                                categories_checked = 0

                        except Exception as e:
                            scraper2.logger.error(f"❌ Error in {category_key}: {e}", exc_info=True)
                            try:
                                await scraper2.restart_browser()
                                categories_checked = 0
                            except:
                                pass

                # 🔥 ЗАПУСКАЕМ ОБА БРАУЗЕРА ПАРАЛЛЕЛЬНО
                await asyncio.gather(
                    process_group_1(),
                    process_group_2()
                )

                elapsed = time.time() - start_time
                sleep_time = max(0, self.config.check_interval_sec - elapsed)

                self.logger.info(f"\n{'=' * 70}")
                self.logger.info(f"✅ DUAL CYCLE {cycle_count} COMPLETED in {elapsed:.1f}s")
                self.logger.info(f"😴 Sleeping for {sleep_time:.1f}s")
                self.logger.info(f"{'=' * 70}\n")

                await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            self.logger.info("⛔ Monitoring stopped by user")
        except Exception as e:
            self.logger.error(f"💥 Fatal error: {e}", exc_info=True)
            raise
        finally:
            await scraper1.close()
            await scraper2.close()


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