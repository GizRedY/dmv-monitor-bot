"""
DMV Monitor Server - VPS Version with Browser Push Notifications
Enhanced with automatic cleanup and subscription refresh
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

# Добавьте ПЕРЕД классом Config:

# Полный список локаций NC (из фронтенда)
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
    check_interval_sec: int = 120  # How often to check each category
    base_city: str = "Raleigh"
    base_coords: Tuple[float, float] = (35.787743, -78.644257)

    # Browser settings
    headless: bool = True
    page_timeout: int = 60000

    # Database/Storage
    data_dir: Path = Path("./data")
    subscriptions_file: Path = Path("./data/subscriptions.json")
    last_check_file: Path = Path("./data/last_check.json")

    # Cleanup settings
    subscription_max_age_days: int = 30  # Remove subscriptions older than this

    # Logging
    log_file: Path = Path("./logs/dmv_monitor.log")
    log_level: str = "INFO"

    # VAPID keys for push notifications
    vapid_private_key: str = "pK7ehUTOBpbL0ilLgPntwnvMPBvjQYXEjrQWz1xRAtg"
    vapid_public_key: str = "BJf7Zamd5ty_QAuk2o5PwDpMPvutYdk-EG-FgtNaodREIOFRj1MTRXRznug45wAHonmkeXgfsFsLyXNq8k8uY-A"
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
    push_subscription: Optional[str] = None  # JSON string of browser push subscription
    categories: Set[str] = field(default_factory=set)
    locations: Set[str] = field(default_factory=set)
    date_range_days: int = 30
    created_at: datetime = field(default_factory=datetime.now)
    last_notification_sent: Optional[datetime] = None

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "push_subscription": self.push_subscription,
            "categories": list(self.categories),
            "locations": list(self.locations),
            "date_range_days": self.date_range_days,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            "last_notification_sent": self.last_notification_sent.isoformat() if self.last_notification_sent else None
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

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.FileHandler(config.log_file),
            logging.StreamHandler()
        ]
    )


# ============================================================================
# NOTIFICATION SERVICE
# ============================================================================

class NotificationService:
    """Handles sending notifications to users via browser push"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("NotificationService")

    def send_push_notification(self, subscription: UserSubscription, title: str, body: str, url: str = "/") -> tuple[bool, Optional[str]]:
        """
        Send browser push notification
        Returns: (success: bool, error_type: Optional[str])
        error_type can be: 'invalid_subscription', 'other'
        """
        try:
            if not subscription.push_subscription:
                self.logger.warning(f"No push subscription for user {subscription.user_id}")
                return False, None

            push_sub = json.loads(subscription.push_subscription)

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
                vapid_claims=self.config.vapid_claims
            )

            self.logger.info(f"Push notification sent to user {subscription.user_id}")
            return True, None

        except WebPushException as e:
            self.logger.error(f"WebPush error for user {subscription.user_id}: {e}")
            # If subscription is no longer valid (410 Gone or 404 Not Found)
            if e.response and e.response.status_code in [404, 410]:
                self.logger.warning(f"Subscription for user {subscription.user_id} is no longer valid")
                return False, 'invalid_subscription'
            return False, 'other'
        except Exception as e:
            self.logger.error(f"Error sending push notification: {e}")
            return False, 'other'

    def notify_user(self, subscription: UserSubscription, availability: LocationAvailability) -> tuple[bool, Optional[str]]:
        """
        Notify user about new availability
        Returns: (success: bool, error_type: Optional[str])
        """
        category_name = DMV_CATEGORIES.get(availability.category, {}).get('name', availability.category)

        title = "🚗 DMV Appointment Available!"

        body_lines = [
            f"📋 {category_name}",
            f"📍 {availability.location_name}",
        ]

        # Add first few slots
        if availability.slots:
            body_lines.append(f"\n📅 Available: {availability.slots[0].date.strftime('%b %d')} at {availability.slots[0].time}")
            if len(availability.slots) > 1:
                body_lines.append(f"+ {len(availability.slots) - 1} more slots")

        body = "\n".join(body_lines)

        return self.send_push_notification(subscription, title, body)


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

    def load_subscriptions(self):
        """Load subscriptions from file, fully replacing in-memory data"""
        try:
            # Reset current in-memory subscriptions to avoid stale entries
            self.subscriptions = {}

            if not self.config.subscriptions_file.exists():
                self.logger.info("No subscriptions file found")
                return

            with open(self.config.subscriptions_file, 'r') as f:
                data = json.load(f)

            loaded_count = 0

            for user_data in data:
                try:
                    # Parse created_at
                    created_at = user_data.get('created_at')
                    if isinstance(created_at, str):
                        created_at = datetime.fromisoformat(created_at)
                    elif created_at is None:
                        created_at = datetime.now()

                    # Parse last_notification_sent
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
                        last_notification_sent=last_notification_sent
                    )
                    self.subscriptions[sub.user_id] = sub
                    loaded_count += 1
                except Exception as e:
                    self.logger.error(f"Skipping invalid subscription entry: {e}")

            self.logger.info(f"Loaded {loaded_count} subscriptions")
        except Exception as e:
            self.logger.error(f"Error loading subscriptions: {e}")

    def save_subscriptions(self):
        """Save subscriptions to file"""
        try:
            self.config.data_dir.mkdir(parents=True, exist_ok=True)
            with open(self.config.subscriptions_file, 'w') as f:
                json.dump([sub.to_dict() for sub in self.subscriptions.values()], f, indent=2)
            self.logger.debug(f"Saved {len(self.subscriptions)} subscriptions")
        except Exception as e:
            self.logger.error(f"Error saving subscriptions: {e}")

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
            self.logger.info(f"Cleaned up {len(removed)} old subscriptions (older than {self.config.subscription_max_age_days} days)")

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
            # Check if user is subscribed to this category
            if category in sub.categories or not sub.categories:  # Empty means all categories
                # Check if user is subscribed to this location
                if location in sub.locations or not sub.locations:  # Empty means all locations
                    interested.append(sub)
        return interested


# ============================================================================
# DMV SCRAPER
# ============================================================================

class DMVScraper:
    """Scrapes DMV appointment availability"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("DMVScraper")
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None

    async def initialize(self):
        """Initialize browser"""
        try:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(
                headless=self.config.headless,
                args=[
                    "--disable-dev-shm-usage",
                    "--disable-web-security",
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled"
                ]
            )

            context = await self.browser.new_context(
                geolocation={"latitude": self.config.base_coords[0], "longitude": self.config.base_coords[1]},
                permissions=["geolocation"],
                ignore_https_errors=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )

            self.page = await context.new_page()
            self.page.set_default_timeout(self.config.page_timeout)
            self.page.on("dialog", lambda d: asyncio.create_task(d.accept()))

            self.logger.info("Browser initialized successfully")

        except Exception as e:
            self.logger.error(f"Error initializing browser: {e}")
            raise

    async def navigate_to_category(self, category_key: str) -> bool:
        """Navigate to a specific category"""
        try:
            category_info = DMV_CATEGORIES.get(category_key)
            if not category_info:
                self.logger.error(f"Unknown category: {category_key}")
                return False

            category_name = category_info["name"]
            self.logger.info(f"Navigating to category: {category_name}")

            await self.page.goto(self.config.dmv_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(1.5)

            make_btn = self.page.locator("#cmdMakeAppt")
            if await make_btn.count() == 0:
                make_btn = self.page.locator("text=Make an Appointment").first
            await make_btn.click()
            await self.page.wait_for_load_state("networkidle")
            await asyncio.sleep(1.0)

            second_make = self.page.locator("input.next-button[value='Make an Appointment']")
            if await second_make.is_visible():
                await second_make.click()
                await self.page.wait_for_load_state("networkidle")
                await asyncio.sleep(1.0)

            ok_btn = self.page.get_by_role("button", name=re.compile(r"^ok$", re.I))
            if await ok_btn.is_visible():
                await ok_btn.click()
                await asyncio.sleep(1.0)

            self.logger.info(f"Selecting category: {category_name}")
            await asyncio.sleep(1.5)

            candidates = [
                self.page.locator(f"text={category_name}").first,
                self.page.locator(f"button:has-text('{category_name}')").first,
                self.page.locator(f"a:has-text('{category_name}')").first,
            ]

            clicked = False
            for candidate in candidates:
                try:
                    if await candidate.count() > 0 and await candidate.is_visible():
                        await candidate.click()
                        clicked = True
                        self.logger.info(f"Clicked on category: {category_name}")
                        break
                except Exception:
                    continue

            if not clicked:
                self.logger.error(f"Could not find category: {category_name}")
                return False

            await self.page.wait_for_load_state("networkidle")
            await asyncio.sleep(1.5)

            try:
                await self.page.wait_for_function("""
                () => {
                    const text = document.body.innerText || '';
                    return text.includes('Select a Location') || text.includes('select a location');
                }""", timeout=30000)
                self.logger.info("Reached location selection page")
                return True
            except Exception:
                self.logger.warning("Did not find 'Select a Location' text, but continuing...")
                return True

        except Exception as e:
            self.logger.error(f"Error navigating to category: {e}")
            return False

    async def get_available_locations(self) -> List[str]:
        """Get list of available locations"""
        try:
            await asyncio.sleep(2.0)

            available_locations = []
            active_tiles = self.page.locator(".QflowObjectItem.ui-selectable.Active-Unit:not(.disabled-unit)")

            count = await active_tiles.count()
            self.logger.info(f"Found {count} active location tiles")

            for i in range(count):
                try:
                    tile = active_tiles.nth(i)
                    text = await tile.inner_text()
                    lines = [line.strip() for line in text.splitlines() if line.strip()]

                    if lines:
                        location_name = lines[0]
                        if "sorry" not in location_name.lower() and "don't have" not in location_name.lower():
                            available_locations.append(location_name)

                except Exception as e:
                    self.logger.warning(f"Error processing tile {i}: {e}")
                    continue

            self.logger.info(f"Found {len(available_locations)} available locations")
            return available_locations

        except Exception as e:
            self.logger.error(f"Error getting available locations: {e}")
            return []

    async def get_appointment_slots(self, location_name: str) -> List[TimeSlot]:
        """Get available appointment slots for a location"""
        slots = []

        try:
            self.logger.info(f"Checking slots for: {location_name}")

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
                                        await element.click()
                                        clicked = True
                                        break
                        if clicked:
                            break
                except Exception:
                    continue

            if not clicked:
                self.logger.warning(f"Could not click on location: {location_name}")
                return slots

            await asyncio.sleep(3.0)

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

            self.logger.info(f"Found {len(slots)} slots for {location_name}")

            # Go back
            try:
                back_btn = self.page.locator('button:has-text("Back")').first
                if await back_btn.is_visible():
                    await back_btn.click()
                else:
                    await self.page.go_back()
                await asyncio.sleep(1.5)
            except Exception:
                await self.page.go_back()
                await asyncio.sleep(1.5)

        except Exception as e:
            self.logger.error(f"Error getting slots for {location_name}: {e}")

        return slots

    async def close(self):
        """Close browser"""
        try:
            if self.browser:
                await self.browser.close()
                self.logger.info("Browser closed")
        except Exception as e:
            self.logger.error(f"Error closing browser: {e}")


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
        self.logger.info("Initializing DMV Monitor Service")
        await self.scraper.initialize()
        self.logger.info("Service initialized successfully")

    def _save_current_availability(self):
        """Persist current availability to JSON so API can read it"""
        try:
            self.config.data_dir.mkdir(parents=True, exist_ok=True)

            availability_list = list(self.current_availability.values())

            self.logger.info(f"Saving {len(availability_list)} availability entries to {self.config.last_check_file}")

            with open(self.config.last_check_file, "w") as f:
                json.dump(availability_list, f, indent=2)

            self.logger.debug(f"Successfully saved availability data")
        except Exception as e:
            self.logger.error(f"Error saving current availability: {e}", exc_info=True)

    def _update_availability_entry(self, availability: LocationAvailability):
        """Update availability record for given category/location"""
        key = f"{availability.category}:{availability.location_name}"

        self.logger.debug(f"Updating availability entry: {key} with {len(availability.slots)} slots")

        self.current_availability[key] = {
            "category": availability.category,
            "location_name": availability.location_name,
            "slots_count": len(availability.slots),
            "last_checked": availability.last_checked.isoformat()
        }

    async def monitor_category(self, category_key: str):
        """Monitor a single category"""
        try:
            self.logger.info(f"=== Monitoring category: {category_key} ===")

            if not await self.scraper.navigate_to_category(category_key):
                self.logger.error(f"Failed to navigate to category: {category_key}")
                return

            # Получаем доступные локации с сайта
            available_locations = await self.scraper.get_available_locations()

            if not available_locations:
                self.logger.info(f"No available locations for category: {category_key}")

                # КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Записываем ВСЕ локации NC с 0 слотами
                for location in ALL_NC_LOCATIONS:
                    availability = LocationAvailability(
                        location_name=location,
                        category=category_key,
                        slots=[]  # 0 слотов
                    )
                    self._update_availability_entry(availability)

                self.logger.info(f"Recorded all {len(ALL_NC_LOCATIONS)} NC locations with 0 slots for {category_key}")
                self._save_current_availability()
                return

            self.logger.info(f"Found {len(available_locations)} available locations for {category_key}")

            # Сначала записываем ВСЕ локации с 0 слотами
            for location in ALL_NC_LOCATIONS:
                availability = LocationAvailability(
                    location_name=location,
                    category=category_key,
                    slots=[]
                )
                self._update_availability_entry(availability)

            # Теперь проверяем доступные локации и обновляем их реальными слотами
            for location in available_locations:
                try:
                    self.logger.info(f"Checking slots for {location} in {category_key}")
                    slots = await self.scraper.get_appointment_slots(location)

                    availability = LocationAvailability(
                        location_name=location,
                        category=category_key,
                        slots=slots
                    )

                    # Обновляем запись для этой локации
                    self._update_availability_entry(availability)
                    self.logger.info(f"Updated availability for {location}: {len(slots)} slots")

                    # Только если есть слоты - проверяем новые и отправляем уведомления
                    if slots:
                        key = f"{category_key}:{location}"
                        current_slots_set = {str(slot) for slot in slots}

                        if key not in self.last_seen_slots:
                            self.last_seen_slots[key] = set()

                        new_slots = current_slots_set - self.last_seen_slots[key]

                        if new_slots:
                            self.logger.info(f"NEW SLOTS FOUND for {location}: {len(new_slots)} new slots!")

                            interested_users = self.subscription_manager.get_interested_users(
                                category_key, location
                            )

                            self.logger.info(f"Found {len(interested_users)} interested users")

                            for user in interested_users:
                                success, error_type = self.notification_service.notify_user(user, availability)

                                if success:
                                    self.logger.info(f"Successfully notified user {user.user_id}")
                                    self.subscription_manager.update_last_notification(user.user_id)
                                elif error_type == 'invalid_subscription':
                                    self.logger.info(f"Removing invalid subscription for user {user.user_id}")
                                    self.subscription_manager.remove_subscription(user.user_id)
                                else:
                                    self.logger.warning(f"Failed to notify user {user.user_id}")

                            self.last_seen_slots[key] = current_slots_set
                        else:
                            self.logger.info(f"No new slots for {location} (already seen all {len(slots)} slots)")
                    else:
                        self.logger.info(f"No available slots for {location}")

                except Exception as e:
                    self.logger.error(f"Error checking location {location}: {e}", exc_info=True)
                    continue

            # После проверки всех локаций - сохраняем результат
            self.logger.info(f"Finished checking category {category_key}, saving results...")
            self._save_current_availability()

        except Exception as e:
            self.logger.error(f"Error monitoring category {category_key}: {e}", exc_info=True)

    async def run(self):
        """Main monitoring loop"""
        self.logger.info("Starting monitoring loop")

        try:
            await self.initialize()

            cycle_count = 0

            while True:
                start_time = time.time()
                cycle_count += 1

                # Reload subscriptions from file (to pick up new subscriptions)
                self.subscription_manager.load_subscriptions()
                self.logger.info(f"Cycle {cycle_count}: Loaded {len(self.subscription_manager.subscriptions)} active subscriptions")

                # Cleanup old subscriptions every 10 cycles (every ~20 minutes if check_interval is 120s)
                if cycle_count % 10 == 0:
                    removed = self.subscription_manager.cleanup_old_subscriptions()
                    if removed > 0:
                        self.logger.info(f"Cleanup: Removed {removed} old subscriptions")

                # Monitor all categories
                for category_key in DMV_CATEGORIES.keys():
                    try:
                        await self.monitor_category(category_key)
                    except Exception as e:
                        self.logger.error(f"Error in category {category_key}: {e}")
                        continue

                elapsed = time.time() - start_time
                sleep_time = max(0, self.config.check_interval_sec - elapsed)

                self.logger.info(f"Cycle {cycle_count} completed in {elapsed:.1f}s. Sleeping for {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            self.logger.info("Monitoring stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error in monitoring loop: {e}")
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
        print("Then set VAPID_PRIVATE_KEY and VAPID_PUBLIC_KEY environment variables")
        print("=" * 80)
        return

    service = DMVMonitorService(config)
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())