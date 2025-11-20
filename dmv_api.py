"""
REST API for DMV Monitor - Browser Push Notifications
Enhanced with subscription management
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import json
from pathlib import Path
import logging
from pywebpush import webpush, WebPushException
import os
from datetime import datetime, timedelta

app = FastAPI(title="DMV Monitor API", version="2.0.0")
logger = logging.getLogger("API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/data", StaticFiles(directory="data"), name="data")

# Data directory
DATA_DIR = Path("./data")
SUBSCRIPTIONS_FILE = DATA_DIR / "subscriptions.json"
AVAILABILITY_FILE = DATA_DIR / "last_check.json"


VAPID_PRIVATE_KEY = "pK7ehUTOBpbL0ilLgPntwnvMPBvjQYXEjrQWz1xRAtg"
VAPID_PUBLIC_KEY = "BJf7Zamd5ty_QAuk2o5PwDpMPvutYdk-EG-FgtNaodREIOFRj1MTRXRznug45wAHonmkeXgfsFsLyXNq8k8uY-A"
VAPID_CLAIMS = {
    "sub": "mailto:activation.service.mailbox@gmail.com"
}

# DMV Categories - copied here to avoid circular import
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
# REQUEST/RESPONSE MODELS
# ============================================================================

class PushSubscriptionKeys(BaseModel):
    """Push subscription keys"""
    p256dh: str
    auth: str


class PushSubscriptionInfo(BaseModel):
    """Browser push subscription info"""
    endpoint: str
    keys: PushSubscriptionKeys


class SubscriptionRequest(BaseModel):
    """Request to create/update subscription"""
    user_id: str
    push_subscription: Optional[str] = None  # JSON string of push subscription
    categories: List[str] = []
    locations: List[str] = []
    date_range_days: int = 30


class SubscriptionResponse(BaseModel):
    """Subscription response"""
    user_id: str
    categories: List[str]
    locations: List[str]
    date_range_days: int
    created_at: str


class CategoryInfo(BaseModel):
    """DMV category information"""
    key: str
    name: str
    description: str


class StatusResponse(BaseModel):
    """Service status"""
    status: str
    total_subscriptions: int
    active_categories: List[str]


class VapidKeyResponse(BaseModel):
    """VAPID public key response"""
    public_key: str

class AvailabilityItem(BaseModel):
    """Single availability record for UI"""
    category: str
    location_name: str
    slots_count: int
    last_checked: str



# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def load_subscriptions() -> dict:
    """Load subscriptions from file"""
    try:
        if SUBSCRIPTIONS_FILE.exists():
            with open(SUBSCRIPTIONS_FILE, 'r') as f:
                return {sub['user_id']: sub for sub in json.load(f)}
        return {}
    except Exception as e:
        logger.error(f"Error loading subscriptions: {e}")
        return {}


def save_subscriptions(subscriptions: dict):
    """Save subscriptions to file"""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(SUBSCRIPTIONS_FILE, 'w') as f:
            json.dump(list(subscriptions.values()), f, indent=2)
    except Exception as e:
        logger.error(f"Error saving subscriptions: {e}")
        raise HTTPException(status_code=500, detail="Failed to save subscription")



def load_availability() -> list:
    """Load current availability snapshot from file"""
    try:
        if not AVAILABILITY_FILE.exists():
            return []

        with open(AVAILABILITY_FILE, "r") as f:
            data = json.load(f)

        if isinstance(data, list):
            return data

        # For backward compatibility if format changes
        return list(data.values())
    except Exception as e:
        logger.error(f"Error loading availability: {e}")
        return []

def send_push_notification(subscription_info: dict, title: str, body: str, url: str = "/") -> bool:
    """Send push notification to a subscriber"""
    try:
        if not subscription_info or 'push_subscription' not in subscription_info:
            logger.warning("No push subscription found")
            return False

        push_sub = json.loads(subscription_info['push_subscription'])

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
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims=VAPID_CLAIMS
        )

        logger.info(f"Push notification sent successfully")
        return True

    except WebPushException as e:
        logger.error(f"WebPush error: {e}")
        # If subscription is no longer valid, you might want to remove it
        if e.response and e.response.status_code in [404, 410]:
            logger.warning("Subscription no longer valid, should be removed")
        return False
    except Exception as e:
        logger.error(f"Error sending push notification: {e}")
        return False


# ============================================================================
# STATIC FILE SERVING
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    """Serve main HTML UI"""
    html_file = Path("./index.html")
    if html_file.exists():
        return FileResponse(html_file)
    return HTMLResponse("<h1>index.html not found</h1>", status_code=404)


@app.get("/app.js")
async def serve_app_js():
    """Serve app.js"""
    js_file = Path("./app.js")
    if js_file.exists():
        return FileResponse(js_file, media_type="application/javascript")
    return HTMLResponse("app.js not found", status_code=404)


@app.get("/sw.js")
async def serve_service_worker():
    """Serve service worker"""
    sw_file = Path("./sw.js")
    if sw_file.exists():
        return FileResponse(sw_file, media_type="application/javascript")
    return HTMLResponse("sw.js not found", status_code=404)


@app.get("/manifest.json")
async def serve_manifest():
    """Serve PWA manifest"""
    manifest_file = Path("./manifest.json")
    if manifest_file.exists():
        return FileResponse(manifest_file, media_type="application/json")
    return HTMLResponse("manifest.json not found", status_code=404)


# Icon files (you'll need to create these)
@app.get("/icon-192.png")
async def serve_icon_192():
    """Serve 192x192 icon"""
    icon_file = Path("./icon-192.png")
    if icon_file.exists():
        return FileResponse(icon_file, media_type="image/png")
    return HTMLResponse("Icon not found", status_code=404)


@app.get("/icon-512.png")
async def serve_icon_512():
    """Serve 512x512 icon"""
    icon_file = Path("./icon-512.png")
    if icon_file.exists():
        return FileResponse(icon_file, media_type="image/png")
    return HTMLResponse("Icon not found", status_code=404)


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/vapid-public-key", response_model=VapidKeyResponse)
async def get_vapid_public_key():
    """Get VAPID public key for push notifications"""
    return VapidKeyResponse(public_key=VAPID_PUBLIC_KEY)


@app.get("/status", response_model=StatusResponse)
async def get_status():
    """Get service status"""
    subscriptions = load_subscriptions()

    # Get unique categories from all subscriptions
    categories = set()
    for sub in subscriptions.values():
        categories.update(sub.get('categories', []))

    return StatusResponse(
        status="running",
        total_subscriptions=len(subscriptions),
        active_categories=list(categories)
    )


@app.get("/categories", response_model=List[CategoryInfo])
async def get_categories():
    """Get list of available DMV categories"""
    return [
        CategoryInfo(
            key=key,
            name=info['name'],
            description=info['description']
        )
        for key, info in DMV_CATEGORIES.items()
    ]



@app.get("/availability", response_model=List[AvailabilityItem])
async def get_availability():
    """Get current appointment availability snapshot for UI"""
    raw = load_availability()
    items: List[AvailabilityItem] = []

    for item in raw:
        try:
            items.append(AvailabilityItem(**item))
        except Exception:
            # Skip invalid records
            continue

    # Sort by location name for stable display
    items.sort(key=lambda x: (x.location_name.lower(), x.category))
    return items

@app.post("/subscriptions", response_model=SubscriptionResponse)
async def create_subscription(subscription: SubscriptionRequest):
    """Create or update a subscription"""
    # Load existing subscriptions
    subscriptions = load_subscriptions()

    # Get existing created_at or set new one
    created_at = datetime.now().isoformat()
    if subscription.user_id in subscriptions:
        created_at = subscriptions[subscription.user_id].get('created_at', created_at)

    # Create/update subscription
    subscriptions[subscription.user_id] = {
        'user_id': subscription.user_id,
        'push_subscription': subscription.push_subscription,
        'categories': subscription.categories,
        'locations': subscription.locations,
        'date_range_days': subscription.date_range_days,
        'created_at': created_at,
        'last_notification_sent': subscriptions.get(subscription.user_id, {}).get('last_notification_sent')
    }

    # Save
    save_subscriptions(subscriptions)

    logger.info(f"Subscription created/updated for user: {subscription.user_id}")

    return SubscriptionResponse(
        user_id=subscription.user_id,
        categories=subscription.categories,
        locations=subscription.locations,
        date_range_days=subscription.date_range_days,
        created_at=created_at
    )


@app.get("/subscriptions/{user_id}", response_model=SubscriptionResponse)
async def get_subscription(user_id: str):
    """Get a specific subscription"""
    subscriptions = load_subscriptions()

    if user_id not in subscriptions:
        raise HTTPException(status_code=404, detail="Subscription not found")

    sub = subscriptions[user_id]
    return SubscriptionResponse(
        user_id=sub['user_id'],
        categories=sub.get('categories', []),
        locations=sub.get('locations', []),
        date_range_days=sub.get('date_range_days', 30),
        created_at=sub.get('created_at', datetime.now().isoformat())
    )


@app.delete("/subscriptions/{user_id}")
async def delete_subscription(user_id: str):
    """Delete a subscription"""
    subscriptions = load_subscriptions()

    if user_id not in subscriptions:
        raise HTTPException(status_code=404, detail="Subscription not found")

    del subscriptions[user_id]
    save_subscriptions(subscriptions)

    logger.info(f"Subscription deleted for user: {user_id}")

    return {"message": "Subscription deleted successfully"}


@app.post("/subscriptions/{user_id}/test")
async def test_notification(user_id: str):
    """Send a test notification to user"""
    subscriptions = load_subscriptions()

    if user_id not in subscriptions:
        raise HTTPException(status_code=404, detail="Subscription not found")

    sub = subscriptions[user_id]

    success = send_push_notification(
        subscription_info=sub,
        title="🧪 DMV Monitor Test",
        body="✅ Your notifications are working! You will receive alerts here when DMV appointments become available.",
        url="/"
    )

    if success:
        return {"message": "Test notification sent successfully"}
    else:
        raise HTTPException(status_code=500, detail="Failed to send notification")


# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )

    # Generate VAPID keys if not set
    if VAPID_PRIVATE_KEY == "YOUR_PRIVATE_KEY_HERE":
        logger.warning("=" * 80)
        logger.warning("VAPID KEYS NOT CONFIGURED!")
        logger.warning("Generate keys with: python -c \"from pywebpush import webpush; print(webpush.generate_vapid_keys())\"")
        logger.warning("Then set VAPID_PRIVATE_KEY and VAPID_PUBLIC_KEY environment variables")
        logger.warning("=" * 80)

    # Run server
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )