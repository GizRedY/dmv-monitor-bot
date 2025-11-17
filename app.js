// App state
const state = {
    platform: null,
    selectedCategory: null,
    selectedLocations: [],
    userId: null,
    subscription: null
};

// Common NC DMV locations
const NC_LOCATIONS = [
    "Raleigh", "Durham", "Chapel Hill", "Cary", "Charlotte", "Wilmington",
    "Asheville", "Greensboro", "Winston-Salem", "Fayetteville", "Apex",
    "Garner", "Morrisville", "Wake Forest", "Knightdale", "Clayton",
    "Holly Springs", "Fuquay-Varina", "Roxboro", "Oxford", "Henderson"
];

// API base URL
const API_URL = window.location.origin;

// ============================================================================
// SCREEN MANAGEMENT
// ============================================================================

function showScreen(screenName) {
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    document.getElementById(`screen-${screenName}`).classList.add('active');
    window.scrollTo(0, 0);
    
    // Load data when entering specific screens
    if (screenName === 'category') {
        loadCategories();
    } else if (screenName === 'locations') {
        loadLocations();
    }
}

// Make showScreen available globally
window.showScreen = showScreen;

// ============================================================================
// PLATFORM SELECTION
// ============================================================================

function selectPlatform(platform) {
    state.platform = platform;
    
    // Show appropriate setup instructions
    document.querySelectorAll('.setup-instructions').forEach(el => {
        el.style.display = 'none';
    });
    document.getElementById(`setup-${platform}`).style.display = 'block';
    
    showScreen('setup');
}

// Make selectPlatform available globally
window.selectPlatform = selectPlatform;

// ============================================================================
// NOTIFICATION PERMISSION
// ============================================================================

async function requestNotificationPermission() {
    if (!('Notification' in window)) {
        return { granted: false, error: 'Your browser does not support notifications' };
    }
    
    if (Notification.permission === 'granted') {
        return { granted: true };
    }
    
    if (Notification.permission !== 'denied') {
        const permission = await Notification.requestPermission();
        if (permission === 'granted') {
            return { granted: true };
        } else {
            return { granted: false, error: 'Please allow notifications to receive appointment alerts' };
        }
    }
    
    return { granted: false, error: 'Notifications are blocked. Please enable them in your browser settings' };
}

// ============================================================================
// CATEGORY SELECTION
// ============================================================================

async function loadCategories() {
    try {
        const response = await fetch(`${API_URL}/categories`);
        const categories = await response.json();
        
        const container = document.getElementById('categoryList');
        container.innerHTML = '';
        
        categories.forEach(cat => {
            const div = document.createElement('div');
            div.className = 'category-item';
            div.onclick = () => selectCategory(cat.key, div);
            div.innerHTML = `
                <h4>${cat.name}</h4>
                <p>${cat.description}</p>
            `;
            container.appendChild(div);
        });
    } catch (error) {
        console.error('Error loading categories:', error);
        showAlert('Failed to load categories', 'error');
    }
}

function selectCategory(categoryKey, element) {
    // Deselect all
    document.querySelectorAll('.category-item').forEach(el => {
        el.classList.remove('selected');
    });
    
    // Select this one
    element.classList.add('selected');
    state.selectedCategory = categoryKey;
    
    // Enable next button
    document.getElementById('categoryNextBtn').disabled = false;
}

// ============================================================================
// LOCATION SELECTION
// ============================================================================

function loadLocations() {
    const grid = document.getElementById('locationGrid');
    grid.innerHTML = '';
    
    NC_LOCATIONS.forEach(location => {
        const div = document.createElement('div');
        div.className = 'location-item';
        div.textContent = location;
        div.onclick = () => toggleLocation(location, div);
        grid.appendChild(div);
    });
}

function toggleLocation(location, element) {
    element.classList.toggle('selected');
    
    const index = state.selectedLocations.indexOf(location);
    if (index > -1) {
        state.selectedLocations.splice(index, 1);
    } else {
        state.selectedLocations.push(location);
    }
    
    // Enable subscribe button if at least one location selected
    document.getElementById('subscribeBtn').disabled = state.selectedLocations.length === 0;
}

function skipLocations() {
    state.selectedLocations = [];
    subscribe();
}

// Make skipLocations available globally
window.skipLocations = skipLocations;

// ============================================================================
// SUBSCRIPTION
// ============================================================================

function showInlineError(message) {
    const errorDiv = document.getElementById('subscribeError');
    errorDiv.textContent = message;
    errorDiv.classList.add('show');
}

function hideInlineError() {
    const errorDiv = document.getElementById('subscribeError');
    errorDiv.classList.remove('show');
}

async function subscribe() {
    const btn = document.getElementById('subscribeBtn');
    const originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = '⏳ Setting up...';
    hideInlineError();
    
    try {
        // Request notification permission
        const permissionResult = await requestNotificationPermission();
        if (!permissionResult.granted) {
            showInlineError(permissionResult.error || 'Notification permission is required. Please enable notifications in your browser settings.');
            btn.disabled = false;
            btn.textContent = originalText;
            return;
        }
        
        // Register service worker for push notifications
        let pushSubscription = null;
        if ('serviceWorker' in navigator && 'PushManager' in window) {
            try {
                const registration = await navigator.serviceWorker.register('/sw.js');
                await navigator.serviceWorker.ready;
                
                // Get VAPID public key
                const vapidKey = await getVapidPublicKey();
                
                // Subscribe to push notifications
                pushSubscription = await registration.pushManager.subscribe({
                    userVisibleOnly: true,
                    applicationServerKey: urlBase64ToUint8Array(vapidKey)
                });
                
                state.subscription = pushSubscription;
            } catch (error) {
                console.error('Error setting up push:', error);
                // Continue anyway - user will still get browser notifications
            }
        }
        
        // Generate user ID (use subscription endpoint as unique ID or fallback to timestamp)
        state.userId = pushSubscription ? 
            btoa(pushSubscription.endpoint).substring(0, 50) : 
            'user_' + Date.now();
        
        // Store userId in localStorage for unsubscribe functionality
        localStorage.setItem('dmv_user_id', state.userId);
        
        // Create subscription
        const response = await fetch(`${API_URL}/subscriptions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                user_id: state.userId,
                push_subscription: pushSubscription ? JSON.stringify(pushSubscription.toJSON()) : null,
                categories: [state.selectedCategory],
                locations: state.selectedLocations,
                date_range_days: 30
            })
        });
        
        if (!response.ok) {
            throw new Error('Failed to create subscription');
        }
        
        // Show success screen
        showSuccessScreen();
        
    } catch (error) {
        console.error('Error subscribing:', error);
        showInlineError('Failed to set up notifications: ' + error.message);
        btn.disabled = false;
        btn.textContent = originalText;
    }
}

// Make subscribe available globally
window.subscribe = subscribe;

function showSuccessScreen() {
    // Get category name
    const categoryElement = document.querySelector('.category-item.selected h4');
    const categoryName = categoryElement ? categoryElement.textContent : state.selectedCategory;
    
    document.getElementById('successCategory').textContent = 
        `Category: ${categoryName}`;
    
    document.getElementById('successLocations').textContent = 
        state.selectedLocations.length > 0 ?
        `Locations: ${state.selectedLocations.join(', ')}` :
        `Locations: All locations in NC`;
    
    showScreen('success');
}

// ============================================================================
// UNSUBSCRIBE
// ============================================================================

async function unsubscribe() {
    if (!confirm('Are you sure you want to unsubscribe from DMV appointment notifications?')) {
        return;
    }
    
    try {
        const userId = state.userId || localStorage.getItem('dmv_user_id');
        
        if (!userId) {
            showAlert('No subscription found', 'error');
            return;
        }
        
        // Delete subscription from server
        const response = await fetch(`${API_URL}/subscriptions/${userId}`, {
            method: 'DELETE'
        });
        
        if (!response.ok && response.status !== 404) {
            throw new Error('Failed to delete subscription');
        }
        
        // Unregister push subscription if it exists
        if ('serviceWorker' in navigator && 'PushManager' in window) {
            try {
                const registration = await navigator.serviceWorker.getRegistration();
                if (registration) {
                    const subscription = await registration.pushManager.getSubscription();
                    if (subscription) {
                        await subscription.unsubscribe();
                    }
                }
            } catch (error) {
                console.error('Error unsubscribing from push:', error);
            }
        }
        
        // Clear state
        state.userId = null;
        state.subscription = null;
        localStorage.removeItem('dmv_user_id');
        
        showAlert('Successfully unsubscribed from notifications', 'success');
        
        // Go back to home after 2 seconds
        setTimeout(() => {
            location.reload();
        }, 2000);
        
    } catch (error) {
        console.error('Error unsubscribing:', error);
        showAlert('Failed to unsubscribe: ' + error.message, 'error');
    }
}

// Make unsubscribe available globally
window.unsubscribe = unsubscribe;

// ============================================================================
// TEST NOTIFICATION
// ============================================================================

async function testNotification() {
    try {
        if (Notification.permission === 'granted') {
            new Notification('🧪 Test Notification', {
                body: '✅ Your notifications are working! You will receive alerts here when DMV appointments become available.',
                icon: '/icon-192.png',
                badge: '/icon-192.png',
                tag: 'test-notification'
            });
            
            showAlert('Test notification sent!', 'success');
        } else {
            showAlert('Please allow notifications first', 'error');
        }
    } catch (error) {
        console.error('Error sending test notification:', error);
        showAlert('Failed to send test notification', 'error');
    }
}

// Make testNotification available globally
window.testNotification = testNotification;

// ============================================================================
// HELPERS
// ============================================================================

function showAlert(message, type = 'info') {
    const alert = document.getElementById('alert');
    alert.textContent = message;
    alert.className = `alert ${type}`;
    alert.style.display = 'block';
    
    setTimeout(() => {
        alert.style.display = 'none';
    }, 5000);
}

// VAPID key conversion
function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding)
        .replace(/\-/g, '+')
        .replace(/_/g, '/');
    
    const rawData = window.atob(base64);
    const outputArray = new Uint8Array(rawData.length);
    
    for (let i = 0; i < rawData.length; ++i) {
        outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
}

async function getVapidPublicKey() {
    try {
        const response = await fetch(`${API_URL}/vapid-public-key`);
        const data = await response.json();
        return data.public_key;
    } catch (error) {
        console.error('Error getting VAPID key:', error);
        throw error;
    }
}

// ============================================================================
// INITIALIZATION
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    console.log('DMV Monitor initialized');
    
    // Check if already installed as PWA
    if (window.matchMedia('(display-mode: standalone)').matches) {
        console.log('Running as installed PWA');
    }
    
    // Check notification support
    if (!('Notification' in window)) {
        console.warn('This browser does not support notifications');
    } else {
        console.log('Notification permission:', Notification.permission);
    }
});