// ============================================================================
// APP STATE
// ============================================================================

const state = {
    platform: null,
    selectedCategories: [],
    selectedLocations: [],
    userId: null,
    subscription: null
};

const API_URL = window.location.origin;

// ============================================================================
// SCREEN MANAGEMENT
// ============================================================================

function showScreen(screenName) {
    document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
    const screenEl = document.getElementById(`screen-${screenName}`);
    if (screenEl) {
        screenEl.classList.add('active');
    }
    window.scrollTo(0, 0);

    if (screenName === 'category') {
        loadCategories();
    } else if (screenName === 'locations') {
        loadLocations();
    }
}

window.showScreen = showScreen;

// ============================================================================
// PLATFORM SELECTION
// ============================================================================

function selectPlatform(platform) {
    state.platform = platform;

    document.querySelectorAll('.setup-instructions').forEach(el => {
        el.style.display = 'none';
    });
    const block = document.getElementById(`setup-${platform}`);
    if (block) {
        block.style.display = 'block';
    }

    showScreen('setup');
}

window.selectPlatform = selectPlatform;

// ============================================================================
// NOTIFICATION PERMISSION
// ============================================================================

async function requestNotificationPermission() {
    if (!('Notification' in window)) {
        return { granted: false, error: 'Browser does not support notifications' };
    }

    if (Notification.permission === 'granted') {
        return { granted: true };
    }

    if (Notification.permission !== 'denied') {
        const permission = await Notification.requestPermission();
        return { granted: permission === 'granted' };
    }

    return { granted: false, error: 'Notifications are blocked in settings.' };
}

// ============================================================================
// LOAD CATEGORIES
// ============================================================================

async function loadCategories() {
    try {
        const response = await fetch(`${API_URL}/categories`);
        const categories = await response.json();

        const container = document.getElementById('categoryList');
        if (!container) return;

        container.innerHTML = '';

        categories.forEach(cat => {
            const div = document.createElement('div');
            div.className = 'category-item';
            if (state.selectedCategories.includes(cat.key)) {
                div.classList.add('selected');
            }

            div.onclick = () => toggleCategory(cat.key, div);
            div.innerHTML = `
                <h4>${cat.name}</h4>
                <p>${cat.description}</p>
            `;
            container.appendChild(div);
        });

        const nextBtn = document.getElementById('categoryNextBtn');
        if (nextBtn) nextBtn.disabled = state.selectedCategories.length === 0;

    } catch (error) {
        console.error('Category load error:', error);
        showAlert('Failed to load categories', 'error');
    }
}

function toggleCategory(key, element) {
    element.classList.toggle('selected');

    const idx = state.selectedCategories.indexOf(key);
    if (idx >= 0) {
        state.selectedCategories.splice(idx, 1);
    } else {
        state.selectedCategories.push(key);
    }

    const nextBtn = document.getElementById('categoryNextBtn');
    if (nextBtn) nextBtn.disabled = state.selectedCategories.length === 0;
}

// ============================================================================
// LOCATION SELECTION
// ============================================================================

const NC_LOCATIONS = [
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
    'Lumberton', 'Marion', 'Marshall', 'Mocksville', 'Monroe', 'Mooresville',
    'Morehead City', 'Morganton', 'Mount Airy', 'Mount Holly', 'Nags Head',
    'New Bern', 'Newton', 'Oxford', 'Polkton', 'Raleigh North', 'Raleigh West',
    'Roanoke Rapids', 'Rocky Mount', 'Roxboro', 'Salisbury', 'Sanford',
    'Shallotte', 'Shelby', 'Siler City', 'Smithfield', 'Statesville',
    'Stedman', 'Sylva', 'Tarboro', 'Taylorsville', 'Thomasville', 'Troy',
    'Washington', 'Wendell', 'Wentworth', 'Whiteville', 'Wilkesboro',
    'Williamston', 'Wilmington North', 'Wilmington South', 'Wilson',
    'Winston Salem North', 'Winston Salem South', 'Yadkinville'
];

function loadLocations() {
    const grid = document.getElementById('locationGrid');
    if (!grid) return;

    grid.innerHTML = '';

    NC_LOCATIONS.forEach(loc => {
        const div = document.createElement('div');
        div.className = 'location-item';
        div.textContent = loc;

        if (state.selectedLocations.includes(loc)) {
            div.classList.add('selected');
        }

        div.onclick = () => toggleLocation(loc, div);
        grid.appendChild(div);
    });

    const btn = document.getElementById('subscribeBtn');
    if (btn) btn.disabled = state.selectedLocations.length === 0;
}

function toggleLocation(loc, element) {
    element.classList.toggle('selected');

    const idx = state.selectedLocations.indexOf(loc);
    if (idx >= 0) {
        state.selectedLocations.splice(idx, 1);
    } else {
        state.selectedLocations.push(loc);
    }

    const btn = document.getElementById('subscribeBtn');
    if (btn) btn.disabled = state.selectedLocations.length === 0;
}

window.filterLocations = function () {
    const searchEl = document.getElementById('locationSearch');
    const query = searchEl.value.toLowerCase();

    document.querySelectorAll('.location-item').forEach(item => {
        const text = item.textContent.toLowerCase();
        item.style.display = text.includes(query) ? '' : 'none';
    });
};

// ============================================================================
// SUBSCRIBE
// ============================================================================

function showInlineError(msg) {
    const el = document.getElementById('subscribeError');
    el.textContent = msg;
    el.classList.add('show');
}

function hideInlineError() {
    const el = document.getElementById('subscribeError');
    el.classList.remove('show');
}

async function subscribe() {
    const btn = document.getElementById('subscribeBtn');
    const original = btn.textContent;

    btn.disabled = true;
    btn.textContent = '⏳ Setting up...';
    hideInlineError();

    try {
        if (state.selectedCategories.length === 0) {
            showInlineError('Please select at least one category.');
            btn.disabled = false;
            btn.textContent = original;
            return;
        }

        const permission = await requestNotificationPermission();
        if (!permission.granted) {
            showInlineError(permission.error || 'Notifications are required.');
            btn.disabled = false;
            btn.textContent = original;
            return;
        }

        // Register service worker
        let pushSubscription = null;

        if ('serviceWorker' in navigator && 'PushManager' in window) {
            const reg = await navigator.serviceWorker.register('/sw.js');
            await navigator.serviceWorker.ready;

            const vapidKey = await getVapidPublicKey();

            pushSubscription = await reg.pushManager.subscribe({
                userVisibleOnly: true,
                applicationServerKey: urlBase64ToUint8Array(vapidKey)
            });

            state.subscription = pushSubscription;
        }

        state.userId = pushSubscription
            ? btoa(pushSubscription.endpoint).substring(0, 50)
            : 'user_' + Date.now();

        localStorage.setItem('dmv_user_id', state.userId);

        await fetch(`${API_URL}/subscriptions`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                user_id: state.userId,
                push_subscription: pushSubscription ? JSON.stringify(pushSubscription.toJSON()) : null,
                categories: state.selectedCategories,
                locations: state.selectedLocations,
                date_range_days: 30
            })
        });

        showSuccessScreen();
        showDonatePopup();

    } catch (err) {
        console.error('Subscribe error:', err);
        showInlineError('Failed: ' + err.message);
    }

    btn.textContent = original;
    btn.disabled = false;
}

window.subscribe = subscribe;

// ============================================================================
// SUCCESS SCREEN
// ============================================================================

function showSuccessScreen() {
    const categoryEl = document.getElementById('successCategory');
    const locationsEl = document.getElementById('successLocations');

    const categories = state.selectedCategories.join(', ');
    const locations = state.selectedLocations.join(', ');

    categoryEl.textContent = categories || 'All categories';
    locationsEl.textContent = locations || 'All NC locations';

    showScreen('success');
}

// ============================================================================
// UNSUBSCRIBE
// ============================================================================

async function unsubscribe() {
    if (!confirm('Unsubscribe?')) return;

    try {
        const userId = state.userId || localStorage.getItem('dmv_user_id');

        await fetch(`${API_URL}/subscriptions/${encodeURIComponent(userId)}`, {
            method: 'DELETE'
        });

        if ('serviceWorker' in navigator) {
            const reg = await navigator.serviceWorker.getRegistration();
            if (reg) {
                const sub = await reg.pushManager.getSubscription();
                if (sub) await sub.unsubscribe();
            }
        }

        state.userId = null;
        state.subscription = null;
        state.selectedCategories = [];
        state.selectedLocations = [];
        localStorage.removeItem('dmv_user_id');

        showAlert('Unsubscribed', 'success');
        setTimeout(() => location.reload(), 1000);

    } catch (err) {
        console.error('Unsubscribe error:', err);
        showAlert('Failed to unsubscribe', 'error');
    }
}

window.unsubscribe = unsubscribe;

// ============================================================================
// TEST NOTIFICATION
// ============================================================================

function testNotification() {
    console.log('testNotification called');

    if (Notification.permission === 'granted') {
        if ('serviceWorker' in navigator) {
            navigator.serviceWorker.ready.then(function(reg) {
                reg.showNotification('🧪 Test notification', {
                    body: 'Notifications working!',
                    icon: '/icon-192.png',
                    badge: '/icon-192.png',
                    tag: 'test-notification'
                }).then(function() {
                    showAlert('Test sent!', 'success');
                }).catch(function(err) {
                    console.error('SW notification error:', err);
                    showAlert('Error: ' + err.message, 'error');
                });
            }).catch(function(err) {
                console.error('SW ready error:', err);
                showAlert('Error: ' + err.message, 'error');
            });
        } else {
            new Notification('🧪 Test notification', {
                body: 'Notifications working!',
                icon: '/icon-192.png'
            });
            showAlert('Test sent!', 'success');
        }
    } else {
        showAlert('Enable notifications first', 'error');
    }
}

window.testNotification = testNotification;

// ============================================================================
// HELPERS
// ============================================================================

function showAlert(msg, type = 'info') {
    const alert = document.getElementById('alert');
    alert.textContent = msg;
    alert.className = `alert ${type}`;
    alert.style.display = 'block';
    setTimeout(() => alert.style.display = 'none', 4000);
}

function urlBase64ToUint8Array(str) {
    const pad = '='.repeat((4 - str.length % 4) % 4);
    const base64 = (str + pad).replace(/-/g, '+').replace(/_/g, '/');
    const raw = atob(base64);
    return Uint8Array.from([...raw].map(ch => ch.charCodeAt(0)));
}

// ============================================================================
// RESTORE EXISTING SUBSCRIPTION
// ============================================================================

async function restoreExistingSubscription() {
    try {
        const savedUserId = localStorage.getItem('dmv_user_id');
        if (!savedUserId) return false;

        const resp = await fetch(`${API_URL}/subscriptions/${encodeURIComponent(savedUserId)}`);
        if (!resp.ok) return false;

        const subData = await resp.json();

        state.userId = savedUserId;
        state.selectedCategories = subData.categories || [];
        state.selectedLocations = subData.locations || [];

        showSuccessScreen();
        showAlert('Notifications already active', 'success');

        return true;

    } catch (err) {
        console.error('Restore error:', err);
        return false;
    }
}

// ============================================================================
// GET VAPID KEY
// ============================================================================

async function getVapidPublicKey() {
    const resp = await fetch(`${API_URL}/vapid-public-key`);
    const data = await resp.json();
    return data.public_key;
}

// ============================================================================
// INITIALIZATION
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    restoreExistingSubscription();
});
// ============================================================================
// LIVE AVAILABILITY POPUP (from last_check.json)
// ============================================================================

let availabilityData = [];

// Format category key into pretty label
function formatCategoryLabel(key) {
    if (!key) return '';
    return key
        .split('_')
        .map(part => part.charAt(0).toUpperCase() + part.slice(1))
        .join(' ');
}

// Render locations list for selected category
function renderAvailabilityList(categoryKey) {
    const listEl = document.getElementById('availabilityList');
    if (!listEl) {
        return;
    }

    if (!categoryKey) {
        listEl.innerHTML = '<div class="availability-empty">Please choose a category.</div>';
        return;
    }

    const items = availabilityData
        .filter(item => item.category === categoryKey)
        .sort((a, b) => a.location_name.localeCompare(b.location_name));

    if (items.length === 0) {
        listEl.innerHTML = '<div class="availability-empty">No locations for this category.</div>';
        return;
    }

    listEl.innerHTML = '';

    items.forEach(item => {
        const row = document.createElement('div');
        row.className = 'availability-row';

        let lastCheckedText = 'Unknown';
        if (item.last_checked) {
            const d = new Date(item.last_checked);
            if (!isNaN(d.getTime())) {
                lastCheckedText = d.toLocaleString();
            } else {
                lastCheckedText = item.last_checked;
            }
        }

        const slotsCount = typeof item.slots_count === 'number' ? item.slots_count : 0;
        const slotsLabel = slotsCount > 0 ? `${slotsCount} slots` : 'No slots';
        const slotsExtraClass = slotsCount > 0 ? '' : ' availability-slots-empty';

        row.innerHTML = `
            <div class="availability-main">
                <div class="availability-location">${item.location_name}</div>
                <div class="availability-meta">Last checked: ${lastCheckedText}</div>
            </div>
            <div class="availability-slots${slotsExtraClass}">
                ${slotsLabel}
            </div>
        `;

        listEl.appendChild(row);
    });
}

// Open modal and load data if needed
async function openAvailabilityModal() {
    const modal = document.getElementById('availabilityModal');
    const listEl = document.getElementById('availabilityList');
    const selectEl = document.getElementById('availabilityCategorySelect');

    if (!modal || !listEl || !selectEl) {
        console.error('Availability modal elements not found');
        return;
    }

    modal.classList.add('open');
    document.body.classList.add('availability-modal-open');

    // Если данных ещё нет - показываем загрузку
    if (availabilityData.length === 0) {
        listEl.innerHTML = '<div class="availability-empty">Loading availability data…</div>';
        return;
    }

    // Заполняем категории если их ещё нет
    if (selectEl.options.length === 0) {
        const categories = Array.from(
            new Set(availabilityData.map(item => item.category))
        ).sort();

        categories.forEach(catKey => {
            const opt = document.createElement('option');
            opt.value = catKey;
            opt.textContent = formatCategoryLabel(catKey);
            selectEl.appendChild(opt);
        });
    }

    // Показываем список
    const current = selectEl.value || (selectEl.options[0] ? selectEl.options[0].value : '');
    if (current) {
        selectEl.value = current;
        renderAvailabilityList(current);
    } else {
        listEl.innerHTML = '<div class="availability-empty">No categories found.</div>';
    }
}

// Close modal
function closeAvailabilityModal() {
    const modal = document.getElementById('availabilityModal');
    if (!modal) {
        return;
    }
    modal.classList.remove('open');
    document.body.classList.remove('availability-modal-open');
    // Не останавливаем автообновление - оно работает постоянно
}

// Handler for category change (used in HTML onchange)
function onAvailabilityCategoryChange() {
    const selectEl = document.getElementById('availabilityCategorySelect');
    if (!selectEl) {
        return;
    }
    const categoryKey = selectEl.value;
    renderAvailabilityList(categoryKey);
}

// Expose functions to window so HTML can call them
window.openAvailabilityModal = openAvailabilityModal;
window.closeAvailabilityModal = closeAvailabilityModal;
window.onAvailabilityCategoryChange = onAvailabilityCategoryChange;

// ============================================================================
// AUTO-UPDATE AVAILABILITY DATA
// ============================================================================

let availabilityUpdateInterval = null;

async function updateAvailabilityData() {
    try {
        const resp = await fetch('data/last_check.json?t=' + Date.now());
        if (!resp.ok) {
            throw new Error('HTTP ' + resp.status);
        }
        availabilityData = await resp.json();

        // Если модалка открыта - обновляем UI
        const modal = document.getElementById('availabilityModal');
        if (modal && modal.classList.contains('open')) {
            const selectEl = document.getElementById('availabilityCategorySelect');
            if (selectEl) {
                const currentValue = selectEl.value;

                // Обновляем категории
                selectEl.innerHTML = '';
                const categories = Array.from(
                    new Set(availabilityData.map(item => item.category))
                ).sort();

                categories.forEach(catKey => {
                    const opt = document.createElement('option');
                    opt.value = catKey;
                    opt.textContent = formatCategoryLabel(catKey);
                    selectEl.appendChild(opt);
                });

                // Восстанавливаем выбор
                if (currentValue && categories.includes(currentValue)) {
                    selectEl.value = currentValue;
                } else if (selectEl.options[0]) {
                    selectEl.value = selectEl.options[0].value;
                }

                // Обновляем список
                renderAvailabilityList(selectEl.value);
            }
        }
    } catch (err) {
        console.error('Failed to update availability data', err);
    }
}

// Запускаем автообновление при загрузке страницы
document.addEventListener('DOMContentLoaded', () => {
    restoreExistingSubscription();

    // Загружаем данные сразу
    updateAvailabilityData();

    // Запускаем автообновление каждые 10 секунд
    availabilityUpdateInterval = setInterval(updateAvailabilityData, 10000);
});

// ============================================================================
// DONATE POPUP
// ============================================================================

function showDonatePopup() {
    const popup = document.getElementById('donatePopup');
    if (!popup) return;

    // Показываем попап только один раз на устройство
    if (localStorage.getItem('dmv_donate_popup_shown') === '1') {
        return;
    }

    popup.classList.add('show');
}

function closeDonatePopup() {
    const popup = document.getElementById('donatePopup');
    if (!popup) return;

    popup.classList.remove('show');
    // Запоминаем, что показали попап, чтобы больше не надоедать
    localStorage.setItem('dmv_donate_popup_shown', '1');
}

function handleDonateClick() {
    closeDonatePopup();
    // Открываем Ko-fi в новой вкладке
    window.open('https://ko-fi.com/gizred', '_blank');
}

// Делаем функции доступными для HTML-атрибутов onclick
window.closeDonatePopup = closeDonatePopup;
window.handleDonateClick = handleDonateClick;

function skipLocations() {
    const allLocations = NC_LOCATIONS;
    const selected = state.selectedLocations || [];

    // Проверяем, выбраны ли сейчас ВСЕ локации
    const allSelected =
        selected.length === allLocations.length &&
        allLocations.every(loc => selected.includes(loc));

    const grid = document.getElementById('locationGrid');
    const btn = document.getElementById('subscribeBtn');

    if (allSelected) {
        // 🔻 Вариант 1: уже выбраны все → сбрасываем выбор
        state.selectedLocations = [];

        if (grid) {
            grid.querySelectorAll('.location-item').forEach(item => {
                item.classList.remove('selected');
            });
        }

        if (btn) {
            btn.disabled = true; // без выбранных локаций подписываться нельзя
        }
    } else {
        // 🔺 Вариант 2: выбрана часть или ничего → выбираем все
        state.selectedLocations = [...allLocations];

        if (grid) {
            grid.querySelectorAll('.location-item').forEach(item => {
                item.classList.add('selected');
                // на всякий случай покажем, даже если до этого их спрятал поиск
                item.style.display = '';
            });
        }

        if (btn) {
            btn.disabled = false;
        }
    }
}

// чтобы работало onclick="skipLocations()"
window.skipLocations = skipLocations;