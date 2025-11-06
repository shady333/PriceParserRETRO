import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re
from concurrent.futures import ThreadPoolExecutor
import threading
import random
import os

# Налаштування
BASE_URL = "https://retromagaz.com/hot-wheels?page="
BUY_OUTPUT_FILE = "car_prices.csv"
SELL_OUTPUT_FILE = "sell_car_prices.csv"
PROGRESS_FILE = "progress.txt"
ERROR_LOG_FILE = "scraper_errors.log"
CURRENT_DATE = datetime.now().strftime('%Y-%m-%d')
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5"
}
PRICE_THRESHOLDS = {
    'premium': 350,
    'rlc': 1200,
    'super_treasure_hunt': 900,
    'diorama': 600,
    'matchbox': 90,
    'treasure_hunts': 90,
    'team_transport': 550,
    'mainline': 90
}
WORDS_TO_IGNORE = {
    "Набір"
}
PAGES_PER_DAY = 60
SAVE_INTERVAL = 5
MAX_WORKERS = 20

# Потокобезпечний список для даних
BUY_DATA = []
SELL_DATA = []
DATA_LOCK = threading.Lock()

# Параметри фільтрування
SKIP_PREMIUM = False
SKIP_RLC = False
SKIP_SUPER_TREASURE_HUNT = False
SKIP_DIORAMA = False
SKIP_MATCHBOX = False
SKIP_TREASURE_HUNTS = False
SKIP_TEAM_TRANSPORT = False


# Функція для логування помилок
def log_error(message):
    with open(ERROR_LOG_FILE, 'a', encoding='utf-8') as f:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"[{timestamp}] {message}\n")


def extract_sku(car_name):
    """
    Витягує SKU з назви товару Hot Wheels.
    - Патерн: 1–4 великі літери + 2–4 цифри (наприклад: GRN86, T9679, X1666, HYY72)
    - Якщо подвійний код через '/', повертає той, що після '/'.
    - Ігнорує вміст у круглих дужках (щоб не брати внутрішні коди типу BNR32).
    """
    if car_name is None:
        return None

    # Нормалізуємо рядок для пошуку (великі літери)
    s = str(car_name).upper().strip().strip('"')

    # 1) Подвійний код через '/' - шукаємо в усьому рядку і повертаємо праву частину
    double_re = re.search(r'\b[A-Z]{1,4}\d{2,4}/([A-Z]{1,4}\d{2,4})\b', s)
    if double_re:
        return double_re.group(1)

    # 2) Видаляємо вміст у дужках (щоб ігнорувати моделі типу (BNR32), (R35) і т.д.)
    s_no_paren = re.sub(r'\([^)]*\)', ' ', s)

    # 3) Знаходимо всі потенційні коди і повертаємо останній (найчастіше SKU стоїть ближче до кінця)
    all_codes = re.findall(r'\b[A-Z]{1,4}\d{2,4}\b', s_no_paren)
    if all_codes:
        return all_codes[-1]

    return None


# Функція для визначення типу товару, порогу та категорії
def get_category_and_threshold(title):
    title_lower = title.lower()
    if 'team transport' in title_lower:
        return 'Team Transport', PRICE_THRESHOLDS['team_transport']
    if ('4шт' in title_lower or '2шт' in title_lower) and 'diorama' in title_lower:
        return 'Diorama', PRICE_THRESHOLDS['diorama']
    if 'premium' in title_lower:
        return 'Premium', PRICE_THRESHOLDS['premium']
    elif 'rlc' in title_lower:
        return 'RLC', PRICE_THRESHOLDS['rlc']
    elif 'super treasure hunt' in title_lower:
        return 'Super Treasure Hunt', PRICE_THRESHOLDS['super_treasure_hunt']
    elif 'matchbox' in title_lower:
        return 'Matchbox', PRICE_THRESHOLDS['matchbox']
    elif 'treasure hunt' in title_lower:
        return 'Treasure Hunts', PRICE_THRESHOLDS['treasure_hunts']
    return 'MainLine', PRICE_THRESHOLDS['mainline']


# Функція для очищення назви
def clean_title(title):
    patterns = [
        r'^\s*Машинка\s+Базова\s+',
        r'^\s*Тематична\s+Машинка\s+',
        r'^\s*Машинка\s+',
        r'^\s*Hot\s+Wheels\s+',
        r'^\s*Premium\s+Hot\s+Wheels\s+',
        r'^\s*Matchbox\s+'
    ]
    clean = title
    for pattern in patterns:
        clean = re.sub(pattern, '', clean, flags=re.IGNORECASE)
    return clean.strip()


def check_ignore_words(text):
    return any(word in text for word in WORDS_TO_IGNORE)


# Функція для парсингу сторінки товару
def scrape_product_page(url):
    try:
        time.sleep(random.uniform(1, 3))
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"Помилка: не вдалося отримати сторінку товару {url} (код: {response.status_code})")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        title_elem = soup.find('div', class_=re.compile('product_title--top'))
        if not title_elem:
            print(f"Помилка: не знайдено div.product_title--top на {url}")
            return None

        title_h1 = title_elem.find('h1') or title_elem.find('p', class_='h1')
        if not title_h1:
            print(f"Помилка: не знайдено h1 або p.h1 у product_title--top на {url}")
            return None

        title = title_h1.text.strip()
        if not title or not ('hot wheels' in title.lower() or 'matchbox' in title.lower()):
            print(f"Пропущено: {title} - не машинка")
            return None

        if check_ignore_words(title):
            print(f"Пропущено: {title} - містить слово з списку для ігнорування")
            return None

        # Витягуємо SKU
        sku = extract_sku(title)
        if not sku:
            error_msg = f"Немає SKU: {title} | URL: {url}"
            print(f"⚠️ {error_msg}")
            log_error(error_msg)
            return None

        category, threshold = get_category_and_threshold(title)
        title_lower = title.lower()
        if (SKIP_PREMIUM and 'premium' in title_lower) or \
                (SKIP_RLC and 'rlc' in title_lower) or \
                (SKIP_SUPER_TREASURE_HUNT and 'super treasure hunt' in title_lower) or \
                (SKIP_DIORAMA and 'diorama' in title_lower) or \
                (SKIP_MATCHBOX and 'matchbox' in title_lower) or \
                (SKIP_TREASURE_HUNTS and 'treasure hunt' in title_lower) or \
                (SKIP_TEAM_TRANSPORT and 'team transport' in title_lower):
            print(f"Пропущено: {title} - фільтр категорії")
            return None

        # Buying price
        price_elem = soup.find('div', class_='product_info--shoping-bar')
        if not price_elem or not price_elem.find('span', class_='price'):
            print(f"Помилка: не знайдено ціну покупки на {url}")
            return None
        price_text = price_elem.find('span', class_='price').text.strip()
        price_text = re.sub(r'[^\d.]', '', price_text)
        buy_price = float(price_text)

        # Selling price
        sell_price_elem = soup.find('p', class_='product_options-price')
        if not sell_price_elem:
            print(f"Помилка: не знайдено ціну продажу на {url}")
            return None

        promo_price_elem = sell_price_elem.find('span', class_='red-text')
        if promo_price_elem:
            sell_price_text = promo_price_elem.text.strip()
        else:
            sell_price_text = sell_price_elem.text.strip()

        sell_price_text = re.sub(r'[^\d.]', '', sell_price_text)
        sell_price = float(sell_price_text)

        if buy_price >= threshold:
            print(f"✅ Знайдено: SKU={sku} | {title} | Купівля={buy_price}, Продаж={sell_price} (поріг {threshold})")
            clean_name = clean_title(title)
            if not clean_name:
                print(f"Помилка: очищена назва порожня для {title} на {url}")
                return None

            # Витягування URL зображення
            image_url = None
            product_image = soup.find('div', class_='product_image')
            if product_image:
                picture = product_image.find('picture')
                if picture:
                    source = picture.find('source')
                    if source and 'srcset' in source.attrs:
                        image_url = 'https://retromagaz.com' + source['srcset'].split()[0]
                    elif picture.find('img') and 'src' in picture.find('img').attrs:
                        image_url = 'https://retromagaz.com' + picture.find('img')['src']

            return {
                'sku': sku,
                'car_name': clean_name,
                'buy_price': buy_price,
                'sell_price': sell_price,
                'category': category,
                'image_url': image_url
            }
        else:
            print(f"Пропущено: {title} - ціна покупки {buy_price} нижче порогу {threshold}")
            return None

    except Exception as e:
        error_msg = f"Помилка на сторінці {url}: {e}"
        print(error_msg)
        log_error(error_msg)
        return None


# Функція для оновлення CSV
def update_csv(file_path, data_list, price_key):
    os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
    except FileNotFoundError:
        df = pd.DataFrame(columns=['sku', 'category', 'car_name', 'image_url'])

    if CURRENT_DATE not in df.columns:
        df[CURRENT_DATE] = pd.NA

    for item in data_list:
        sku = item['sku']
        car_name = item['car_name']
        price = item[price_key]
        category = item['category']
        image_url = item.get('image_url', '')

        car_name = f'"{car_name}"' if ',' in car_name else car_name

        if sku not in df['sku'].values:
            # Новий товар
            df.loc[len(df)] = [sku, category, car_name, image_url] + [pd.NA] * (len(df.columns) - 4)
            df.loc[df['sku'] == sku, CURRENT_DATE] = price
        else:
            # Оновлюємо існуючий товар
            df.loc[df['sku'] == sku, CURRENT_DATE] = price
            df.loc[df['sku'] == sku, 'category'] = category
            df.loc[df['sku'] == sku, 'car_name'] = car_name  # Оновлюємо назву (найновіша)
            if image_url:
                df.loc[df['sku'] == sku, 'image_url'] = image_url

    columns = ['sku', 'category', 'car_name', 'image_url'] + [col for col in df.columns if
                                                              col not in ['sku', 'category', 'car_name', 'image_url']]
    df = df[columns]
    df.to_csv(file_path, index=False, encoding='utf-8-sig', sep=',')
    print(f"💾 Дані збережено в {file_path}")


# Функція для парсингу сторінки пагінації
def scrape_page(page_num):
    global BUY_DATA, SELL_DATA
    url = f"{BASE_URL}{page_num}"
    print(f"📄 Парсимо сторінку {page_num}...")

    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Помилка: не вдалося отримати сторінку {url} (код: {response.status_code})")
            return False

        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.find_all('div', class_='game-card')
        if not items:
            print(f"Попередження: не знайдено товарів на сторінці {page_num}")
            return False

        product_urls = [item.find('a', class_='game-card__image')['href'] for item in items if
                        item.find('a', class_='game-card__image') and item.find('a', class_='game-card__image').get(
                            'href')]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(scrape_product_page, url) for url in product_urls]
            for future in futures:
                result = future.result()
                if result:
                    with DATA_LOCK:
                        BUY_DATA.append({
                            'sku': result['sku'],
                            'car_name': result['car_name'],
                            'price': result['buy_price'],
                            'category': result['category'],
                            'image_url': result['image_url']
                        })
                        SELL_DATA.append({
                            'sku': result['sku'],
                            'car_name': result['car_name'],
                            'price': result['sell_price'],
                            'category': result['category'],
                            'image_url': result['image_url']
                        })

        return True

    except Exception as e:
        error_msg = f"Помилка на сторінці {url}: {e}"
        print(error_msg)
        log_error(error_msg)
        return False


# Головна логіка
def main():
    global BUY_DATA, SELL_DATA

    print("🚗 Запуск скрапера Hot Wheels з підтримкою SKU")
    print("=" * 60)

    response = requests.get(BASE_URL + "1", headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
    pagination = soup.find_all('li', class_='item')
    max_pages = max([int(li['data-p']) for li in pagination if 'data-p' in li.attrs], default=1)

    current_page = 0
    iteration = 1
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            current_page = int(f.read().strip())
    else:
        with open(PROGRESS_FILE, "w") as f:
            f.write(str(current_page))

    if current_page >= max_pages:
        current_page = 0
        with open(PROGRESS_FILE, "w") as f:
            f.write(str(current_page))

    start_page = current_page + 1
    end_page = ((start_page - 1 + PAGES_PER_DAY) % max_pages) + 1

    print(f"📊 Start page: {start_page}, End page: {end_page}, Max pages: {max_pages}")
    print("=" * 60)

    while (start_page != end_page) or (iteration < PAGES_PER_DAY):
        if not scrape_page(start_page):
            print(f"Парсинг завершено на сторінці {start_page}")
            break

        if start_page % SAVE_INTERVAL == 0 or start_page == end_page:
            with DATA_LOCK:
                if BUY_DATA:
                    update_csv(BUY_OUTPUT_FILE, BUY_DATA, 'price')
                    BUY_DATA = []
                if SELL_DATA:
                    update_csv(SELL_OUTPUT_FILE, SELL_DATA, 'price')
                    SELL_DATA = []

        start_page += 1
        iteration += 1
        if start_page > max_pages:
            start_page = 0
        time.sleep(2)

        with open(PROGRESS_FILE, "w") as f:
            f.write(str(start_page))

    if BUY_DATA or SELL_DATA:
        with DATA_LOCK:
            if BUY_DATA:
                update_csv(BUY_OUTPUT_FILE, BUY_DATA, 'price')
                BUY_DATA = []
            if SELL_DATA:
                update_csv(SELL_OUTPUT_FILE, SELL_DATA, 'price')
                SELL_DATA = []
    else:
        print("Немає даних для збереження")

    print("=" * 60)
    print("✅ Парсинг завершено!")


if __name__ == "__main__":
    main()