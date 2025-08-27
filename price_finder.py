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
OUTPUT_FILE = "car_prices.csv"
PROGRESS_FILE = "progress.txt"
CURRENT_DATE = datetime.now().strftime('%Y-%m-%d')
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5"
}
PRICE_THRESHOLDS = {
    'premium': 500,
    'rlc': 2000,
    'super_treasure_hunt': 1500,
    'diorama': 1000,
    'matchbox': 120,
    'treasure_hunts': 150,
    'team_transport': 800,
    'mainline': 120
}
PAGES_PER_DAY = 40  # Кількість сторінок за день
SAVE_INTERVAL = 5  # Зберігати CSV кожні 5 сторінок
MAX_WORKERS = 10  # Максимальна кількість потоків

# Потокобезпечний список для даних (ініціалізація на початку)
DATA = []
DATA_LOCK = threading.Lock()

# Параметри фільтрування
SKIP_PREMIUM = False
SKIP_RLC = False
SKIP_SUPER_TREASURE_HUNT = False
SKIP_DIORAMA = False
SKIP_MATCHBOX = False
SKIP_TREASURE_HUNTS = False
SKIP_TEAM_TRANSPORT = False

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

# Функція для парсингу сторінки товару
def scrape_product_page(url):
    try:
        time.sleep(random.uniform(1, 3))  # Випадкова затримка
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"Помилка: не вдалося отримати сторінку товару {url} (код: {response.status_code})")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        title_elem = soup.find('div', class_=re.compile('product_title--top'))
        if not title_elem:
            print(f"Помилка: не знайдено div.product_title--top на {url}")
            print(f"HTML фрагмент: {soup.find('div', class_=re.compile('product_title')).prettify()[:500]}")
            return None

        title_h1 = title_elem.find('h1') or title_elem.find('p', class_='h1')
        if not title_h1:
            print(f"Помилка: не знайдено h1 або p.h1 у product_title--top на {url}")
            print(f"HTML div: {title_elem.prettify()[:500]}")
            return None

        title = title_h1.text.strip()
        if not title or not ('hot wheels' in title.lower() or 'matchbox' in title.lower()):
            print(f"Пропущено: {title} - не машинка")
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

        price_elem = soup.find('div', class_='product_info--shoping-bar')
        if not price_elem or not price_elem.find('span', class_='price'):
            print(f"Помилка: не знайдено ціну на {url}")
            return None

        price_text = price_elem.find('span', class_='price').text.strip()
        price_text = re.sub(r'[^\d.]', '', price_text)
        price = float(price_text)

        if price >= threshold:
            print(f"Знайдено: {title} - ціна {price} (поріг {threshold}, категорія {category})")
            clean_name = clean_title(title)
            if not clean_name:
                print(f"Помилка: очищена назва порожня для {title} на {url}")
                return None

            # Витягування URL зображення зі сторінки товару
            image_url = None
            product_image = soup.find('div', class_='product_image')
            if product_image:
                picture = product_image.find('picture')
                if picture:
                    source = picture.find('source')
                    if source and 'srcset' in source.attrs:
                        image_url = 'https://retromagaz.com' + source['srcset'].split()[0]  # Беремо перше зображення (webp)
                    elif picture.find('img') and 'src' in picture.find('img').attrs:
                        image_url = 'https://retromagaz.com' + picture.find('img')['src']  # Беремо png, якщо webp відсутнє

            return {'car_name': clean_name, 'price': price, 'category': category, 'image_url': image_url}
        else:
            print(f"Пропущено: {title} - ціна {price} нижче порогу {threshold}")
            return None

    except Exception as e:
        print(f"Помилка на сторінці {url}: {e}")
        return None

# Функція для парсингу сторінки пагінації
def scrape_page(page_num):
    global DATA  # Оголосимо DATA як глобальну змінну тут
    url = f"{BASE_URL}{page_num}"
    print(f"Парсимо сторінку {page_num}...")

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

        product_urls = [item.find('a', class_='game-card__image')['href'] for item in items if item.find('a', class_='game-card__image') and item.find('a', class_='game-card__image').get('href')]

        # Обробляємо товари паралельно
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(scrape_product_page, url) for url in product_urls]
            for future in futures:
                result = future.result()
                if result:
                    with DATA_LOCK:
                        DATA.append(result)

        next_page = soup.find('li', class_='item', attrs={'data-p': str(page_num + 1)})
        return bool(next_page)

    except Exception as e:
        print(f"Помилка на сторінці {url}: {e}")
        return False

# Функція для оновлення CSV
def update_csv():
    global DATA  # Оголосимо DATA як глобальну змінну тут
    os.makedirs(os.path.dirname(OUTPUT_FILE) or '.', exist_ok=True)
    try:
        df = pd.read_csv(OUTPUT_FILE, encoding='utf-8-sig')
    except FileNotFoundError:
        df = pd.DataFrame(columns=['category', 'car_name', 'image_url'])  # Додано image_url до колонок

    if CURRENT_DATE not in df.columns:
        df[CURRENT_DATE] = pd.NA

    for item in DATA:
        car_name = item['car_name']
        price = item['price']
        category = item['category']
        image_url = item.get('image_url', '')  # Отримати image_url, якщо є

        car_name = f'"{car_name}"' if ',' in car_name else car_name

        if car_name not in df['car_name'].values:
            df.loc[len(df)] = [category, car_name, image_url] + [pd.NA] * (len(df.columns) - 3)
            df.loc[df['car_name'] == car_name, CURRENT_DATE] = price
        else:
            df.loc[df['car_name'] == car_name, CURRENT_DATE] = price
            df.loc[df['car_name'] == car_name, 'category'] = category
            if image_url:
                df.loc[df['car_name'] == car_name, 'image_url'] = image_url

    columns = ['category', 'car_name', 'image_url'] + [col for col in df.columns if col not in ['category', 'car_name', 'image_url']]
    df = df[columns]
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig', sep=',')
    print(f"Дані збережено в {OUTPUT_FILE}")
    with DATA_LOCK:
        DATA = []  # Очищаємо дані після збереження

# Головна логіка
def main():
    # Витягування максимальної кількості сторінок
    response = requests.get(BASE_URL + "1", headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
    pagination = soup.find_all('li', class_='item')
    max_pages = max([int(li['data-p']) for li in pagination if 'data-p' in li.attrs], default=1)

    # Читання або ініціалізація прогресу
    current_page = 0  # Почнемо з 0, щоб уникнути пропуску першої сторінки
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            current_page = int(f.read().strip())
    else:
        with open(PROGRESS_FILE, "w") as f:
            f.write(str(current_page))

    # Скидання, якщо перевищено max_pages
    if current_page >= max_pages:
        current_page = 0
        with open(PROGRESS_FILE, "w") as f:
            f.write(str(current_page))

    # Обмеження для сьогоднішнього дня
    start_page = current_page + 1  # Починаємо з наступної сторінки
    end_page = min(start_page + PAGES_PER_DAY - 1, max_pages)

    print(f"Start page: {start_page}, End page: {end_page}, Max pages: {max_pages}")

    # Парсинг сторінок
    while start_page <= end_page:
        if not scrape_page(start_page):
            print(f"Парсинг завершено на сторінці {start_page}")
            break

        # Зберігаємо CSV кожні SAVE_INTERVAL сторінок
        if start_page % SAVE_INTERVAL == 0 or start_page == end_page:
            update_csv()

        start_page += 1
        time.sleep(2)  # Затримка між сторінками

        # Збереження прогресу після кожної сторінки
        with open(PROGRESS_FILE, "w") as f:
            f.write(str(start_page))

    # Зберігаємо залишки даних у кінці
    if DATA:
        update_csv()
    else:
        print("Немає даних для збереження")

if __name__ == "__main__":
    main()
