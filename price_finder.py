import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re
from concurrent.futures import ThreadPoolExecutor
import threading
import random

# Налаштування
base_url = "https://retromagaz.com/hot-wheels?page="
output_file = "car_prices.csv"
current_date = datetime.now().strftime('%Y-%m-%d')
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5"
}
price_thresholds = {
    'premium': 500,
    'rlc': 2000,
    'super_treasure_hunt': 1500,
    'diorama': 1000,
    'matchbox': 150,
    'treasure_hunts': 150,
    'team_transport': 800,
    'mainline': 150
}
# Параметри фільтрування
skip_premium = False
skip_rlc = False
skip_super_treasure_hunt = False
skip_diorama = False
skip_matchbox = False
skip_treasure_hunts = False
skip_team_transport = False
save_interval = 5  # Зберігати CSV кожні 5 сторінок
max_workers = 3  # Максимальна кількість потоків

# Потокобезпечний список для даних
data = []
data_lock = threading.Lock()


# Функція для визначення типу товару, порогу та категорії
def get_category_and_threshold(title):
    title_lower = title.lower()
    # Перевірка для Diorama: якщо є "4шт" або "2шт" і "diorama"
    if ('4шт' in title_lower or '2шт' in title_lower) and 'diorama' in title_lower:
        return 'Diorama', price_thresholds['diorama']
    # Інші категорії
    if 'premium' in title_lower:
        return 'Premium', price_thresholds['premium']
    elif 'rlc' in title_lower:
        return 'RLC', price_thresholds['rlc']
    elif 'super treasure hunt' in title_lower:
        return 'Super Treasure Hunt', price_thresholds['super_treasure_hunt']
    elif 'diorama' in title_lower:
        return 'Diorama', price_thresholds['diorama']
    elif 'matchbox' in title_lower:
        return 'Matchbox', price_thresholds['matchbox']
    elif 'treasure hunt' in title_lower:
        return 'Treasure Hunts', price_thresholds['treasure_hunts']
    elif 'team transport' in title_lower:
        return 'Team Transport', price_thresholds['team_transport']
    return 'MainLine', price_thresholds['mainline']


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
def scrape_product_page(url, skip_premium, skip_rlc, skip_super_treasure_hunt, skip_diorama, skip_matchbox,
                        skip_treasure_hunts, skip_team_transport):
    try:
        time.sleep(random.uniform(1, 3))  # Випадкова затримка
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Помилка: не вдалося отримати сторінку товару {url} (код: {response.status_code})")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')

        # Витягуємо назву
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
        if not title:
            print(f"Помилка: назва порожня на {url}")
            return None

        # Перевіряємо, чи це машинка (на оригінальній назві)
        if not ('hot wheels' in title.lower() or 'matchbox' in title.lower()):
            print(f"Пропущено: {title} - не машинка")
            return None

        # Визначаємо категорію та поріг на оригінальній назві
        category, threshold = get_category_and_threshold(title)

        # Фільтрування категорій (на оригінальній назві)
        title_lower = title.lower()
        if skip_premium and 'premium' in title_lower:
            print(f"Пропущено: {title} - містить Premium")
            return None
        if skip_rlc and 'rlc' in title_lower:
            print(f"Пропущено: {title} - містить RLC")
            return None
        if skip_super_treasure_hunt and 'super treasure hunt' in title_lower:
            print(f"Пропущено: {title} - містить Super Treasure Hunt")
            return None
        if skip_diorama and 'diorama' in title_lower:
            print(f"Пропущено: {title} - містить Diorama")
            return None
        if skip_matchbox and 'matchbox' in title_lower:
            print(f"Пропущено: {title} - містить Matchbox")
            return None
        if skip_treasure_hunts and 'treasure hunt' in title_lower:
            print(f"Пропущено: {title} - містить Treasure Hunts")
            return None
        if skip_team_transport and 'team transport' in title_lower:
            print(f"Пропущено: {title} - містить Team Transport")
            return None

        # Витягуємо ціну
        price_elem = soup.find('div', class_='product_info--shoping-bar')
        if not price_elem or not price_elem.find('span', class_='price'):
            print(f"Помилка: не знайдено ціну на {url}")
            return None

        price_text = price_elem.find('span', class_='price').text.strip()
        price_text = re.sub(r'[^\d.]', '', price_text)

        try:
            price = float(price_text)
        except ValueError:
            print(f"Помилка: не вдалося конвертувати ціну для {title} на {url}")
            return None

        # Перевіряємо поріг
        if price >= threshold:
            print(f"Знайдено: {title} - ціна {price} (поріг {threshold}, категорія {category})")
            # Очищаємо назву лише перед збереженням
            clean_name = clean_title(title)
            if not clean_name:
                print(f"Помилка: очищена назва порожня для {title} на {url}")
                return None
            return {'car_name': clean_name, 'price': price, 'category': category}
        else:
            print(f"Пропущено: {title} - ціна {price} нижче порогу {threshold} (категорія {category})")
            return None

    except Exception as e:
        print(f"Помилка на сторінці {url}: {e}")
        return None


# Функція для парсингу сторінки пагінації
def scrape_page(page_num, skip_premium, skip_rlc, skip_super_treasure_hunt, skip_diorama, skip_matchbox,
                skip_treasure_hunts, skip_team_transport):
    url = f"{base_url}{page_num}"
    print(f"Парсимо сторінку {page_num}...")

    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Помилка: не вдалося отримати сторінку {url} (код: {response.status_code})")
            return False

        soup = BeautifulSoup(response.text, 'html.parser')

        # Знаходимо всі товари
        items = soup.find_all('div', class_='game-card')
        if not items:
            print(f"Попередження: не знайдено товарів на сторінці {page_num}")
            return False

        # Обробляємо товари паралельно
        product_urls = []
        for item in items:
            link_elem = item.find('a', class_='game-card__image')
            if link_elem and link_elem.get('href'):
                product_urls.append(link_elem['href'])
            else:
                print("Помилка: не знайдено посилання на товар")

        # Використовуємо ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    scrape_product_page, url, skip_premium, skip_rlc,
                    skip_super_treasure_hunt, skip_diorama, skip_matchbox, skip_treasure_hunts, skip_team_transport
                ) for url in product_urls
            ]
            for future in futures:
                result = future.result()
                if result:
                    with data_lock:
                        data.append(result)

        # Перевіряємо наявність наступної сторінки
        next_page = soup.find('li', class_='item', attrs={'data-p': str(page_num + 1)})
        return bool(next_page)

    except Exception as e:
        print(f"Помилка на сторінці {url}: {e}")
        return False


# Функція для оновлення CSV
def update_csv(data, output_file, current_date):
    try:
        df = pd.read_csv(output_file, encoding='utf-8-sig')
    except FileNotFoundError:
        df = pd.DataFrame(columns=['category', 'car_name'])

    # Перевіряємо, чи є колонка для поточної дати
    if current_date not in df.columns:
        df[current_date] = pd.NA

    for item in data:
        car_name = item['car_name']
        price = item['price']
        category = item['category']

        # Екрануємо коми в назві
        car_name = f'"{car_name}"' if ',' in car_name else car_name

        if car_name not in df['car_name'].values:
            # Додаємо новий рядок із категорією
            df.loc[len(df)] = [category, car_name] + [pd.NA] * (len(df.columns) - 2)
            df.loc[df['car_name'] == car_name, current_date] = price
        else:
            # Оновлюємо ціну та категорію
            df.loc[df['car_name'] == car_name, current_date] = price
            df.loc[df['car_name'] == car_name, 'category'] = category

    # Переставляємо колонки, щоб category була першою
    columns = ['category', 'car_name'] + [col for col in df.columns if col not in ['category', 'car_name']]
    df = df[columns]

    # Зберігаємо CSV
    df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=',')
    print(f"Дані збережено в {output_file}")


# Обхід сторінок
page = 1
max_pages = 10  # Для тестування, змініть на 106 для повного парсингу
while page <= max_pages:
    if not scrape_page(page, skip_premium, skip_rlc, skip_super_treasure_hunt, skip_diorama, skip_matchbox,
                       skip_treasure_hunts, skip_team_transport):
        print(f"Парсинг завершено на сторінці {page}")
        break

    # Зберігаємо CSV кожні save_interval сторінок
    if data and page % save_interval == 0:
        update_csv(data, output_file, current_date)
        data = []  # Очищаємо data після збереження

    page += 1
    time.sleep(2)  # Затримка між сторінками

# Зберігаємо залишки даних у кінці
if data:
    update_csv(data, output_file, current_date)
else:
    print("Немає даних для збереження")
