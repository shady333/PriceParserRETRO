import pandas as pd
import sys
import logging
from datetime import datetime

# Налаштування логування
logging.basicConfig(filename='merge_log.txt', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def merge_duplicates_by_sku(input_file, output_file=None):
    """
    Об'єднує дублікати товарів на основі SKU.
    Для кожного SKU:
    - Зберігає найновішу назву (car_name)
    - Зберігає останнє зображення (image_url)
    - Об'єднує всі ціни з різних дат
    """
    # Якщо вихідний файл не вказано, переписуємо вхідний
    if output_file is None:
        output_file = input_file

    # Читання CSV-файлу
    try:
        df = pd.read_csv(input_file, encoding='utf-8-sig')
    except FileNotFoundError:
        error_msg = f"Файл {input_file} не знайдено."
        print(f"❌ Помилка: {error_msg}")
        logging.error(error_msg)
        return

    print(f"📂 Завантажено файл: {input_file}")
    print(f"📊 Кількість рядків до об'єднання: {len(df)}")

    # Перевірка наявності колонки SKU
    if 'sku' not in df.columns:
        error_msg = f"Колонка 'sku' не знайдена в файлі {input_file}"
        print(f"❌ Помилка: {error_msg}")
        logging.error(error_msg)
        return

    # Колонки з датами (формат 2025-XX-XX)
    date_columns = [col for col in df.columns if col.startswith('20')]
    print(f"📅 Знайдено колонок з датами: {len(date_columns)}")

    # Знаходимо дублікати SKU
    duplicate_skus = df[df['sku'].duplicated(keep=False)]['sku'].unique()

    if len(duplicate_skus) > 0:
        print(f"\n🔍 Знайдено {len(duplicate_skus)} SKU з дублікатами:")
        for sku in duplicate_skus:
            duplicates = df[df['sku'] == sku]
            car_names = duplicates['car_name'].tolist()
            print(f"  • SKU {sku}: {len(duplicates)} записів")
            for name in car_names:
                print(f"    - {name}")
            logging.info(f"Об'єднання дублікатів для SKU {sku}: {car_names}")
    else:
        print("\n✅ Дублікатів не знайдено!")

    # Функція для вибору найновішої назви
    def get_latest_name_and_image(group):
        """
        Вибирає назву і зображення з рядка, що має найновішу ціну.
        Якщо цін немає, бере найдовшу назву.
        """
        latest_date = None
        latest_row = None

        for idx, row in group.iterrows():
            # Знаходимо останню дату з непорожньою ціною
            non_null_dates = [col for col in date_columns if pd.notnull(row[col])]
            if non_null_dates:
                current_latest = max(non_null_dates)
                if latest_date is None or current_latest > latest_date:
                    latest_date = current_latest
                    latest_row = row

        if latest_row is None:
            # Якщо цін немає, беремо рядок із найдовшою назвою
            latest_row = group.loc[group['car_name'].str.len().idxmax()]

        return pd.Series({
            'car_name': latest_row['car_name'],
            'image_url': latest_row['image_url']
        })

    # Об'єднуємо дані за SKU
    agg_dict = {
        'category': 'first',  # Беремо першу категорію (повинна бути однакова)
    }

    # Для дат беремо максимальне значення (непорожнє)
    for date_col in date_columns:
        agg_dict[date_col] = lambda x: x.dropna().iloc[-1] if not x.dropna().empty else pd.NA

    # Групуємо за SKU
    merged_df = df.groupby('sku', as_index=False).agg(agg_dict)

    # Отримуємо найновіші назви та зображення
    latest_names_images = df.groupby('sku').apply(get_latest_name_and_image).reset_index()

    # Об'єднуємо з основним DataFrame
    merged_df = merged_df.merge(latest_names_images, on='sku', how='left')

    # Переставляємо колонки в потрібному порядку
    final_columns = ['sku', 'category', 'car_name', 'image_url'] + date_columns
    merged_df = merged_df[final_columns]

    # Логування результатів
    original_count = len(df)
    merged_count = len(merged_df)
    removed_count = original_count - merged_count

    print(f"\n📊 Результати об'єднання:")
    print(f"  • Рядків до: {original_count}")
    print(f"  • Рядків після: {merged_count}")
    print(f"  • Видалено дублікатів: {removed_count}")

    if removed_count > 0:
        logging.info(f"Об'єднано {removed_count} дублікатів. Залишилось {merged_count} унікальних SKU.")

    # Зберігаємо результат
    merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n💾 Результат збережено в {output_file}")
    logging.info(f"Результат збережено в {output_file}")


def main():
    print("🔧 Скрипт об'єднання дублікатів за SKU")
    print("=" * 60)

    if len(sys.argv) < 2:
        print("❌ Помилка: Не вказано файл для обробки")
        print("\nВикористання:")
        print("  python merge_duplicates_sku.py <input_file> [output_file]")
        print("\nПриклад:")
        print("  python merge_duplicates_sku.py car_prices.csv")
        print("  python merge_duplicates_sku.py car_prices.csv car_prices_merged.csv")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    merge_duplicates_by_sku(input_file, output_file)

    print("=" * 60)
    print("✅ Обробка завершена!")


if __name__ == "__main__":
    main()