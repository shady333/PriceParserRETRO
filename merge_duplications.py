import pandas as pd
import re
import sys

def extract_product_code_and_color(car_name):
    # Код товару: JBM19 або HVL09/HVN37
    code_pattern = r'[A-Z]{3,4}\d{2}(?:/[A-Z]{3,4}\d{2})?'
    code_match = re.search(code_pattern, car_name)
    product_code = code_match.group(0) if code_match else car_name

    # Колір: останнє слово або два слова (наприклад, "Blue" або "Metallic Red")
    color_pattern = r'\b(?:[A-Z][a-z]*(?:\s[A-Z][a-z]*)?)\s*$'
    color_match = re.search(color_pattern, car_name)
    color = color_match.group(0) if color_match else 'Unknown'

    return product_code, color

def merge_duplicates(input_file, output_file=None):
    # Якщо вихідний файл не вказано, переписуємо вхідний
    if output_file is None:
        output_file = input_file

    # Читання CSV-файлу
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Помилка: Файл {input_file} не знайдено.")
        return

    # Додаємо колонки product_code і color
    df[['product_code', 'color']] = df['car_name'].apply(lambda x: pd.Series(extract_product_code_and_color(x)))

    # Створюємо унікальний ідентифікатор
    df['unique_id'] = df['product_code'] + '_' + df['color']

    # Колонки з датами
    date_columns = [col for col in df.columns if col.startswith('2025-')]

    # Функція для визначення рядка з найновішими цінами
    def get_latest_row(group):
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
        return latest_row

    # Групуємо за unique_id і вибираємо рядок із найновішими цінами
    latest_rows = df.groupby('unique_id').apply(get_latest_row).reset_index(drop=True)

    # Об’єднуємо ціни для всіх рядків із однаковим unique_id
    agg_dict = {col: 'first' for col in ['category', 'image_url', 'car_name']}
    agg_dict.update({date: 'max' for date in date_columns})  # Беремо непорожнє значення для цін
    merged_df = df.groupby('unique_id').agg(agg_dict).reset_index()

    # Замінюємо car_name на той, що з найновішими цінами
    merged_df = merged_df.drop(columns=['car_name']).merge(
        latest_rows[['unique_id', 'car_name']], on='unique_id', how='left'
    )

    # Переставляємо колонки в потрібному порядку, виключаючи product_code і color
    final_columns = ['category', 'car_name', 'image_url'] + date_columns
    merged_df = merged_df[final_columns]

    # Зберігаємо результат
    merged_df.to_csv(output_file, index=False)
    print(f"Дублікати об’єднано, результат збережено в {output_file}")

# Використання через командний рядок або виклик функції
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Використання: python script.py input_file [output_file]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    merge_duplicates(input_file, output_file)