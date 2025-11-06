import pandas as pd
import re
import sys
import os
from datetime import datetime


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


def migrate_csv_add_sku(input_file, output_file=None, remove_no_sku=True):
    """
    Додає колонку SKU до існуючого CSV файлу.

    Args:
        input_file: Вхідний CSV файл
        output_file: Вихідний CSV файл (якщо None, створюється backup і перезаписується оригінал)
        remove_no_sku: Чи видаляти рядки без SKU (за замовчуванням True)
    """
    print("🔧 Міграція CSV: додавання колонки SKU")
    print("=" * 70)

    # Читання CSV
    try:
        df = pd.read_csv(input_file, encoding='utf-8-sig')
        print(f"✅ Завантажено файл: {input_file}")
        print(f"📊 Кількість рядків: {len(df)}")
    except FileNotFoundError:
        print(f"❌ Помилка: Файл {input_file} не знайдено")
        return
    except Exception as e:
        print(f"❌ Помилка читання файлу: {e}")
        return

    # Перевірка наявності колонки car_name
    if 'car_name' not in df.columns:
        print("❌ Помилка: Колонка 'car_name' не знайдена в файлі")
        return

    # Перевірка чи вже є SKU
    if 'sku' in df.columns:
        print("⚠️  Увага: Колонка 'sku' вже існує в файлі")
        response = input("Продовжити і перезаписати SKU? (y/n): ")
        if response.lower() != 'y':
            print("❌ Операція скасована")
            return

    # Створення backup якщо перезаписуємо оригінал
    if output_file is None:
        backup_file = input_file.replace('.csv', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        df.to_csv(backup_file, index=False, encoding='utf-8-sig')
        print(f"💾 Створено backup: {backup_file}")
        output_file = input_file

    # Витягування SKU
    print("\n🔍 Витягування SKU з назв товарів...")
    df['sku'] = df['car_name'].apply(extract_sku)

    # Статистика
    total_rows = len(df)
    rows_with_sku = df['sku'].notna().sum()
    rows_without_sku = total_rows - rows_with_sku

    print(f"\n📊 Результати витягування SKU:")
    print(f"  • Всього рядків: {total_rows}")
    print(f"  • З SKU: {rows_with_sku} ({rows_with_sku / total_rows * 100:.1f}%)")
    print(f"  • Без SKU: {rows_without_sku} ({rows_without_sku / total_rows * 100:.1f}%)")

    # Показуємо приклади товарів без SKU
    if rows_without_sku > 0:
        print(f"\n⚠️  Товари без SKU (перші 10):")
        no_sku_items = df[df['sku'].isna()]['car_name'].head(10)
        for idx, name in enumerate(no_sku_items, 1):
            print(f"  {idx}. {name}")

        # Логування всіх товарів без SKU
        log_file = 'migration_no_sku.log'
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"Товари без SKU - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 70 + "\n\n")
            for name in df[df['sku'].isna()]['car_name']:
                f.write(f"{name}\n")
        print(f"📝 Повний список збережено в: {log_file}")

        if remove_no_sku:
            print(f"\n🗑️  Видалення {rows_without_sku} рядків без SKU...")
            df = df[df['sku'].notna()]
            print(f"✅ Залишилось рядків: {len(df)}")

    # Перестановка колонок: sku на початок
    columns = ['sku'] + [col for col in df.columns if col != 'sku']
    df = df[columns]

    # Збереження результату
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n💾 Результат збережено в: {output_file}")
    print(f"📋 Структура: {', '.join(df.columns[:5])}...")

    # Перевірка на дублікати SKU
    duplicate_skus = df[df['sku'].duplicated(keep=False)]['sku'].unique()
    if len(duplicate_skus) > 0:
        print(f"\n⚠️  Знайдено {len(duplicate_skus)} SKU з дублікатами:")
        for sku in duplicate_skus[:10]:
            count = (df['sku'] == sku).sum()
            print(f"  • {sku}: {count} записів")
        if len(duplicate_skus) > 10:
            print(f"  ... та ще {len(duplicate_skus) - 10}")
        print(f"\n💡 Рекомендація: Запустіть merge_duplicates_sku.py для об'єднання дублікатів")
    else:
        print(f"\n✅ Дублікатів SKU не знайдено!")

    print("=" * 70)
    print("✅ Міграція завершена!")


def main():
    print("\n🚗 Hot Wheels CSV Migration Tool")
    print("Додавання колонки SKU до існуючих даних\n")

    if len(sys.argv) < 2:
        print("Використання:")
        print("  python migrate_add_sku.py <input_file> [output_file] [--keep-no-sku]")
        print("\nПриклади:")
        print("  python migrate_add_sku.py car_prices.csv")
        print("  python migrate_add_sku.py car_prices.csv car_prices_new.csv")
        print("  python migrate_add_sku.py car_prices.csv --keep-no-sku")
        print("\nОпції:")
        print("  --keep-no-sku    Залишити рядки без SKU (за замовчуванням видаляються)")
        print("\nЯкщо output_file не вказано:")
        print("  - Створюється backup оригінального файлу")
        print("  - Оригінальний файл перезаписується")
        sys.exit(1)

    input_file = sys.argv[1]

    # Парсинг аргументів
    output_file = None
    keep_no_sku = False

    for arg in sys.argv[2:]:
        if arg == '--keep-no-sku':
            keep_no_sku = True
        elif not arg.startswith('--'):
            output_file = arg

    remove_no_sku = not keep_no_sku

    if keep_no_sku:
        print("ℹ️  Режим: Зберігати рядки без SKU\n")
    else:
        print("ℹ️  Режим: Видаляти рядки без SKU\n")

    migrate_csv_add_sku(input_file, output_file, remove_no_sku)


if __name__ == "__main__":
    main()