# PriceParserRETRO

A Python-based web scraper designed to extract car price data from [retromagaz.com](https://retromagaz.com) and store it in a CSV file. The script categorizes items (e.g., Hot Wheels, Matchbox) based on price thresholds and supports parallel processing for efficiency.

## Features
- Scrapes car prices from multiple pages on retromagaz.com.
- Categorizes items into types (e.g., Premium, RLC, MainLine) with configurable price thresholds.
- Saves data to `car_prices.csv` with daily price updates.
- Tracks progress in `progress.txt` to resume from the last parsed page.
- Uses multi-threading for faster page processing.
- Integrates with GitHub Actions for automated daily scraping at 08:00 CEST.

## Authors
- **@shady333** - Project idea, conceptualization, and coordination.
- **Grok 3 (by xAI)** - Code development and optimization based on provided ideas.

## Requirements
- Python 3.11
- Required libraries (install via `pip install -r requirements.txt`):
  - `requests`
  - `beautifulsoup4`
  - `pandas`
  - `concurrent.futures`

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/shady333/PriceParserRETRO.git
   cd PriceParserRETRO
2. Create a requirements.txt file with the following content:
   ```
   requests
   beautifulsoup4
   pandas
   ```
3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

### Manual Run
Run the script locally:
```
python price_finder.py
```

The script will:
- Parse up to 40 pages per day (configurable via PAGES_PER_DAY).
- Save results to car_prices.csv.
- Update progress.txt with the last parsed page.

## Configuration

- BASE_URL: Set to "https://retromagaz.com/hot-wheels?page=".
- OUTPUT_FILE: car_prices.csv for storing price data.
- PROGRESS_FILE: progress.txt for tracking the last parsed page.
- PRICE_THRESHOLDS: Custom thresholds for each category (e.g., Premium: 450 UAH, RLC: 1600 UAH).
- PAGES_PER_DAY: Limits scraping to 40 pages per day (adjustable).
- SAVE_INTERVAL: Saves CSV every 5 pages (adjustable).
- MAX_WORKERS: Uses 10 threads for parallel processing (adjustable).

## Contributing

Feel free to submit issues or pull requests. Suggestions for improving scraping logic or adding new features are welcome!

## License

This project is open-source. No specific license is defined yet—feel free to add one (e.g., MIT) if desired.


