import os
import json
import time
import re
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException


# ----------------------------
# Persistent store for "already parsed lots"
# ----------------------------
class SeenLotsStore:
    def __init__(self, path: str = "seen_lots.json"):
        self.path = path
        self.seen = set()

    def load(self):
        if not os.path.exists(self.path):
            self.seen = set()
            return self.seen
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self.seen = set(data)
            elif isinstance(data, dict) and "seen" in data and isinstance(data["seen"], list):
                self.seen = set(data["seen"])
            else:
                self.seen = set()
        except Exception:
            self.seen = set()
        return self.seen

    def add_many(self, urls):
        for u in urls:
            if u:
                self.seen.add(u)

    def save(self):
        tmp = self.path + ".tmp"
        payload = sorted(self.seen)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)


# ----------------------------
# Parser
# ----------------------------
class BankrotParser:
    def __init__(self, base_url: str, headless: bool = False, page_load_timeout: int = 25):
        self.base_url = base_url.rstrip("/")

        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--window-size=1400,900")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
        )

        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        self.driver.set_page_load_timeout(page_load_timeout)
        self.wait = WebDriverWait(self.driver, 12)

    def close(self):
        try:
            self.driver.quit()
        except Exception:
            pass

    # ---------- extraction helpers ----------
    def _parse_info_wrapper(self, soup, title_text: str):
        wrappers = soup.select("div.lot-info__wrapper")
        target = title_text.strip().lower()
        for w in wrappers:
            h3 = w.select_one("h3.lot-info__title")
            if not h3:
                continue
            if h3.get_text(" ", strip=True).strip().lower() != target:
                continue

            out = {}
            for item in w.select("div.lot-info__item"):
                sub = item.select_one(".lot-info__subtitle")
                val_el = item.select_one(".lot-info__value")
                if not sub or not val_el:
                    continue
                key = sub.get_text(" ", strip=True).strip()
                val = val_el.get_text(" ", strip=True).strip()
                out[key] = val
            return out
        return {}

    def _extract_lot_and_trade_numbers(self, soup):
        el = soup.select_one("span.lot__help")
        if not el:
            return ("Не найдено", "Не найдено")
        text = el.get_text(" ", strip=True)

        lot_num = "Не найдено"
        trade_num = "Не найдено"

        m = re.search(r"Лот\s*№\s*(\d+)", text, flags=re.IGNORECASE)
        if m:
            lot_num = m.group(1)
        m = re.search(r"торги\s*№\s*(\d+)", text, flags=re.IGNORECASE)
        if m:
            trade_num = m.group(1)

        return (lot_num, trade_num)

    def _extract_status(self, soup):
        el = soup.select_one("span.lot__status")
        return el.get_text(" ", strip=True) if el else "Не найдено"

    def _extract_details_info(self, soup):
        out = {}
        for item in soup.select("div.lot-details-info__item"):
            sub = item.select_one(".lot-details-info__subtitle")
            if not sub:
                continue
            key = sub.get_text(" ", strip=True).strip()

            val_el = item.select_one(".lot-details-info__value")
            if not val_el:
                val_el = item.find(["span", "div", "a"])
            if not val_el:
                continue

            link_num = item.select_one("a[data-number]")
            if link_num and key.strip().upper() in {"ИНН", "ОГРН"}:
                out[key] = link_num.get("data-number") or link_num.get_text(" ", strip=True).strip()
            else:
                out[key] = val_el.get_text(" ", strip=True).strip()
        return out

    def _extract_debtor_inn_contact(self, soup):
        d = self._extract_details_info(soup)
        debtor = (
            d.get("Наименование / ФИО")
            or d.get("Полное наименование")
            or d.get("Наименование")
            or d.get("Сведения о должнике")
            or "Не найдено"
        )
        inn = d.get("ИНН", "Не найдено")
        contact = d.get("Контактное лицо", "Не найдено")
        return debtor, inn, contact

    def _extract_address_from_text(self, text: str):
        if not text:
            return "Не найдено"
        t = " ".join(text.split())

        m = re.search(
            r"(?i)(?:расположен\w*|наход\w*)\s+по\s+адрес[ау]?\s*:\s*(.+?)(?=(?:начальн\w*\s+цен|задаток|кадастров\w*\s+номер|$))",
            t
        )
        if m:
            return m.group(1).strip(" ,.;")

        m = re.search(
            r"(?i)по\s+адрес[ау]?\s*:\s*(.+?)(?=(?:начальн\w*\s+цен|задаток|кадастров\w*\s+номер|$))",
            t
        )
        if m:
            return m.group(1).strip(" ,.;")

        m = re.search(
            r"(?i)местонахождение\s*:\s*(.+?)(?=(?:начальн\w*\s+цен|задаток|$))",
            t
        )
        if m:
            return m.group(1).strip(" ,.;")

        return "Не найдено"

    def _extract_address_from_desc_p(self, desc_p):
        for a in desc_p.select("a"):
            use = a.select_one("use")
            href = use.get("xlink:href", "") if use else ""
            if "icon-location" in href:
                addr = a.get_text(" ", strip=True).strip()
                return addr if addr else "Не найдено"
        return self._extract_address_from_text(desc_p.get_text(" ", strip=True))

    def _extract_description_and_address(self, soup):
        content = soup.select_one("div.lot__content.text-break")
        if not content:
            return ("Не найдено", "Не найдено")

        desc_p = content.select_one('p[itemprop="description"]')
        if desc_p:
            desc_text = desc_p.get_text(" ", strip=True).strip()
            address = self._extract_address_from_desc_p(desc_p)
            return (desc_text if desc_text else "Не найдено", address)

        ps = content.select("p")
        full = "\n".join(p.get_text(" ", strip=True).strip() for p in ps if p.get_text(strip=True))
        addr = self._extract_address_from_text(full)
        return (full if full.strip() else "Не найдено", addr)

    # ---------- listing ----------
    def get_listing_urls(self, category_url: str, max_lots: int = 300):
        lot_links = set()
        current_page = 1

        while len(lot_links) < max_lots:
            print(f"Листинг: страница {current_page}")
            separator = "&" if "?" in category_url else "?"
            url = f"{category_url}{separator}page={current_page}"

            try:
                self.driver.get(url)
            except Exception:
                # иногда сайт тормозит; попробуем продолжить
                pass

            try:
                # Ждем появления ссылок на лоты, чтобы убедиться, что контент загружен
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/lot/']")))
                time.sleep(2.0)
            except TimeoutException:
                print("Листинг не загрузился или страницы закончились.")
                break

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            links = soup.select("a[href*='/lot/']")
            if not links:
                print("Ссылки на лоты не найдены (конец списка).")
                break

            before = len(lot_links)
            for a in links:
                if len(lot_links) >= max_lots:
                    break
                href = (a.get("href") or "").strip()
                if not href or href.startswith("#"):
                    continue
                full_link = href if href.startswith("http") else self.base_url + href
                lot_links.add(full_link.split("#")[0])

            print(f"  найдено ссылок: {len(links)}, уникальных всего: {len(lot_links)}")

            if len(lot_links) == before:
                print("Новых лотов не добавилось — остановка.")
                break

            time.sleep(random.uniform(1.6, 2.8))
            current_page += 1

        return list(lot_links)

    # ---------- lot parsing (returns dict) ----------
    def parse_lot_page(self, url: str):
        try:
            self.driver.get(url)
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(random.uniform(1.1, 1.9))

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            lot_number, trade_number = self._extract_lot_and_trade_numbers(soup)
            prices = self._parse_info_wrapper(soup, "Цены")
            dates = self._parse_info_wrapper(soup, "Даты торгов")

            start_price = prices.get("Начальная", "Не найдено")
            step = prices.get("Шаг повышения", "Не найдено")
            zadatok = prices.get("Задаток", "Отсутствует") or "Отсутствует"

            accept_from = dates.get("Приём заявок с", "Не найдено")
            accept_to = dates.get("Приём заявок до", "Не найдено")
            auction_date = dates.get("Дата проведения", "Не найдено")

            status = self._extract_status(soup)
            description, address = self._extract_description_and_address(soup)
            debtor, inn_debtor, contact_person = self._extract_debtor_inn_contact(soup)

            return {
                "URL": url,
                "Номер лота": lot_number,
                "Торги №": trade_number,
                "Адрес": address,
                "Начальная цена": start_price,
                "Шаг повышения": step,
                "Задаток": zadatok,
                "Приём заявок с": accept_from,
                "Приём заявок до": accept_to,
                "Дата проведения": auction_date,
                "Статус": status,
                "Должник": debtor,
                "ИНН должника": inn_debtor,
                "Контактное лицо": contact_person,
                "Описание": description,
            }

        except Exception as e:
            print(f"Ошибка парсинга: {url} -> {e}")
            return None


# ----------------------------
# Excel: split by auction date (sheets)
# ----------------------------
def _parse_bankrot_datetime(value: str):
    """
    Ожидаем строки вида:
    '04.02.2026 12:00' или '04.02.2026 12:00' (внутри могут быть nbsp/лишние пробелы)
    """
    if not value or value == "Не найдено":
        return None
    v = re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()
    # чаще всего 'dd.mm.yyyy hh:mm'
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(v, fmt)
        except Exception:
            pass
    return None


def save_to_excel_split_by_date(rows, filename: str):
    if not rows:
        print("Нет данных для сохранения.")
        return

    df = pd.DataFrame(rows)

    # техническая колонка для группировки
    df["_auction_dt"] = df["Дата проведения"].apply(_parse_bankrot_datetime)
    df["_auction_date"] = df["_auction_dt"].apply(lambda x: x.date().isoformat() if x else "unknown")

    # сортировка
    df = df.sort_values(by=["_auction_date", "_auction_dt"], kind="stable")

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        # Общий лист
        df.drop(columns=["_auction_dt", "_auction_date"]).to_excel(writer, index=False, sheet_name="ALL")

        # Листы по датам проведения
        for date_key, g in df.groupby("_auction_date"):
            sheet = date_key
            # Excel sheet name limit 31 chars
            if len(sheet) > 31:
                sheet = sheet[:31]
            g.drop(columns=["_auction_dt", "_auction_date"]).to_excel(writer, index=False, sheet_name=sheet)

        # автоширина (без фанатизма)
        from openpyxl.utils import get_column_letter
        for sheet_name, ws in writer.sheets.items():
            # считаем ширины по первой ~300 строке (чтобы не тормозить)
            max_row = min(ws.max_row, 300)
            for col_idx in range(1, ws.max_column + 1):
                col_letter = get_column_letter(col_idx)
                max_len = 0
                for r in range(1, max_row + 1):
                    val = ws.cell(row=r, column=col_idx).value
                    if val is None:
                        continue
                    max_len = max(max_len, len(str(val)))
                ws.column_dimensions[col_letter].width = min(max_len + 2, 80)

    print(f"Excel сохранён: {filename}")


# ----------------------------
# Parallel worker
# ----------------------------
def worker_parse(base_url: str, urls, headless: bool, worker_id: int):
    parser = BankrotParser(base_url, headless=headless)
    out = []
    try:
        for i, url in enumerate(urls, start=1):
            row = parser.parse_lot_page(url)
            if row:
                out.append(row)
            # лёгкая этика + защита от банов
            time.sleep(random.uniform(0.8, 1.6))
        return out
    finally:
        parser.close()


def chunk_list(items, n_chunks):
    n_chunks = max(1, int(n_chunks))
    chunks = [[] for _ in range(n_chunks)]
    for idx, item in enumerate(items):
        chunks[idx % n_chunks].append(item)
    return chunks


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    BASE_URL = "https://bankrotbaza.ru"

    # ✅ Только квартиры
    APARTMENTS_URL = "https://bankrotbaza.ru/search?comb=all&category%5B%5D=27&type_auction=on&sort=created_desc"

    # --- Будущие апгрейды ---
    # CATEGORIES = {
    #     "2": ("Автомобили", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=1&type_auction=on&sort=created_desc"),
    #     "3": ("Земельные участки", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=12&type_auction=on&sort=created_desc"),
    #     "4": ("Нежилая недвижимость", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=5&type_auction=on&sort=created_desc"),
    #     "5": ("Жилая недвижимость", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=10&type_auction=on&sort=created_desc"),
    #     "6": ("Грузовой транспорт", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=2&type_auction=on&sort=created_desc"),
    #     "7": ("Задолженности и права требования", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=33&type_auction=on&sort=created_desc"),
    #     "8": ("Ценные бумаги", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=30&type_auction=on&sort=created_desc"),
    #     "9": ("ТМЦ", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=24&type_auction=on&sort=created_desc"),
    #     "10": ("Станки", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=16&type_auction=on&sort=created_desc"),
    #     "11": ("Коммерческий транспорт", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=3&type_auction=on&sort=created_desc"),
    #     "12": ("Имущественный комплекс", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=36&type_auction=on&sort=created_desc"),
    #     "13": ("Сельхоз недвижимость", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=40&type_auction=on&sort=created_desc"),
    #     "14": ("Сельхоз транспорт", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=41&type_auction=on&sort=created_desc"),
    #     "15": ("Драгоценности", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=31&type_auction=on&sort=created_desc"),
    #     "16": ("Сельхоз оборудование", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=42&type_auction=on&sort=created_desc"),
    #     "17": ("Мебель", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=25&type_auction=on&sort=created_desc"),
    #     "18": ("ПО", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=22&type_auction=on&sort=created_desc"),
    #     "19": ("Электрический инвентарь", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=21&type_auction=on&sort=created_desc"),
    #     "20": ("Компьютеры и оргтехника", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=20&type_auction=on&sort=created_desc"),
    #     "21": ("Торговое оборудование", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=19&type_auction=on&sort=created_desc"),
    #     "22": ("Производство прочее", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=18&type_auction=on&sort=created_desc"),
    #     "23": ("Производственные линии", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=17&type_auction=on&sort=created_desc"),
    #     "24": ("Электрическое пром. оборудование", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=15&type_auction=on&sort=created_desc"),
    #     "25": ("Бытовки и незавершенное строительство", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=14&type_auction=on&sort=created_desc"),
    #     "26": ("Гаражи и машиноместа", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=13&type_auction=on&sort=created_desc"),
    #     "27": ("Мототехника", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=9&type_auction=on&sort=created_desc"),
    #     "28": ("Авиатехника", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=8&type_auction=on&sort=created_desc"),
    #     "29": ("Водный транспорт", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=7&type_auction=on&sort=created_desc"),
    #     "30": ("Прицепы", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=6&type_auction=on&sort=created_desc"),
    #     "31": ("Автобусы", "https://bankrotbaza.ru/search?comb=all&category%5B%5D=4&type_auction=on&sort=created_desc"),
    # }

    MAX_LOTS = 100
    WORKERS = 3
    HEADLESS = False
    SEEN_FILE = "seen_lots.json"

    # 1) собираем ссылки листингом 
    listing_parser = BankrotParser(BASE_URL, headless=HEADLESS)
    try:
        print("Сбор ссылок (квартиры)...")
        all_links = listing_parser.get_listing_urls(APARTMENTS_URL, max_lots=MAX_LOTS)
    finally:
        listing_parser.close()

    print(f"Собрано ссылок: {len(all_links)}")

    # 2) фильтруем уже собранные
    store = SeenLotsStore(SEEN_FILE)
    seen = store.load()

    new_links = [u for u in all_links if u not in seen]
    print(f"Новых (непарсенных) лотов: {len(new_links)} / уже было: {len(all_links) - len(new_links)}")

    if not new_links:
        print("Ничего нового — выходим.")
        raise SystemExit(0)

    # 3) параллельный парсинг (каждому потоку — свой Chrome)
    chunks = chunk_list(new_links, WORKERS)
    results = []

    print(f"Запуск параллельно: {WORKERS} браузера(ов)")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = []
        for wid, ch in enumerate(chunks, start=1):
            if not ch:
                continue
            futures.append(ex.submit(worker_parse, BASE_URL, ch, HEADLESS, wid))

        for f in as_completed(futures):
            part = f.result()
            if part:
                results.extend(part)

    print(f"Успешно распарсено: {len(results)}")

    # 4) сохраняем Excel “по датам”
    today = datetime.now().strftime("%Y-%m-%d")
    out_xlsx = f"bankrot_apartments_{today}.xlsx"
    save_to_excel_split_by_date(results, out_xlsx)

    # 5) обновляем память о лотах (сохраняем даже если часть упала)
    parsed_urls = [r["URL"] for r in results if r and r.get("URL")]
    store.add_many(parsed_urls)
    store.save()
    print(f"Память сохранена: {SEEN_FILE} (всего: {len(store.seen)})")
