import os
import json
import time
import re
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from bs4 import BeautifulSoup
from openpyxl import load_workbook

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException


# ----------------------------
# Output columns (strict order)
# ----------------------------
OUTPUT_COLUMNS = [
    "Номер аукциона / лота",
    "Адрес объекта",
    "Начальная цена",
    "Шаг аукциона",
    "Размер задатка",
    "Дата и время начала / окончания торгов",
    "Статус аукциона",
    "Информация о должнике",
    "Описание объекта",
]


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
# Excel append (one file, append rows)
# ----------------------------
def append_to_excel(rows, filename: str, sheet_name: str = "ALL"):
    """
    rows: list[dict] with keys = OUTPUT_COLUMNS
    Appends new rows to an existing Excel file (or creates it).
    """
    if not rows:
        print("Нет новых данных для записи в Excel.")
        return

    df_new = pd.DataFrame(rows)

    # ensure all columns exist and strict order
    for col in OUTPUT_COLUMNS:
        if col not in df_new.columns:
            df_new[col] = ""
    df_new = df_new[OUTPUT_COLUMNS]

    # create file if not exists
    if not os.path.exists(filename):
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            df_new.to_excel(writer, index=False, sheet_name=sheet_name)
        print(f"Создан новый Excel: {filename}")
        return

    wb = load_workbook(filename)
    if sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        start_row = ws.max_row + 1
    else:
        ws = wb.create_sheet(sheet_name)
        start_row = 1

    # append
    with pd.ExcelWriter(filename, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
        if start_row == 1:
            df_new.to_excel(writer, index=False, sheet_name=sheet_name)
        else:
            df_new.to_excel(writer, index=False, sheet_name=sheet_name, header=False, startrow=start_row - 1)

    # quick autosize for the active sheet (not perfect but OK)
    wb = load_workbook(filename)
    ws = wb[sheet_name]
    max_row = min(ws.max_row, 300)
    for col_idx in range(1, ws.max_column + 1):
        max_len = 0
        for r in range(1, max_row + 1):
            v = ws.cell(row=r, column=col_idx).value
            if v is None:
                continue
            max_len = max(max_len, len(str(v)))
        ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = min(max_len + 2, 80)
    wb.save(filename)

    print(f"Добавлено строк в {filename}: {len(df_new)}")


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

        # Optional: if you want to use your Chrome profile (helps if site blocks)
        # Close all Chrome windows before enabling this!
        # chrome_options.add_argument(r"--user-data-dir=C:\Users\basch\AppData\Local\Google\Chrome\User Data")
        # chrome_options.add_argument(r"--profile-directory=Default")

        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        self.driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
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
                pass

            # IMPORTANT: site is slow — wait for lot links
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/lot/']")))
                time.sleep(2.0)
            except TimeoutException:
                print("Лоты не появились (возможно страницы закончились или контент не прогрузился).")
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

    # ---------- lot parsing ----------
    def parse_lot_page(self, url: str):
        """
        Returns: (url, output_row_dict) or (url, None)
        output_row_dict keys exactly match OUTPUT_COLUMNS
        """
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
            trade_period = f"{accept_from} — {accept_to}"

            status = self._extract_status(soup)

            description, address = self._extract_description_and_address(soup)

            debtor, inn_debtor, contact_person = self._extract_debtor_inn_contact(soup)
            debtor_info = f"{debtor}; ИНН: {inn_debtor}"
            if contact_person and contact_person != "Не найдено":
                debtor_info += f"; Контакт: {contact_person}"

            auction_lot = f"торги №{trade_number}, лот №{lot_number}"

            row = {
                "Номер аукциона / лота": auction_lot,
                "Адрес объекта": address,
                "Начальная цена": start_price,
                "Шаг аукциона": step,
                "Размер задатка": zadatok,
                "Дата и время начала / окончания торгов": trade_period,
                "Статус аукциона": status,
                "Информация о должнике": debtor_info,
                "Описание объекта": description,
            }

            return (url, row)

        except Exception as e:
            print(f"Ошибка парсинга: {url} -> {e}")
            return (url, None)


# ----------------------------
# Parallel helpers
# ----------------------------
def worker_parse(base_url: str, urls, headless: bool):
    parser = BankrotParser(base_url, headless=headless)
    out_rows = []
    out_urls = []
    try:
        for url in urls:
            u, row = parser.parse_lot_page(url)
            if row:
                out_rows.append(row)
                out_urls.append(u)
            time.sleep(random.uniform(0.8, 1.6))
        return out_rows, out_urls
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

    # Only apartments
    APARTMENTS_URL = "https://bankrotbaza.ru/search?comb=all&category%5B%5D=27&type_auction=on&sort=created_desc"

    # Settings
    MAX_LOTS = 300
    WORKERS = 3          # 2 or 3 recommended
    HEADLESS = False
    SEEN_FILE = "seen_lots.json"
    OUT_XLSX = "bankrot_apartments.xlsx"

    # 1) listing (single browser)
    listing_parser = BankrotParser(BASE_URL, headless=HEADLESS)
    try:
        print("Сбор ссылок (квартиры)...")
        all_links = listing_parser.get_listing_urls(APARTMENTS_URL, max_lots=MAX_LOTS)
    finally:
        listing_parser.close()

    print(f"Собрано ссылок: {len(all_links)}")

    # 2) filter already seen
    store = SeenLotsStore(SEEN_FILE)
    seen = store.load()

    new_links = [u for u in all_links if u not in seen]
    print(f"Новых (непарсенных) лотов: {len(new_links)} / уже было: {len(all_links) - len(new_links)}")

    if not new_links:
        print("Ничего нового — выходим.")
        raise SystemExit(0)

    # 3) parallel parse (each thread has its own Chrome)
    chunks = chunk_list(new_links, WORKERS)
    results_rows = []
    parsed_urls = []

    print(f"Запуск параллельно: {WORKERS} браузера(ов)")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = []
        for ch in chunks:
            if ch:
                futures.append(ex.submit(worker_parse, BASE_URL, ch, HEADLESS))

        for f in as_completed(futures):
            part_rows, part_urls = f.result()
            results_rows.extend(part_rows)
            parsed_urls.extend(part_urls)

    print(f"Успешно распарсено: {len(results_rows)}")

    # 4) append to ONE excel file
    append_to_excel(results_rows, OUT_XLSX, sheet_name="ALL")

    # 5) update seen store (so repeats won't parse next run)
    store.add_many(parsed_urls)
    store.save()
    print(f"Память сохранена: {SEEN_FILE} (всего: {len(store.seen)})")
