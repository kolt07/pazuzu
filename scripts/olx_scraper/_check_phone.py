# -*- coding: utf-8 -*-
import re
import sys
from pathlib import Path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.olx_scraper.fetcher import get_session

url = "https://www.olx.ua/d/uk/obyavlenie/prodazh-fermi-pdhodit-dlya-svinofermi-kurnika-korvnika-IDYkiYl.html"
s = get_session()
r = s.get(url, timeout=25)
r.encoding = r.apparent_encoding or "utf-8"
html = r.text

tels = re.findall(r'href=["\']tel:([^"\']+)["\']', html)
phones = re.findall(r'0\d{2}\s*\d{3}\s*\d{2}\s*\d{2}|0\d{2}\s*\d{3}\s*\d{4}', html)
print("tel: links:", tels[:5])
print("phone patterns:", list(set(phones))[:10])
print("Contains 096/097/098:", "096" in html or "097" in html or "098" in html)
