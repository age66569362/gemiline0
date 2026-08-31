# -*- coding: utf-8 -*-
"""
TaiPower PV Collector v0.3.4.3
Suntotal + TaiPower OpenData EMS + CWA Weather

本版設計：
1. 每 10 分鐘採集一次。
2. Suntotal：北 / 中 / 南三個代表案場。
3. EMS genary.json：保留 central 彰濱光、south 南鹽光。
   不再與 Suntotal 計算 Diff，也不做 Quality 判定。
4. 中央氣象署 M3：O-A0003-001 REST API，取得 8 個氣象欄位：
   AirTemperature、RelativeHumidity、WindSpeed、Precipitation、Weather、
   VisibilityDescription、AirPressure、WindDirection。
5. 中央氣象署 M4：O-A0091-001 改用已實測成功的 File API / JSON。
6. SolarRadiation 已確認為每日累積值，保留四欄：
   SolarRadiation、SolarInterval、SolarAvgWm2、SolarReset。
7. 每個來源獨立記錄資料更新時間，以及本程式實際抓取時間。
8. CSV 數字統一格式，避免浮點尾數與科學記號。
9. 修正北部 307 kW 容量：0.307 MW（不是 0.000307 MW）。
10. 保留 raw 原始資料，方便後續 AI / ML 稽核。
11. v0.3.4.1：M1/M2/M3/M4 獨立 retry / error isolation；單一來源失敗不終止整輪。
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests, os
from bs4 import BeautifulSoup



# ============================================================
# 基本設定
# ============================================================

VERSION = "0.3.4.3"

INTERVAL_SECONDS = 10 * 60
TIMEOUT = 20
RETRY_ATTEMPTS = 3
RETRY_DELAYS = (2, 5)

# 固定以程式檔所在目錄作為資料根目錄，避免因 PowerShell
# 啟動位置不同而把 CSV/JSONL/raw 寫到不同地方。
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR / "taipower_data_v0342"
RAW_DIR = BASE_DIR / "raw"
JSONL_DIR = BASE_DIR / "jsonl"
CSV_DIR = BASE_DIR / "csv"

IS_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS", "").lower() == "true"
SAVE_RAW = os.getenv(
    "SAVE_RAW",
    "0" if IS_GITHUB_ACTIONS else "1",
).strip().lower() not in {"0", "false", "no", "off"}

for directory in (RAW_DIR, JSONL_DIR, CSV_DIR):
    directory.mkdir(parents=True, exist_ok=True)


# ============================================================
# 官方來源
# ============================================================

SUNTOTAL_URL = "https://service.taipower.com.tw/dreweb/Suntotal"

EMS_URL = (
    "https://service.taipower.com.tw/"
    "data/opendata/apply/file/d006001/001.json"
)

CWA_API_URL = "https://opendata.cwa.gov.tw/api/v1/rest/datastore"

CWA_SOLAR_FILE_API_URL = (
    "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/"
    "O-A0091-001"
)

SOLAR_STATE_FILE = BASE_DIR / "solar_state.json"


# ============================================================
# CWA API Key
#
# 建議在 PowerShell 設定：
#   $env:CWA_API_KEY="你的API_KEY"
#
# 也可以直接把下面字串改成自己的 Key。
# ============================================================

CWA_API_KEY = os.getenv("CWA_API_KEY", "").strip()




# ============================================================
# 台灣時區
# ============================================================

TW_TZ = timezone(timedelta(hours=8))


# ============================================================
# HTTP Header
# ============================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


session = requests.Session()
session.headers.update(HEADERS)


# ============================================================
# 三個代表性光電案場
#
# 北部 307 kW = 0.307 MW
# ============================================================

PLANTS = {
    "north": {
        "city": "新北市",
        "site": "淡水淨水場",
        "capacity_mw": 0.307,
        "ems_name": None,
        "weather_station_id": "466900",   # 淡水
        "solar_station_id": "466900",     # 淡水
    },
    "central": {
        "city": "彰化縣",
        "site": "彰化彰濱",
        "capacity_mw": 100.0,
        "ems_name": "彰濱光",
        "weather_station_id": "467490",   # 臺中（M3 實測已確認）
        "solar_station_id": "467490",     # 臺中（署屬日射量站）
    },
    "south": {
        "city": "台南市",
        "site": "台南鹽田",
        "capacity_mw": 150.0,
        "ems_name": "南鹽光",
        "weather_station_id": "467410",   # 臺南（M3 實測已確認）
        "solar_station_id": "467410",     # 臺南（署屬日射量站）
    },
}


EMS_NAMES = {
    "彰濱光": "central",
    "南鹽光": "south",
}


# ============================================================
# 工具
# ============================================================

def now_tw():
    return datetime.now(TW_TZ)


def redact_secret(text):
    """避免 API Key 出現在 console / GitHub Actions log。"""
    text = str(text)
    if CWA_API_KEY:
        text = text.replace(CWA_API_KEY, "***REDACTED***")
    text = re.sub(
        r"([?&]Authorization=)[^&\\s]+",
        r"\\1***REDACTED***",
        text,
        flags=re.IGNORECASE,
    )
    return text


def safe_exception_text(exc):
    return redact_secret(repr(exc))


def timestamp():
    return now_tw().strftime("%Y%m%d_%H%M%S")


def iso_now():
    return now_tw().isoformat()


def safe_float(value):
    """
    只有真正缺值才回傳 None。
    0 是有效資料。
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().replace(",", "")

    if not text:
        return None

    if text.upper() in {
        "N/A",
        "NA",
        "-",
        "NONE",
        "NULL",
        "X",
        "XXX",
    }:
        return None

    match = re.search(
        r"-?\d+(?:\.\d+)?",
        text
    )

    if not match:
        return None

    try:
        return float(match.group(0))
    except Exception:
        return None


def round_or_none(value, digits):
    if value is None:
        return None

    try:
        return round(float(value), digits)
    except Exception:
        return None


def clean_cwa_value(value):
    """
    CWA 常見特殊碼：
      -99：缺值 / 異常
      -98：特殊缺值/狀態碼
      -990：部分觀測欄位（實測包含 Precipitation）可能出現的缺值碼
      X / XXX：儀器或缺值

    注意：Collector 只把已實測確認的特殊碼轉成 None，
    不採用「所有負值一律視為缺值」的過度泛化規則。
    """
    if value is None:
        return None

    text = str(value).strip()

    if text.upper() in {
        "",
        "X",
        "XXX",
        "N/A",
        "NA",
        "NONE",
        "NULL",
    }:
        return None

    try:
        number = float(text)

        if number in (-99, -98, -990):
            return None

        return number

    except Exception:
        return text




def run_with_retry(label, func, attempts=RETRY_ATTEMPTS):
    """執行單一來源請求；失敗時重試，但不讓例外拖垮整輪。"""
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            if attempt > 1:
                print(f"[RETRY] {label} attempt {attempt}/{attempts}...")
            return func()
        except Exception as e:
            last_error = e
            print(f"[ERROR] {label} attempt {attempt}/{attempts}: {safe_exception_text(e)}")
            if attempt < attempts:
                delay = RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)]
                print(f"[RETRY] {label} in {delay} sec...")
                time.sleep(delay)
    print(f"[FAILED] {label} after {attempts} attempts")
    return None


def empty_suntotal():
    return {
        "source": "suntotal",
        "request_time": None,
        "data_time": None,
        "total_power_mw": None,
        "sites": {
            region: {
                "city": PLANTS[region]["city"],
                "site": PLANTS[region]["site"],
                "capacity_mw": PLANTS[region]["capacity_mw"],
                "ratio": None,
                "estimated_mw": None,
                "status": None,
            }
            for region in ("north", "central", "south")
        },
    }


def empty_ems():
    return {
        "_collector_time": None,
        "_data_time": None,
        "_data_time_source": None,
    }
def parse_iso_datetime(value):
    if not value:
        return None

    text = str(value).strip()

    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def record_date_str(record):
    """
    以 record 自己的 collector_time 決定每日 CSV/JSONL 檔名。

    這可避免採集在 23:59:59 建立、但實際寫檔已跨到 00:00
    時，被寫進隔天檔案的午夜邊界問題。
    """
    dt = parse_iso_datetime(
        record.get("collector_time")
        if isinstance(record, dict)
        else None
    )

    if dt is None:
        dt = now_tw()

    return dt.strftime("%Y-%m-%d")


def infer_ems_data_time(dt):
    """
    genary.json 目前沒有穩定可直接使用的資料時間欄位。
    已知 EMS 網頁以 10 分鐘節點更新（00/10/20/...），
    因此這裡只做「依已知更新週期」的推定，並在 record
    另外標記 ems_data_time_source=inferred_schedule。
    """
    return dt.replace(
        minute=(dt.minute // 10) * 10,
        second=0,
        microsecond=0,
    ).isoformat()


# ============================================================
# Suntotal
# ============================================================

def fetch_suntotal():
    print("[Suntotal] Request...")

    request_time = now_tw()

    response = session.post(
        SUNTOTAL_URL,
        timeout=TIMEOUT,
    )

    response.raise_for_status()
    response.encoding = "utf-8"

    html = response.text

    print(
        f"[Suntotal] HTTP={response.status_code} "
        f"bytes={len(response.content)}"
    )

    return {
        "request_time": request_time.isoformat(),
        "html": html,
    }


def parse_suntotal(html):
    soup = BeautifulSoup(
        html,
        "html.parser"
    )

    text_content = soup.get_text(
        " ",
        strip=True
    )

    # --------------------------------------------------------
    # 資料時間
    # --------------------------------------------------------

    data_time = None

    patterns = [
        r"資料時間\s*[:：]?\s*(\d{1,2}:\d{2})",
        r"(\d{1,2}:\d{2})",
    ]

    for pattern in patterns:
        match = re.search(
            pattern,
            text_content
        )

        if match:
            data_time = match.group(1)
            break

    # --------------------------------------------------------
    # 總即時發電功率
    # --------------------------------------------------------

    total_power_mw = None

    power_patterns = [
        r"([\d,]+(?:\.\d+)?)\s*MW",
        r"([\d,]+(?:\.\d+)?)\s*MW",
    ]

    for pattern in power_patterns:
        match = re.search(
            pattern,
            text_content,
            re.IGNORECASE
        )

        if match:
            total_power_mw = safe_float(
                match.group(1)
            )
            break

    # --------------------------------------------------------
    # 三個案場
    # --------------------------------------------------------

    sites = {}

    site_map = {
        "north": ("新北市", "淡水淨水場"),
        "central": ("彰化縣", "彰化彰濱"),
        "south": ("台南市", "台南鹽田"),
    }

    for region, (city, site) in site_map.items():

        ratio = None
        status = None

        # 使用案場名稱之後的第一個百分比
        pattern = (
            rf"{re.escape(site)}"
            rf".{{0,1200}}?"
            rf"([\d]+(?:\.\d+)?)%"
        )

        match = re.search(
            pattern,
            text_content,
            re.DOTALL
        )

        if match:
            ratio = safe_float(
                match.group(1)
            )

        # 狀態
        status_match = re.search(
            rf"{re.escape(site)}"
            rf".{{0,500}}?"
            rf"(發電中|停止發電|停機|故障)",
            text_content,
            re.DOTALL
        )

        if status_match:
            status = status_match.group(1)

        capacity_mw = PLANTS[region]["capacity_mw"]

        estimated_mw = None

        if ratio is not None:
            estimated_mw = (
                capacity_mw
                * ratio
                / 100.0
            )

        sites[region] = {
            "city": city,
            "site": site,
            "capacity_mw": capacity_mw,
            "ratio": ratio,
            "estimated_mw": estimated_mw,
            "status": status,
        }

    return {
        "source": "suntotal",
        "data_time": data_time,
        "total_power_mw": total_power_mw,
        "sites": sites,
    }


# ============================================================
# EMS
# ============================================================

def fetch_ems():
    print("[EMS] Request...")

    request_time = now_tw()

    response = session.get(
        EMS_URL,
        timeout=TIMEOUT,
    )

    response.raise_for_status()
    response.encoding = "utf-8"

    raw = response.text

    print(
        f"[EMS] HTTP={response.status_code} "
        f"bytes={len(response.content)}"
    )

    return {
        "request_time": request_time.isoformat(),
        "raw": raw,
    }


def parse_ems(raw):
    """
    M2：解析台電官方 OpenData d006001/001.json。

    使用官方 DateTime，並只保留 EMS_NAMES 指定的彰濱光 / 南鹽光。
    """
    content = raw.lstrip("\ufeff")
    data = json.loads(content)

    if not isinstance(data, dict):
        raise RuntimeError("TaiPower OpenData payload is not a JSON object")

    rows = data.get("aaData")
    if not isinstance(rows, list):
        raise RuntimeError("TaiPower OpenData payload missing aaData list")

    official_data_time = data.get("DateTime")
    result = {}

    print(f"[EMS] OpenData rows: {len(rows)}")
    print(f"[EMS] Official data time: {official_data_time}")

    for row in rows:
        if not isinstance(row, dict):
            continue

        name = str(row.get("機組名稱", "")).strip()
        if name not in EMS_NAMES:
            continue

        capacity_mw = safe_float(row.get("裝置容量(MW)"))
        power_mw = safe_float(row.get("淨發電量(MW)"))
        ratio = safe_float(row.get("淨發電量/裝置容量比(%)"))
        status = str(row.get("備註", "") or "").strip()

        region = EMS_NAMES[name]

        result[region] = {
            "name": name,
            "capacity_mw": capacity_mw,
            "power_mw": power_mw,
            "ratio": ratio,
            "status": status,
        }

        print(
            "[EMS FOUND]",
            region,
            name,
            f"{power_mw} MW",
            f"{ratio}%",
        )

    result["_official_data_time"] = official_data_time
    return result


# ============================================================
# CWA API - M3 Weather + M4 Solar
# ============================================================

def cwa_request(dataset_id, station_id):
    """M3：O-A0003-001 REST API，使用已實測成功的 StationId 參數。"""
    if not CWA_API_KEY:
        raise RuntimeError(
            "尚未設定 CWA_API_KEY。"
            "請先在 PowerShell 執行："
            '$env:CWA_API_KEY="你的API_KEY"'
        )

    url = f"{CWA_API_URL}/{dataset_id}"

    params = {
        "Authorization": CWA_API_KEY,
        "format": "JSON",
        "StationId": station_id,
    }

    response = session.get(
        url,
        params=params,
        timeout=TIMEOUT,
    )

    response.raise_for_status()
    return response.json()


def cwa_solar_file_request():
    """
    M4：O-A0091-001 官方 File API / JSON。

    REST datastore 對 O-A0091-001 已實測會回 404；File API 已實測可取得
    34 個日射量站，因此正式 Collector 固定走 File API。
    """
    if not CWA_API_KEY:
        raise RuntimeError(
            "尚未設定 CWA_API_KEY。"
            "請先在 PowerShell 執行："
            '$env:CWA_API_KEY="你的API_KEY"'
        )

    request_time = now_tw()

    params = {
        "Authorization": CWA_API_KEY,
        "format": "JSON",
    }

    response = session.get(
        CWA_SOLAR_FILE_API_URL,
        params=params,
        timeout=TIMEOUT,
    )

    response.raise_for_status()

    return {
        "request_time": request_time.isoformat(),
        "payload": response.json(),
        "bytes": len(response.content),
    }


def get_records_station_list(payload):
    """
    同時支援 REST 的 records.Station 與 File API 的 dataset.Station。
    不把外層 JSON 結構寫死，遞迴尋找任何 Station list/dict。
    """
    if isinstance(payload, dict):
        stations = payload.get("Station")

        if isinstance(stations, dict):
            return [stations]

        if isinstance(stations, list):
            return stations

        records = payload.get("records")
        if isinstance(records, dict):
            stations = records.get("Station")
            if isinstance(stations, dict):
                return [stations]
            if isinstance(stations, list):
                return stations

        for value in payload.values():
            result = get_records_station_list(value)
            if result:
                return result

    elif isinstance(payload, list):
        for value in payload:
            result = get_records_station_list(value)
            if result:
                return result

    return []


def parse_cwa_general_station(payload, station_id):
    """M3：解析 O-A0003-001 的 8 個正式氣象欄位。"""
    stations = get_records_station_list(payload)

    selected = None

    for station in stations:
        sid = str(station.get("StationId", "")).strip()
        if sid == station_id:
            selected = station
            break

    # StationId REST 查詢正常情況只會回一站；仍保留 fallback。
    if selected is None and len(stations) == 1:
        selected = stations[0]

    if not selected:
        return None

    obs_time = selected.get("ObsTime", {})
    data_time = (
        obs_time.get("DateTime")
        if isinstance(obs_time, dict)
        else None
    )

    weather = selected.get("WeatherElement", {})
    if not isinstance(weather, dict):
        weather = {}

    # O-A0003-001 的 Precipitation 位於 WeatherElement.Now.Precipitation。
    now_block = weather.get("Now", {})
    if not isinstance(now_block, dict):
        now_block = {}

    return {
        "station_id": station_id,
        "station_name": selected.get("StationName"),
        "data_time": data_time,
        "air_temperature": clean_cwa_value(
            weather.get("AirTemperature")
        ),
        "relative_humidity": clean_cwa_value(
            weather.get("RelativeHumidity")
        ),
        "wind_speed": clean_cwa_value(
            weather.get("WindSpeed")
        ),
        "precipitation": clean_cwa_value(
            now_block.get("Precipitation")
        ),
        "weather": clean_cwa_value(
            weather.get("Weather")
        ),
        "visibility": clean_cwa_value(
            weather.get("VisibilityDescription")
        ),
        "air_pressure": clean_cwa_value(
            weather.get("AirPressure")
        ),
        "wind_direction": clean_cwa_value(
            weather.get("WindDirection")
        ),
    }


def parse_cwa_solar_station(payload, station_id):
    """M4：從 File API 完整資料中找指定 StationId。"""
    stations = get_records_station_list(payload)

    selected = None

    for station in stations:
        sid = str(station.get("StationId", "")).strip()
        if sid == station_id:
            selected = station
            break

    if not selected:
        return None

    obs_time = selected.get("ObsTime", {})
    data_time = (
        obs_time.get("DateTime")
        if isinstance(obs_time, dict)
        else None
    )

    weather = selected.get("WeatherElement", {})
    if not isinstance(weather, dict):
        weather = {}

    value = weather.get("SolarRadiation")

    return {
        "station_id": station_id,
        "station_name": selected.get("StationName"),
        "data_time": data_time,
        "solar_radiation": clean_cwa_value(value),
    }


def load_solar_state():
    """讓 Collector 重啟後仍能接續計算 SolarInterval。"""
    if not SOLAR_STATE_FILE.exists():
        return {}

    try:
        data = json.loads(
            SOLAR_STATE_FILE.read_text(encoding="utf-8")
        )
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print("[WARN] Solar state load failed:", safe_exception_text(e))
        return {}


def save_solar_state(state):
    try:
        SOLAR_STATE_FILE.write_text(
            json.dumps(
                state,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception as e:
        print("[WARN] Solar state save failed:", safe_exception_text(e))


def derive_solar_metrics(region, solar, previous_state):
    """
    SolarRadiation：CWA 原始每日累積值 MJ/m²
    SolarInterval：相鄰有效資料的區間增量 MJ/m²
    SolarAvgWm2：區間平均日射強度 W/m²
    SolarReset：偵測到每日累積量重置

    實測顯示：00:00 資料可能仍保留前一日累積值，00:10 才歸零，
    因此不能寫死換日時間；以「累積值下降」偵測 reset。
    """
    result = {
        "solar_radiation": None,
        "solar_interval": None,
        "solar_avg_w_m2": None,
        "solar_reset": False,
    }

    if not solar:
        return result

    current_value = clean_cwa_value(
        solar.get("solar_radiation")
    )
    current_dt = parse_iso_datetime(
        solar.get("data_time")
    )

    if not isinstance(current_value, (int, float)):
        result["solar_radiation"] = current_value
        return result

    current_value = float(current_value)
    result["solar_radiation"] = current_value

    prev = previous_state.get(region, {})
    prev_value = safe_float(prev.get("solar_radiation"))
    prev_dt = parse_iso_datetime(prev.get("data_time"))

    # 第一筆或重啟後沒有可用上一筆：只保留原始累積值。
    if prev_value is None or prev_dt is None or current_dt is None:
        return result

    dt_seconds = (current_dt - prev_dt).total_seconds()

    # CWA 同一筆資料被重複抓到，不做差分。
    if dt_seconds <= 0:
        return result

    # 累積量下降 = reset。不要產生負的 interval。
    if current_value < prev_value:
        result["solar_reset"] = True
        result["solar_interval"] = current_value

        # reset 當筆通常是 0；若不是 0，無法精確知道 reset 發生在區間哪一秒，
        # 因此避免製造假的平均 W/m²。
        if current_value == 0:
            result["solar_avg_w_m2"] = 0.0

        return result

    interval = current_value - prev_value
    result["solar_interval"] = interval
    result["solar_avg_w_m2"] = (
        interval * 1_000_000.0 / dt_seconds
    )

    return result


def update_solar_state(state, region, solar):
    """只用有效 SolarRadiation + data_time 更新狀態。"""
    if not solar:
        return

    value = clean_cwa_value(solar.get("solar_radiation"))
    data_time = solar.get("data_time")

    if not isinstance(value, (int, float)) or not data_time:
        return

    state[region] = {
        "station_id": solar.get("station_id"),
        "data_time": data_time,
        "solar_radiation": float(value),
    }

# ============================================================
# 資料整理
# ============================================================

def build_record(
    collector_time,
    suntotal,
    ems,
    weather_by_region,
):
    regions = {}

    for region in (
        "north",
        "central",
        "south",
    ):

        plant = PLANTS[region]

        st = suntotal["sites"].get(
            region,
            {}
        )

        em = ems.get(
            region,
            {}
        )

        weather = weather_by_region.get(
            region,
            {}
        )

        regions[region] = {
            "city": plant["city"],
            "site": plant["site"],
            "capacity_mw": plant[
                "capacity_mw"
            ],

            "suntotal": {
                "ratio": st.get("ratio"),
                "estimated_mw": st.get(
                    "estimated_mw"
                ),
                "status": st.get("status"),
            },

            "ems": {
                "name": em.get("name"),
                "power_mw": em.get(
                    "power_mw"
                ),
                "ratio": em.get("ratio"),
                "status": em.get("status"),
            },

            "weather": weather,
        }

    return {
        "collector_version": VERSION,
        "collector_time": collector_time.isoformat(),

        "suntotal": {
            "collector_time": suntotal.get(
                "request_time"
            ),
            "data_time": suntotal.get(
                "data_time"
            ),
            "total_power_mw": round_or_none(
                suntotal.get(
                    "total_power_mw"
                ),
                3,
            ),
        },

        "ems": {
            "collector_time": ems.get(
                "_collector_time"
            ),
            "data_time": ems.get(
                "_data_time"
            ),
            "data_time_source": ems.get(
                "_data_time_source"
            ),
        },

        "regions": regions,
    }


# ============================================================
# Raw 保存
# ============================================================

def save_raw(
    collector_id,
    suntotal_html,
    ems_raw,
    cwa_raw,
):
    raw_dir = (
        RAW_DIR
        / collector_id
    )

    raw_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    if suntotal_html:
        (
            raw_dir
            / "suntotal.html"
        ).write_text(
            suntotal_html,
            encoding="utf-8"
        )

    if ems_raw:
        (
            raw_dir
            / "ems_genary.json"
        ).write_text(
            ems_raw,
            encoding="utf-8"
        )

    for region, payloads in (
        cwa_raw.items()
    ):
        region_dir = (
            raw_dir
            / "cwa"
        )
        region_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        for name, payload in payloads.items():

            if payload is None:
                continue

            (
                region_dir
                / f"{region}_{name}.json"
            ).write_text(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    indent=2
                ),
                encoding="utf-8"
            )


# ============================================================
# JSONL
# ============================================================

def save_jsonl(record):
    date_str = record_date_str(record)

    file_path = (
        JSONL_DIR
        / f"pv_{date_str}.jsonl"
    )

    with file_path.open(
        "a",
        encoding="utf-8"
    ) as f:

        f.write(
            json.dumps(
                record,
                ensure_ascii=False,
                separators=(",", ":")
            )
            + "\n"
        )


# ============================================================
# CSV
# ============================================================

CSV_FIELDS = [
    "collector_time",

    "suntotal_collector_time",
    "suntotal_data_time",
    "suntotal_total_mw",

    "ems_collector_time",
    "ems_data_time",
    "ems_data_time_source",

    "weather_collector_time",
    "weather_data_time",
    "solar_collector_time",
    "solar_data_time",

    "region",
    "city",
    "site",
    "capacity_mw",

    "suntotal_ratio",
    "suntotal_power_mw",
    "suntotal_status",

    "ems_name",
    "ems_power_mw",
    "ems_ratio",
    "ems_status",

    "weather_station_id",
    "weather_station_name",
    "solar_station_id",
    "solar_station_name",

    # M4：原始 + 衍生四欄
    "SolarRadiation",
    "SolarInterval",
    "SolarAvgWm2",
    "SolarReset",

    # M3：8 個氣象欄位
    "AirTemperature",
    "RelativeHumidity",
    "WindSpeed",
    "Precipitation",
    "Weather",
    "Visibility",
    "Pressure",
    "WindDirection",
]


def csv_num(value, digits):
    if value is None:
        return ""

    try:
        # 固定小數位，避免 3.129999999999 / 科學記號。
        return f"{float(value):.{digits}f}"
    except Exception:
        return ""


def save_csv(record):
    date_str = record_date_str(record)
    file_path = CSV_DIR / f"pv_{date_str}.csv"
    file_exists = file_path.exists()

    with file_path.open(
        "a",
        newline="",
        encoding="utf-8-sig",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=CSV_FIELDS,
        )

        if not file_exists:
            writer.writeheader()

        for region, data in record["regions"].items():
            weather = data["weather"]

            writer.writerow({
                "collector_time": record["collector_time"],

                "suntotal_collector_time":
                    record["suntotal"]["collector_time"],
                "suntotal_data_time":
                    record["suntotal"]["data_time"],
                "suntotal_total_mw": csv_num(
                    record["suntotal"]["total_power_mw"], 3
                ),

                "ems_collector_time":
                    record["ems"].get("collector_time"),
                "ems_data_time":
                    record["ems"].get("data_time"),
                "ems_data_time_source":
                    record["ems"].get("data_time_source"),

                "weather_collector_time":
                    weather.get("weather_request_time"),
                "weather_data_time":
                    weather.get("weather_data_time"),
                "solar_collector_time":
                    weather.get("solar_request_time"),
                "solar_data_time":
                    weather.get("solar_data_time"),

                "region": region,
                "city": data["city"],
                "site": data["site"],
                "capacity_mw": csv_num(
                    data["capacity_mw"], 3
                ),

                "suntotal_ratio": csv_num(
                    data["suntotal"]["ratio"], 2
                ),
                "suntotal_power_mw": csv_num(
                    data["suntotal"]["estimated_mw"], 3
                ),
                "suntotal_status":
                    data["suntotal"]["status"],

                "ems_name": data["ems"]["name"],
                "ems_power_mw": csv_num(
                    data["ems"]["power_mw"], 3
                ),
                "ems_ratio": csv_num(
                    data["ems"]["ratio"], 3
                ),
                "ems_status": data["ems"]["status"],

                "weather_station_id":
                    weather.get("weather_station_id"),
                "weather_station_name":
                    weather.get("weather_station_name"),
                "solar_station_id":
                    weather.get("solar_station_id"),
                "solar_station_name":
                    weather.get("solar_station_name"),

                "SolarRadiation": csv_num(
                    weather.get("solar_radiation"), 2
                ),
                "SolarInterval": csv_num(
                    weather.get("solar_interval"), 4
                ),
                "SolarAvgWm2": csv_num(
                    weather.get("solar_avg_w_m2"), 2
                ),
                "SolarReset": (
                    "1" if weather.get("solar_reset") else "0"
                ),

                "AirTemperature": csv_num(
                    weather.get("air_temperature"), 1
                ),
                "RelativeHumidity": csv_num(
                    weather.get("relative_humidity"), 1
                ),
                "WindSpeed": csv_num(
                    weather.get("wind_speed"), 1
                ),
                "Precipitation": csv_num(
                    weather.get("precipitation"), 1
                ),
                "Weather": weather.get("weather"),
                "Visibility": weather.get("visibility"),
                "Pressure": csv_num(
                    weather.get("air_pressure"), 1
                ),
                "WindDirection": csv_num(
                    weather.get("wind_direction"), 1
                ),
            })

# ============================================================
# Console
# ============================================================

def fmt(value, digits=3):
    if value is None:
        return "N/A"

    if isinstance(value, (int, float)):
        return f"{float(value):.{digits}f}"

    return str(value)


def print_record(record):
    print()
    print("=" * 132)

    print("Collector:", record["collector_time"])
    print(
        "Suntotal collector:",
        record["suntotal"]["collector_time"],
    )
    print(
        "Suntotal data time:",
        record["suntotal"]["data_time"],
    )
    print(
        "Suntotal total MW:",
        fmt(record["suntotal"]["total_power_mw"], 3),
    )
    print(
        "EMS collector:",
        record["ems"].get("collector_time"),
    )
    print(
        "EMS data time:",
        record["ems"].get("data_time"),
    )

    print("-" * 132)
    print(
        f"{'Region':<9}"
        f"{'ST %':>8}"
        f"{'ST MW':>10}"
        f"{'EMS MW':>10}"
        f"{'Temp':>8}"
        f"{'RH':>7}"
        f"{'Wind':>8}"
        f"{'SolarCum':>11}"
        f"{'SolarΔ':>10}"
        f"{'W/m²':>10}"
        f"{'Reset':>8}"
        f"{'Weather':>15}"
    )
    print("-" * 132)

    for region, data in record["regions"].items():
        weather = data["weather"]

        print(
            f"{region:<9}"
            f"{fmt(data['suntotal']['ratio'], 2):>8}"
            f"{fmt(data['suntotal']['estimated_mw'], 3):>10}"
            f"{fmt(data['ems']['power_mw'], 3):>10}"
            f"{fmt(weather.get('air_temperature'), 1):>8}"
            f"{fmt(weather.get('relative_humidity'), 1):>7}"
            f"{fmt(weather.get('wind_speed'), 1):>8}"
            f"{fmt(weather.get('solar_radiation'), 2):>11}"
            f"{fmt(weather.get('solar_interval'), 4):>10}"
            f"{fmt(weather.get('solar_avg_w_m2'), 2):>10}"
            f"{str(bool(weather.get('solar_reset'))):>8}"
            f"{str(weather.get('weather') or 'N/A'):>15}"
        )

    print("-" * 132)
    print("Weather / Solar station information:")

    for region, data in record["regions"].items():
        weather = data["weather"]
        print(
            f"  {region}: "
            f"Weather={weather.get('weather_station_name')}"
            f"({weather.get('weather_station_id')}) "
            f"@ {weather.get('weather_data_time')} | "
            f"Solar={weather.get('solar_station_name')}"
            f"({weather.get('solar_station_id')}) "
            f"@ {weather.get('solar_data_time')}"
        )

# ============================================================
# 單次採集
# ============================================================

def collect_once():
    collector_time = now_tw()
    collector_id = timestamp()

    print()
    print("=" * 100)
    print(f"TaiPower PV Collector v{VERSION}")
    print("M1 Suntotal + M2 TaiPower OpenData EMS + M3 CWA Weather + M4 CWA Solar")
    print("Interval: 10 minutes")
    print("Independent retry/isolation enabled")
    print("Solar: cumulative + interval + average W/m² + reset flag")
    print("Collector ID:", collector_id)
    print("=" * 100)

    # M1 Suntotal -------------------------------------------------
    suntotal_raw = None
    suntotal = empty_suntotal()

    def m1_job():
        response = fetch_suntotal()
        parsed = parse_suntotal(response["html"])
        parsed["request_time"] = response["request_time"]
        return response, parsed

    m1_result = run_with_retry("M1 Suntotal", m1_job)
    if m1_result is not None:
        response, suntotal = m1_result
        suntotal_raw = response["html"]
        print("[OK] M1 Suntotal")

    # M2 EMS ------------------------------------------------------
    ems_raw = None
    ems = empty_ems()

    def m2_job():
        response = fetch_ems()
        parsed = parse_ems(response["raw"])
        parsed["_collector_time"] = response["request_time"]
        parsed["_data_time"] = parsed.pop("_official_data_time", None)
        parsed["_data_time_source"] = "official_datetime"
        return response, parsed

    m2_result = run_with_retry("M2 EMS", m2_job)
    if m2_result is not None:
        response, ems = m2_result
        ems_raw = response["raw"]
        print("[OK] M2 EMS")

    # M4 Solar：一次抓完整 payload，三區共用 ----------------------
    solar_request_time = None
    solar_payload = None

    solar_response = run_with_retry("M4 CWA Solar File API", cwa_solar_file_request)
    if solar_response is not None:
        solar_request_time = solar_response["request_time"]
        solar_payload = solar_response["payload"]
        print(
            "[OK] M4 CWA Solar File API "
            f"bytes={solar_response['bytes']}"
        )

    solar_state = load_solar_state()
    updated_solar_state = dict(solar_state)

    # M3 Weather + M4 Solar merge --------------------------------
    weather_by_region = {}
    cwa_raw = {}

    for region in ("north", "central", "south"):
        plant = PLANTS[region]
        weather_station_id = plant["weather_station_id"]
        solar_station_id = plant["solar_station_id"]

        general_payload = None
        general = None
        solar = None
        weather_request_time = None

        def weather_job():
            request_time = now_tw().isoformat()
            payload = cwa_request("O-A0003-001", weather_station_id)
            parsed = parse_cwa_general_station(payload, weather_station_id)
            if parsed is None:
                raise RuntimeError(
                    f"CWA Weather {region} parser returned None for {weather_station_id}"
                )
            return request_time, payload, parsed

        weather_result = run_with_retry(f"M3 CWA Weather {region}", weather_job)
        if weather_result is not None:
            weather_request_time, general_payload, general = weather_result
            print(f"[OK] M3 CWA Weather {region}")

        if solar_payload is not None:
            try:
                solar = parse_cwa_solar_station(solar_payload, solar_station_id)
            except Exception as e:
                print(f"[ERROR] M4 CWA Solar parse {region}: {safe_exception_text(e)}")

        solar_metrics = derive_solar_metrics(region, solar, solar_state)
        update_solar_state(updated_solar_state, region, solar)

        weather = {
            "weather_request_time": weather_request_time,
            "weather_data_time": general.get("data_time") if general else None,
            "solar_request_time": solar_request_time,
            "solar_data_time": solar.get("data_time") if solar else None,
            "weather_station_id": weather_station_id,
            "weather_station_name": general.get("station_name") if general else None,
            "solar_station_id": solar_station_id,
            "solar_station_name": solar.get("station_name") if solar else None,
            "solar_radiation": solar_metrics["solar_radiation"],
            "solar_interval": solar_metrics["solar_interval"],
            "solar_avg_w_m2": solar_metrics["solar_avg_w_m2"],
            "solar_reset": solar_metrics["solar_reset"],
            "air_temperature": general.get("air_temperature") if general else None,
            "relative_humidity": general.get("relative_humidity") if general else None,
            "wind_speed": general.get("wind_speed") if general else None,
            "precipitation": general.get("precipitation") if general else None,
            "weather": general.get("weather") if general else None,
            "visibility": general.get("visibility") if general else None,
            "air_pressure": general.get("air_pressure") if general else None,
            "wind_direction": general.get("wind_direction") if general else None,
        }

        weather_by_region[region] = weather
        cwa_raw[region] = {
            "general": general_payload,
            "solar": solar_payload,
        }

    # 只有已成功解析的 solar 才會改動 state；失敗模組不清掉歷史 state。
    try:
        save_solar_state(updated_solar_state)
    except Exception as e:
        print("[WARN] Solar state save failed:", safe_exception_text(e))

    # Raw ---------------------------------------------------------
    if SAVE_RAW:
        try:
            save_raw(collector_id, suntotal_raw, ems_raw, cwa_raw)
        except Exception as e:
            print("[WARN] Raw save failed:", safe_exception_text(e))
    else:
        print("[INFO] Raw saving disabled (SAVE_RAW=0)")

    # 無論單一來源是否失敗，都建立並保存本輪 record。
    record = build_record(collector_time, suntotal, ems, weather_by_region)

    try:
        save_jsonl(record)
        save_csv(record)
        print("[OK] JSONL + CSV saved")
    except Exception as e:
        print("[ERROR] Data save:", safe_exception_text(e))

    print_record(record)
    return True


# ============================================================
# 等待下一個 10 分鐘
# ============================================================

def sleep_until_next_10min():
    current = now_tw()

    next_minute = (
        ((current.minute // 10) + 1)
        * 10
    )

    if next_minute >= 60:

        nxt = (
            current
            + timedelta(hours=1)
        ).replace(
            minute=0,
            second=0,
            microsecond=0,
        )

    else:

        nxt = current.replace(
            minute=next_minute,
            second=0,
            microsecond=0,
        )

    seconds = max(
        1,
        int(
            (
                nxt - current
            ).total_seconds()
        )
    )

    print()
    print(
        "[WAIT] 下一次採集：",
        nxt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    print(
        f"[WAIT] 等待 {seconds} 秒"
    )

    time.sleep(
        seconds
    )


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="TaiPower PV Collector")
    parser.add_argument(
        "--once",
        action="store_true",
        help="只採集一次後結束；適合 GitHub Actions。",
    )
    args = parser.parse_args()

    run_once = args.once or IS_GITHUB_ACTIONS

    print("=" * 100)
    print(f"TaiPower PV Collector v{VERSION}")
    print("Suntotal + TaiPower OpenData EMS + CWA Weather")
    print("Interval: 10 minutes")
    print("Mode:", "single run" if run_once else "continuous")
    print("Raw saving:", "enabled" if SAVE_RAW else "disabled")
    if not run_once:
        print("按 Ctrl+C 可以停止採集器。")
    print("=" * 100)
    print(f"Data directory : {BASE_DIR}")
    print(f"CSV directory  : {CSV_DIR}")
    print(f"JSONL directory: {JSONL_DIR}")
    print(f"Raw directory  : {RAW_DIR}")

    if not CWA_API_KEY:
        print()
        print("[WARN] 尚未設定 CWA_API_KEY")
        print('[WARN] PowerShell：$env:CWA_API_KEY="你的API_KEY"')
        print("[WARN] GitHub Actions：請在 Repository Secrets 建立 CWA_API_KEY")
        print("[WARN] 未設定時仍會抓 Suntotal / EMS，但 CWA 欄位會是空值。")

    if run_once:
        collect_once()
        return

    try:
        while True:
            try:
                collect_once()
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print()
                print("[FATAL CYCLE ERROR]", safe_exception_text(e))

            sleep_until_next_10min()

    except KeyboardInterrupt:
        print()
        print("[STOP] 使用者停止採集器。")
        sys.exit(0)





if __name__ == "__main__":
    main()


