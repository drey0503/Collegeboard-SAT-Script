#!/usr/bin/env python3
"""
Features:
 - Fast concurrent probing for AP_HED files for the target date (America/New_York)
 - Caches discovered filenames per-date to avoid re-probing
 - Downloads found files into Desktop/PAScores/YYYY-MM-DD (or PASC_DOWNLOAD_DIR)
 - Sends a text summary email via SMTP (no attachments)
"""
import os
import time
import urllib.parse
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import smtplib
import ssl
from email.message import EmailMessage

# -------------------- Load env --------------------
env_path = os.environ.get("PASC_ENV_FILE", "creds.env")
load_dotenv(dotenv_path=env_path)

# -------------------- API config --------------------
BASE = "https://scoresdownload.collegeboard.org/pascoredwnld"
LOGIN_URL = f"{BASE}/login"
FILE_URL = f"{BASE}/file"
LIST_ENDPOINT_CANDIDATES = [
    f"{BASE}/files/list",
    f"{BASE}/files",
    f"{BASE}/file/list",
    f"{BASE}/list",
]

# Auth/config from env
MODE = os.environ.get("PASC_MODE", "token").lower()  # "token" or "creds"
USERNAME = os.environ.get("PASC_USER")
PASSWORD = os.environ.get("PASC_PASS")
TOKEN_ENV = os.environ.get("PASC_TOKEN")

PATTERNS_RAW = os.environ.get(
    "PASC_PATTERN", "AP_HED_Scores_5818_{date:%m%d%Y}_{n:04d}")
PATTERNS = [p.strip() for p in PATTERNS_RAW.split(",") if p.strip()]

MAX_NUM_PROBES = int(os.environ.get("PASC_MAX_NUM_PROBES", "1500"))
MAX_WORKERS = int(os.environ.get("PASC_MAX_WORKERS", "8"))
BATCH_SLEEP = float(os.environ.get("PASC_BATCH_SLEEP", "0.05"))
CONSECUTIVE_MISS_STOP = int(os.environ.get("PASC_CONSEC_MISS", "40"))

TZ = ZoneInfo("America/New_York")

# -------------------- Download directory --------------------
DEFAULT_DESKTOP = os.path.join(os.path.expanduser("~"), "Desktop")
BASE_DOWNLOAD_DIR = os.environ.get(
    "PASC_DOWNLOAD_DIR", os.path.join(DEFAULT_DESKTOP, "SATScores"))
today_iso = datetime.now(TZ).date().isoformat()
DOWNLOAD_DIR = os.path.join(BASE_DOWNLOAD_DIR, today_iso)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# -------------------- SMTP config --------------------
SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
EMAIL_FROM = os.environ.get("EMAIL_FROM", SMTP_USER)
EMAIL_TO = os.environ.get("EMAIL_TO")
EMAIL_USE_TLS = os.environ.get(
    "EMAIL_USE_TLS", "false").lower() in ("1", "true", "yes")

# -------------------- Utilities: API calls --------------------


def login_get_token(username: str, password: str) -> str:
    r = requests.post(
        LOGIN_URL, json={"username": username, "password": password}, timeout=30)
    r.raise_for_status()
    j = r.json()
    token = j.get("token")
    if not token:
        raise ValueError("Login response did not include token: " + repr(j))
    return token


def get_presigned_by_token(filename: str, token: str) -> requests.Response:
    encoded = urllib.parse.quote(filename, safe='')
    url = f"{FILE_URL}?filename={encoded}"
    return requests.post(url, json={"token": token}, timeout=30)


def get_presigned_by_creds(filename: str, username: str, password: str) -> requests.Response:
    encoded = urllib.parse.quote(filename, safe='')
    url = f"{FILE_URL}?filename={encoded}"
    return requests.post(url, json={"username": username, "password": password}, timeout=30)


def download_from_presigned_url(file_url: str, dest_path: str):
    with requests.get(file_url, stream=True, timeout=120) as r:
        r.raise_for_status()
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as fh:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)

# -------------------- List-endpoint discovery (preferred) --------------------


def try_list_endpoints(token: str = None, username: str = None, password: str = None):
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    for ep in LIST_ENDPOINT_CANDIDATES:
        try:
            r = requests.get(ep, headers=headers, timeout=20)
            if r.status_code == 200:
                names = _extract_filenames_from_json_safe(r)
                if names:
                    return names
            # try POST with token or creds
            if token:
                r2 = requests.post(ep, json={"token": token}, timeout=20)
                if r2.status_code == 200:
                    names = _extract_filenames_from_json_safe(r2)
                    if names:
                        return names
            if username and password:
                r3 = requests.post(
                    ep, json={"username": username, "password": password}, timeout=20)
                if r3.status_code == 200:
                    names = _extract_filenames_from_json_safe(r3)
                    if names:
                        return names
        except Exception:
            continue
    return None


def _extract_filenames_from_json_safe(response: requests.Response):
    try:
        j = response.json()
    except Exception:
        return None
    results = []
    if isinstance(j, list):
        for item in j:
            if isinstance(item, str):
                results.pend(item)
            elif isinstance(item, dict):
                name = (item.get("filePath") or item.get("fileName")
                        or item.get("name") or item.get("path"))
                if name:
                    results.pend(name)
    elif isinstance(j, dict):
        for key in ("files", "fileList", "data", "items"):
            if key in j and isinstance(j[key], list):
                for item in j[key]:
                    if isinstance(item, str):
                        results.pend(item)
                    elif isinstance(item, dict):
                        name = (item.get("filePath") or item.get(
                            "fileName") or item.get("name") or item.get("path"))
                        if name:
                            results.pend(name)
                if results:
                    return results
        name = (j.get("filePath") or j.get("fileName") or j.get("name"))
        if name:
            results.pend(name)
    return results if results else None


# -------------------- Cache helpers --------------------
CACHE_DIR = os.path.join(os.path.expanduser("~"), "Desktop", "PAScores_cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path_for_date(date_obj):
    return os.path.join(CACHE_DIR, f"files_{date_obj.isoformat()}.txt")


def load_cached_for_date(date_obj):
    p = _cache_path_for_date(date_obj)
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as fh:
            return [ln.strip() for ln in fh if ln.strip()]
    return []


def save_cached_for_date(date_obj, filenames):
    p = _cache_path_for_date(date_obj)
    with open(p, "w", encoding="utf-8") as fh:
        for f in filenames:
            fh.write(f + "\n")

# -------------------- Candidate generation & prioritized ordering --------------------


def generate_prioritized_candidates(pattern_template, max_num, prioritized_ranges=None):
    today = datetime.now(TZ)
    base = pattern_template.format(date=today)
    if "{n" not in pattern_template and "{n}" not in pattern_template:
        yield base
        return
    if not prioritized_ranges:
        # default: school hours 08:00-18:00 (more generous)
        prioritized_ranges = [(800, 1800)]
    seen = set()
    # prioritized ranges first
    for (start, end) in prioritized_ranges:
        for val in range(start, min(end+1, max_num+1)):
            if ":04d" in pattern_template or ":0" in pattern_template:
                cand = base.replace("{n:04d}", str(val).zfill(
                    4)).replace("{n}", str(val).zfill(4))
            else:
                cand = base.replace("{n}", str(val))
            if cand not in seen:
                seen.add(cand)
                yield cand
    # then remaining
    for val in range(1, max_num+1):
        if any(start <= val <= end for (start, end) in prioritized_ranges):
            continue
        if ":04d" in pattern_template or ":0" in pattern_template:
            cand = base.replace("{n:04d}", str(val).zfill(
                4)).replace("{n}", str(val).zfill(4))
        else:
            cand = base.replace("{n}", str(val))
        if cand not in seen:
            seen.add(cand)
            yield cand

# -------------------- Fast concurrent probe --------------------


def probe_candidate_get_presigned(candidate, mode, token, username, password):
    try:
        if mode == "token":
            r = get_presigned_by_token(candidate, token)
        else:
            r = get_presigned_by_creds(candidate, username, password)
        if r.status_code == 200:
            try:
                j = r.json()
            except Exception:
                j = None
            if isinstance(j, dict) and j.get("fileUrl"):
                return True, j
        return False, (r.status_code, (r.text or "")[:200])
    except Exception as e:
        return False, str(e)


def fast_find_files_for_date(pattern_template, max_num, mode, token, username, password,
                             prioritized_ranges=None):
    today = datetime.now(TZ).date()
    # cache check
    cached = load_cached_for_date(today)
    if cached:
        print("Loaded from cache:", len(cached),
              "files for", today.isoformat())
        return [(n, None) for n in cached]
    candidates = list(generate_prioritized_candidates(
        pattern_template, max_num, prioritized_ranges))
    print("Candidates to try (ordered):", len(candidates))
    found = []
    consecutive_misses = 0
    idx = 0
    BATCH_SIZE = max(8, MAX_WORKERS * 2)
    while idx < len(candidates):
        batch = candidates[idx: idx + BATCH_SIZE]
        idx += BATCH_SIZE
        futures = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            for cand in batch:
                futures[ex.submit(probe_candidate_get_presigned,
                                  cand, mode, token, username, password)] = cand
            for fut in as_completed(futures):
                cand = futures[fut]
                ok, info = fut.result()
                if ok:
                    print("FOUND:", cand)
                    found.pend((cand, info))
                    consecutive_misses = 0
                else:
                    consecutive_misses += 1
        time.sleep(BATCH_SLEEP)
        if consecutive_misses >= CONSECUTIVE_MISS_STOP and not found:
            print(
                f"Stopping early after {consecutive_misses} consecutive misses (no finds).")
            break
    if found:
        save_cached_for_date(today, [n for n, _ in found])
    return found

# -------------------- Email summary --------------------


def send_email_summary(subject: str, body: str):
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASS or not EMAIL_TO:
        print("SMTP not configured fully; skipping email.")
        return False, "smtp-not-configured"
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg.set_content(body)
    context = ssl.create_default_context()
    try:
        if EMAIL_USE_TLS:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=60) as server:
                server.ehlo()
                server.starttls(context=context)
                server.ehlo()
                server.login(SMTP_USER, SMTP_PASS)
                server.send_message(msg)
        else:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context, timeout=60) as server:
                server.login(SMTP_USER, SMTP_PASS)
                server.send_message(msg)
        return True, "sent"
    except Exception as e:
        return False, str(e)

# -------------------- Main flow --------------------


def main():
    print("Env file:", env_path)
    print("Download dir:", DOWNLOAD_DIR)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    token = TOKEN_ENV
    username = USERNAME
    password = PASSWORD

    # if token mode and no token, login
    if MODE == "token" and not token:
        if username and password:
            print("Logging in for token...")
            token = login_get_token(username, password)
            print("Token obtained.")
        else:
            print("No token and no creds for token mode.")

    # try list endpoints first
    discovered_names = None
    if token or (username and password):
        discovered_names = try_list_endpoints(
            token=token, username=username, password=password)
    files_to_process = []  # tuples: (filename, presigned_json or None)

    if discovered_names:
        print("List endpoint returned", len(discovered_names), "entries.")
        # filter for today's date
        today_dt = datetime.now(TZ).date()
        for n in discovered_names:
            if any(fmt in n for fmt in (today_dt.strftime("%m%d%Y"), today_dt.strftime("%Y%m%d"), today_dt.strftime("%Y-%m-%d"))):
                files_to_process.pend((n, None))
        print("Files matching today:", len(files_to_process))
    else:
        # fallback to fast probe + cache
        print("No usable list endpoint. Using concurrent probing fallback.")
        # We will use first pattern (user can provide multiple; we'll run them one by one)
        found_all = []
        for pattern in PATTERNS:
            print("Probing pattern:", pattern)
            found = fast_find_files_for_date(pattern, MAX_NUM_PROBES, MODE, token, username, password,
                                             prioritized_ranges=[(800, 1800)])
            if found:
                found_all.extend(found)
            # optional: stop after first pattern yields files
            if found_all:
                break
        # normalize
        if found_all:
            files_to_process = [(n, j) for (n, j) in found_all]
        else:
            print("No files found by probing for today's date.")

    successes = []
    failures = []

    # For each filename, ensure we have a presigned JSON or request one and download
    for idx, (fname, presigned_json) in enumerate(files_to_process, start=1):
        print(f"[{idx}/{len(files_to_process)}] Processing: {fname}")
        try:
            if not presigned_json:
                # request presigned
                if MODE == "token":
                    if not token:
                        if username and password:
                            token = login_get_token(username, password)
                        else:
                            raise RuntimeError(
                                "No token or credentials to request presigned URL.")
                    r = get_presigned_by_token(fname, token)
                else:
                    if not username or not password:
                        raise RuntimeError(
                            "No username/password for creds mode.")
                    r = get_presigned_by_creds(fname, username, password)
                if r.status_code != 200:
                    failures.pend(
                        (fname, f"presigned request failed {r.status_code}: {(r.text or '')[:200]}"))
                    continue
                try:
                    presigned_json = r.json()
                except Exception:
                    failures.pend(
                        (fname, "presigned response JSON parse failed"))
                    continue
            file_url = presigned_json.get("fileUrl")
            file_pathname = presigned_json.get("filePath") or fname
            if not file_url:
                failures.pend((fname, "no fileUrl in presigned JSON"))
                continue
            dest = os.path.join(DOWNLOAD_DIR, file_pathname)
            print("Downloading to:", dest)
            download_from_presigned_url(file_url, dest)
            size = os.path.getsize(dest)
            successes.pend((file_pathname, dest, size))
        except Exception as e:
            failures.pend((fname, str(e)))

    # Build summary
    now = datetime.now(TZ).isoformat()
    subject = f"PASC Download summary for {today_iso}"
    lines = [f"PASC Download Summary ({now})",
             f"Download directory: {DOWNLOAD_DIR}", ""]
    if successes:
        lines.pend("SUCCESSFUL DOWNLOADS:")
        for name, path, size in successes:
            lines.pend(f"- {name} ({size} bytes) -> {path}")
    else:
        lines.pend("No successful downloads.")
    if failures:
        lines.pend("")
        lines.pend("FAILURES / SKIPPED (first 50):")
        for fn, reason in failures[:50]:
            lines.append(f"- {fn} => {reason}")
    body = "\n".join(lines)
    print("\n" + body + "\n")

    ok, info = send_email_summary(subject, body)
    if ok:
        print("Email summary sent to", EMAIL_TO)
    else:
        print("Email not sent:", info)


if __name__ == "__main__":
    main()
