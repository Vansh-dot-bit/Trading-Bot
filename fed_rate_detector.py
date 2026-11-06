import json

import logging

import re

import smtplib

from datetime import datetime, timezone

from email.message import EmailMessage

from pathlib import Path

from urllib.parse import urljoin, urlparse



import requests

from bs4 import BeautifulSoup





# ============================================================

# CONFIGURATION

# ============================================================



BASE_URL = "https://www.federalreserve.gov"

FOMC_YEAR_PAGE = BASE_URL + "/newsevents/pressreleases/{year}-press-fomc.htm"



# These files are stored beside this script.

STATE_FILE = Path(__file__).with_name("fed_rate_state.json")

HISTORY_FILE = Path(__file__).with_name("fed_rate_history.json")

LOG_FILE = Path(__file__).with_name("fed_rate_detector.log")



REQUEST_TIMEOUT = 30



# Keep False while testing. No Gmail credentials are required.

ENABLE_EMAIL = False

GMAIL_SENDER = ""

GMAIL_APP_PASSWORD = ""

GMAIL_RECIPIENT = ""



HEADERS = {

    "User-Agent": (

        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "

        "AppleWebKit/537.36 (KHTML, like Gecko) "

        "Chrome/130.0.0.0 Safari/537.36"

    )

}





# ============================================================

# LOGGING AND HTTP SESSION

# ============================================================



logging.basicConfig(

    level=logging.INFO,

    format="%(asctime)s | %(levelname)s | %(message)s",

    handlers=[

        logging.StreamHandler(),

        logging.FileHandler(LOG_FILE, encoding="utf-8"),

    ],

)



SESSION = requests.Session()

SESSION.headers.update(HEADERS)

# Injected by bot.py when running as part of the integrated system.
SHARED_NOTIFIER = None
API_GUARD = None

def _request_get(url, **kwargs):
    if API_GUARD is not None:
        return API_GUARD.get(SESSION, url, service="Fed rate detector", **kwargs)
    return SESSION.get(url, **kwargs)






# ============================================================

# STATE MANAGEMENT

# ============================================================



def load_state():

    """Load the last processed FOMC statement, if one exists."""

    if not STATE_FILE.exists():

        return None



    try:

        with STATE_FILE.open("r", encoding="utf-8") as file:

            state = json.load(file)



        if not isinstance(state, dict):

            raise ValueError("State file must contain a JSON object.")



        required = {"statement_date", "statement_url", "target_lower", "target_upper"}

        if not required.issubset(state):

            raise ValueError("State file is missing required fields.")



        return state

    except (json.JSONDecodeError, OSError, ValueError) as error:

        logging.error("Cannot read state file: %s", error)

        raise RuntimeError(

            "State file is invalid. Back it up and inspect it before continuing; "

            "refusing to overwrite it automatically."

        ) from error





def save_state(decision):

    """Save the successfully processed statement atomically."""

    temporary_file = STATE_FILE.with_suffix(".tmp")

    with temporary_file.open("w", encoding="utf-8") as file:

        json.dump(decision, file, indent=4)

        file.flush()

    temporary_file.replace(STATE_FILE)





def load_history():

    """Load the historical record of unique FOMC statements."""

    if not HISTORY_FILE.exists():

        return []



    try:

        with HISTORY_FILE.open("r", encoding="utf-8") as file:

            history = json.load(file)

        if not isinstance(history, list) or any(not isinstance(item, dict) for item in history):

            raise ValueError("History file must contain a JSON list of objects.")

        return history

    except (json.JSONDecodeError, OSError, ValueError) as error:

        logging.error("Cannot read history file: %s", error)

        raise RuntimeError(

            "History file is invalid. Back it up and inspect it before continuing; "

            "refusing to overwrite it automatically."

        ) from error





def record_history(decision, comparison_result, previous):

    """Record each statement once, including the range it was compared against."""

    history = load_history()

    statement_url = decision["statement_url"]



    if any(item.get("statement_url") == statement_url for item in history):

        logging.info("Historical record already exists for this statement.")

        return



    record = dict(decision)

    record["comparison_result"] = comparison_result

    record["checked_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if previous is not None:

        record["previous_target_lower"] = float(previous["target_lower"])

        record["previous_target_upper"] = float(previous["target_upper"])

    else:

        record["previous_target_lower"] = None

        record["previous_target_upper"] = None



    history.append(record)

    temporary_file = HISTORY_FILE.with_suffix(".tmp")

    with temporary_file.open("w", encoding="utf-8") as file:

        json.dump(history, file, indent=4)

        file.flush()

    temporary_file.replace(HISTORY_FILE)

    logging.info("Historical record saved (%s total statement(s)).", len(history))





# ============================================================

# RATE PARSING

# ============================================================



def fraction_to_float(value):

    """Convert values such as 3-3/4, 3/4, 4, or 3.75 to floats."""

    value = value.strip()



    if "-" in value and "/" in value:

        whole, fraction = value.split("-", 1)

        numerator, denominator = fraction.split("/", 1)

        return float(whole) + float(numerator) / float(denominator)



    if "/" in value:

        numerator, denominator = value.split("/", 1)

        return float(numerator) / float(denominator)



    return float(value)





def extract_rate_decision(statement_url, statement_date):

    """Extract the target range and decision sentence from an official statement."""

    response = _request_get(statement_url, timeout=REQUEST_TIMEOUT)

    response.raise_for_status()



    soup = BeautifulSoup(response.text, "html.parser")

    page_text = " ".join(soup.get_text(" ", strip=True).split())



    number = r"\d+(?:-\d+/\d+|/\d+|\.\d+)?"

    pattern = re.compile(

        r"The Committee decided to\s+"

        r"(raise|lower|maintain|keep)"

        r".{0,250}?target range for the federal funds rate"

        r".{0,250}?\bto\s+"

        rf"({number})\s+to\s+({number})\s+percent\b",

        re.IGNORECASE,

    )



    match = pattern.search(page_text)

    if not match:

        logging.error("Could not extract a target range from %s", statement_url)

        search_position = page_text.lower().find(

            "target range for the federal funds rate"

        )

        if search_position >= 0:

            logging.error(

                "Source context: %s",

                page_text[max(0, search_position - 250):search_position + 600],

            )

        raise ValueError(

            "Target-rate decision not recognized. No state was changed."

        )



    action = match.group(1).lower()

    lower = fraction_to_float(match.group(2))

    upper = fraction_to_float(match.group(3))



    if lower >= upper:

        raise ValueError("Extracted target range is invalid.")



    direction_map = {

        "raise": "RAISED",

        "lower": "LOWERED",

        "maintain": "MAINTAINED",

        "keep": "MAINTAINED",

    }



    # Start at the actual policy-decision phrase, not a preceding page element

    # such as the time, social-share links, or page navigation.

    decision_start = page_text.lower().rfind(

        "the committee decided to", 0, match.start() + 1

    )

    if decision_start < 0:

        decision_start = match.start()



    sentence_end = page_text.find(".", match.end())

    if sentence_end < 0:

        sentence_end = min(len(page_text), match.end() + 250)

    else:

        sentence_end += 1



    decision_sentence = page_text[decision_start:sentence_end].strip()



    return {

        "statement_date": statement_date,

        "statement_url": statement_url,

        "target_lower": round(lower, 4),

        "target_upper": round(upper, 4),

        "direction": direction_map[action],

        "decision_sentence": decision_sentence,

    }





# ============================================================

# FIND LATEST OFFICIAL FOMC STATEMENT — YEAR-INDEPENDENT

# ============================================================



def _statement_from_link(link):

    """Return normalized statement metadata for a dated official Fed link."""

    href = link.get("href", "").strip()

    if not href:

        return None



    absolute_url = urljoin(BASE_URL + "/", href)

    parsed = urlparse(absolute_url)

    if parsed.netloc.lower() != "www.federalreserve.gov":

        return None



    match = re.search(r"/monetary(\d{8})a\.htm$", parsed.path, re.IGNORECASE)

    if not match:

        return None



    date_string = match.group(1)

    try:

        statement_date = datetime.strptime(date_string, "%Y%m%d").date()

    except ValueError:

        return None



    return {

        "date": statement_date.isoformat(),

        "url": absolute_url,

        "date_key": date_string,

    }





def statements_on_year_page(year):

    """Return dated FOMC statement links from the current year's official page."""

    index_url = FOMC_YEAR_PAGE.format(year=year)

    response = _request_get(index_url, timeout=REQUEST_TIMEOUT)



    if response.status_code == 404:

        raise RuntimeError(

            f"The official FOMC index for {year} returned HTTP 404. "

            "No older years will be fetched; no state was changed."

        )



    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    statements = {}



    for link in soup.find_all("a", href=True):

        title = " ".join(link.get_text(" ", strip=True).split())

        if title.casefold() != "federal reserve issues fomc statement":

            continue



        item = _statement_from_link(link)

        if item and item["date"].startswith(f"{year}-"):

            statements[item["date_key"]] = item



    if not statements:

        raise RuntimeError(

            f"No dated FOMC statement links were recognized on the official "

            f"{year} page. No older years will be fetched and no state was changed."

        )



    return list(statements.values())





def find_latest_statement():

    """Find the newest statement on the current year's official FOMC index only."""

    current_year = datetime.now(timezone.utc).year

    logging.info("Checking official FOMC index for current year %s only", current_year)



    try:

        statements = statements_on_year_page(current_year)

    except requests.RequestException as error:

        raise RuntimeError(

            f"Could not check the official FOMC index for {current_year}; "

            "no older years will be fetched and no state was changed."

        ) from error



    latest = max(statements, key=lambda item: item["date_key"])

    logging.info("Found %s statement(s) on the current-year page.", len(statements))

    return latest





# ============================================================

# OPTIONAL GMAIL NOTIFICATION

# ============================================================



def send_email(subject, body):
    """Use bot.py's Gmail transport when integrated; retain standalone email support."""
    if SHARED_NOTIFIER is not None:
        sent = SHARED_NOTIFIER.send_custom_alert(subject, body)
        if not sent:
            raise RuntimeError("Shared Gmail notifier is disabled; Fed alert was not dispatched.")
        logging.info("Fed alert dispatched through the bot Gmail notifier.")
        return True
    if not ENABLE_EMAIL:
        logging.info("Email disabled; no email sent.")
        return False
    if not all([GMAIL_SENDER, GMAIL_APP_PASSWORD, GMAIL_RECIPIENT]):
        raise RuntimeError("Email enabled, but Gmail configuration is incomplete.")
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = GMAIL_SENDER
    message["To"] = GMAIL_RECIPIENT
    message.set_content(body)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
        server.login(GMAIL_SENDER, GMAIL_APP_PASSWORD)
        server.send_message(message)
    logging.info("Gmail notification sent.")
    return True


# RATE CHANGE DETECTOR

# ============================================================



def compare_decisions(previous, current):

    """Compare old and new target ranges and identify the change."""

    if previous is None:

        return "FIRST_RUN"



    if (

        previous.get("statement_date") == current["statement_date"]

        and previous.get("statement_url") == current["statement_url"]

    ):

        return "DUPLICATE"



    old_lower = float(previous["target_lower"])

    old_upper = float(previous["target_upper"])

    new_lower = float(current["target_lower"])

    new_upper = float(current["target_upper"])



    if new_lower > old_lower and new_upper > old_upper:

        return "RATE_RAISED"

    if new_lower < old_lower and new_upper < old_upper:

        return "RATE_LOWERED"

    if new_lower == old_lower and new_upper == old_upper:

        return "NEW_STATEMENT_UNCHANGED"



    return "RATE_RANGE_CHANGED"





# ============================================================

# MAIN

# ============================================================



def check_once():

    logging.info("=" * 55)

    logging.info("FEDERAL RESERVE RATE DETECTOR")

    logging.info(

        "UTC check time: %s",

        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),

    )



    previous = load_state()

    latest = find_latest_statement()



    logging.info("Latest statement date: %s", latest["date"])

    logging.info("Official URL: %s", latest["url"])



    # Only parse the newest statement. If parsing fails, do not use an older one.

    current = extract_rate_decision(latest["url"], latest["date"])



    logging.info(

        "Current target range: %.2f%% - %.2f%%",

        current["target_lower"],

        current["target_upper"],

    )

    logging.info("Official decision: %s", current["direction"])

    logging.info("Decision text: %s", current["decision_sentence"])



    result = compare_decisions(previous, current)

    logging.info("Comparison result: %s", result)



    if result == "FIRST_RUN":

        save_state(current)

        record_history(current, result, previous)

        logging.info("Initial baseline saved. No alert sent.")

        return



    if result == "DUPLICATE":

        # This also creates a baseline history entry if upgrading from a version

        # that saved only fed_rate_state.json and had no history file.

        record_history(current, result, previous)

        logging.info("This statement has already been processed; latest source was checked again.")

        return



    if result == "NEW_STATEMENT_UNCHANGED":

        logging.info("New statement, but target range is unchanged.")

        save_state(current)

        record_history(current, result, previous)

        return



    if result == "RATE_RAISED":

        subject = "Fed Rate Alert: Target Range Increased"

        market_view = "Potentially bearish for crypto; not a guaranteed price direction."

    elif result == "RATE_LOWERED":

        subject = "Fed Rate Alert: Target Range Decreased"

        market_view = "Potentially bullish for crypto; not a guaranteed price direction."

    else:

        subject = "Fed Rate Alert: Target Range Changed"

        market_view = "The target range changed in a way that needs manual review."



    old_range = (

        f"{float(previous['target_lower']):.2f}% - "

        f"{float(previous['target_upper']):.2f}%"

    )

    new_range = (

        f"{current['target_lower']:.2f}% - "

        f"{current['target_upper']:.2f}%"

    )



    body = (

        "Federal Reserve rate decision detected.\n\n"

        f"Previous target range: {old_range}\n"

        f"New target range: {new_range}\n"

        f"Decision: {current['direction']}\n"

        f"Statement date: {current['statement_date']}\n\n"

        f"Official statement:\n{current['decision_sentence']}\n\n"

        f"Market interpretation: {market_view}\n\n"

        f"Official source: {current['statement_url']}\n\n"

        "This is an informational alert, not a trade instruction."

    )



    # Notify first; only save after a successful notification when email is enabled.

    send_email(subject, body)

    save_state(current)

    record_history(current, result, previous)

    logging.info("Decision successfully processed.")

    logging.info("=" * 55)





def ask_runtime_config():
    """Ask for Fed detector options after bot.py's own setup questions."""
    enabled = input("Enable Federal Reserve rate detector? (Y/n): ").strip().lower() != "n"
    if not enabled:
        print("[FED] Detector disabled for this run.")
        return {"enabled": False}
    raw = input("Fed detector polling interval in hours (default 6): ").strip()
    try:
        hours = float(raw) if raw else 6.0
        if not (0.1 <= hours <= 168):
            raise ValueError
    except ValueError:
        print("Invalid interval; using 6 hours.")
        hours = 6.0
    return {"enabled": True, "poll_seconds": int(hours * 3600)}


def run_service(config, notifier=None, api_guard=None, stop_event=None):
    """Independent Fed polling loop. Errors never escape and stop sibling services."""
    global SHARED_NOTIFIER, API_GUARD
    SHARED_NOTIFIER = notifier
    API_GUARD = api_guard
    interval = max(60, int(config.get("poll_seconds", 6 * 3600)))
    logging.info("Fed detector service started; polling every %s seconds.", interval)
    while stop_event is None or not stop_event.is_set():
        try:
            check_once()
        except Exception:
            logging.exception("Fed rate detector cycle failed; will retry on next cycle")
        if stop_event is not None:
            if stop_event.wait(interval):
                break
        else:
            time.sleep(interval)
    logging.info("Fed detector service stopped.")


def main():
    try:
        check_once()
    except Exception:
        logging.exception("Rate detector failed.")
        raise


if __name__ == "__main__":
    main()
