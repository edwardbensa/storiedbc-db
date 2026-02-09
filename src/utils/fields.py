"""Derived fields utility functions"""

# Imports
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse
from loguru import logger
from .files import generate_image_filename
from .parsers import to_datetime
from .lookups import find_doc


def generate_image_url(doc: dict, url_str: str, img_type: str,
                       container_name: str, account_name) -> str:
    """
    Generates the Azure Blob Storage URL for a given document's image.
    """
    if img_type not in ["user", "club", "cover", "creator"]:
        raise ValueError("Type must be either 'user', 'club', or 'cover'")

    try:
        if not url_str or not isinstance(url_str, str) or not url_str.strip():
            return ""
        parsed_url = urlparse(url_str)
        extension = os.path.splitext(parsed_url.path)[1] or ".jpg"

        blob_name = f"{generate_image_filename(doc, img_type)}{extension}"
        url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}"
        return url
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to generate image URL: {e}")
        return ""


def generate_rlog(doc: dict):
    """Replaces rstatus_history with reading_log."""
    current_rstatus = doc["rstatus_id"]
    if current_rstatus == "rs4":
        doc["reading_log"] = ""
        doc.pop("rstatus_history")
        return doc

    # Find rstatus_history and lh_rstatus
    rstatus_history = doc["rstatus_history"]
    lh_rstatus = ""
    if rstatus_history != "":
        lh_rstatus = str(rstatus_history).rsplit(',', maxsplit=1)[-1].split(":")[0].strip()

    # Amend current_rstatus if current is "Reading" and last historical is "Paused"
    if current_rstatus == "rs2" and lh_rstatus == "rs3":
        current_rstatus = lh_rstatus
        doc["rstatus_id"] = current_rstatus

    # Set rstatus_history to now if blank and current_rstatus is "Paused"
    if current_rstatus == "rs3" and rstatus_history == "":
        rstatus_history = f"rs3: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"

    # Set start and end entries
    start = f"rs2: {doc["date_started"]}" if doc["date_started"] != "" else ""
    end = f"rs1: {doc["date_completed"]}" if doc["date_completed"] != "" else ""

    # Set start to 7 days before now if blank and current_rstatus is "Paused"
    if start == "" and current_rstatus == "rs3":
        start_date = datetime.now() - timedelta(days=7)
        start = f"rs2: {start_date.strftime("%Y-%m-%d %H:%M:%S")}"

    # Set start to 21 days before end if blank and current_rstatus is "Read"/"Paused"/"DNF"
    if start == "" and current_rstatus in ("rs1", "rs3", "rs5"):
        completed_date = to_datetime(doc["date_completed"])
        if completed_date is not None:
            start_date = completed_date - timedelta(days=21)
            start = f"rs2: {start_date.strftime("%Y-%m-%d %H:%M:%S")}"

    # Set default start and end if both blank and current_rstatus is "Read"/"Reading"/"DNF"
    if start + end == "" and current_rstatus in ("rs1", "rs2", "rs5"):
        start_date = to_datetime("2025-10-10 10:10:10")
        end_date = to_datetime("2025-10-31 20:31:31")
        if start_date is not None:
            start = f"rs2: {start_date.strftime("%Y-%m-%d %H:%M:%S")}"
        if current_rstatus != "rs2" and end_date is not None:
            end = f"{current_rstatus}: {end_date.strftime("%Y-%m-%d %H:%M:%S")}"

    # Create reading log
    r_log = f"{start}, {rstatus_history}" if rstatus_history != "" else start
    r_log = f"{r_log}, {end}" if end != "" else r_log

    return r_log


def compute_d2r(doc):
    """Calculate days to read and add to user_reads."""

    # Skip entries that don't have a reading_log
    r_log = doc.get("reading_log")
    if r_log == "":
        return ""

    # Split into tokens
    tokens = [t.strip() for t in r_log.split(",")]

    # Change last token to "Read" if the rstatus is "Paused"/"DNF"
    last_token = tokens[-1]
    last_status, last_ts = last_token.split(":", 1)
    last_token = f"rs1: {last_ts}" if last_status in ("rs3", "rs5") else last_token

    # Add "Read" as last token if last_status is "Reading"
    if last_status == "rs2":
        new_token = f"rs1: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}"
        tokens.append(new_token)

    events = []
    for t in tokens:
        key, value = t.split(":", 1)
        events.append((key.strip(), value.strip()))

    # Collect reading intervals
    intervals = []
    start_time = None

    for key, value in events:
        if key == "rs2":  # start reading
            start_time = to_datetime(value)
        elif key in ("rs3", "rs1"):  # paused or finished
            if start_time:
                end_time = to_datetime(value)
                if end_time:
                    intervals.append(end_time - start_time)
                start_time = None

    d2r = sum((i.total_seconds() / 86400 for i in intervals))
    d2r = 1 if d2r == 0 else d2r

    return d2r

def compute_rr(doc, book_versions):
    """Calculate reading rates and add to user_reads."""

    # Skip entries that don't have d2r
    d2r = doc.get("days_to_read", None)
    if d2r is None:
        return ""

    version_id = doc.get("version_id")
    bv_doc = find_doc(book_versions, "version_id", version_id)

    # Skip entries that don't have format
    fmt = bv_doc.get("format")
    if fmt == "":
        return ""

    if fmt == "audiobook":
        book_length = bv_doc.get("length")
    else:
        book_length = bv_doc.get("page_count")

    if book_length == "" or book_length is None:
        return ""

    read_rate = book_length / d2r

    # Adjust read rate in cases of small d2r
    r_log = doc.get("reading_log")
    l_rstatus = str(r_log).rsplit(',', maxsplit=1)[-1].split(":")[0].strip()
    if l_rstatus == "rs3" and d2r <= 5:
        read_rate = book_length / 10

    return read_rate
