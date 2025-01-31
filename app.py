import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
from fuzzywuzzy import process
from concurrent.futures import ThreadPoolExecutor

# -----------------------------
# LOAD SECRETS
# -----------------------------
BASE_URL = st.secrets["TELEGRAM_BASE_URL"]
START_POST = int(st.secrets["START_POST"])
END_POST = int(st.secrets["END_POST"])
MAX_THREADS = 10  # Number of concurrent requests

# -----------------------------
# PAGE CONFIG & CUSTOM STYLE
# -----------------------------
st.set_page_config(page_title="📌 חיפוש הזדמנויות גיוס", 
                   page_icon="🔍", layout="wide")

st.markdown(
    """
    <style>
        body { direction: rtl; text-align: right; }
        .stTextInput > div > div > input { text-align: right; }
        .stDataFrame, .stTable { direction: rtl; }
        .stButton > button {
            font-size: 18px;
            font-weight: bold;
            background-color: #FF4B4B;
            color: white;
            width: 100%;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# REGEX PARSING FUNCTIONS
# -----------------------------
def parse_ad_number(text_content: str) -> str:
    """
    Extract 'מודעה מספר #XXXX' from text. If not found, return 'לא נמצא'.
    """
    match = re.search(r"מודעה\s*מספר\s*#(\d+)", text_content)
    return match.group(1) if match else "לא נמצא"


def parse_between(text: str, start_marker: str) -> str:
    """
    Extract single-line (or short multiline) fields that look like:
        "<start_marker>: VALUE"
    until either a dashed line, '⬅️', or end of string.
    Returns empty string if not found.
    Uses [\s\S] instead of '.' or DOTALL to match multiline safely in Python 3.12+.
    """
    pattern = rf"{start_marker}\s*:\s*([\s\S]*?)(?=\n-+\s|\n⬅️|$)"
    match = re.search(pattern, text)
    if match:
        return match.group(1).strip()
    return ""


def parse_section(text: str, section_title: str) -> str:
    """
    Extract multiline sections that start with:
        "⬅️ <section_title>:"
    until the next "⬅️" or dashed line or end-of-string.
    Returns empty if not found.
    """
    pattern = rf"⬅️\s*{section_title}\s*:\s*([\s\S]*?)(?=\n⬅️|\n-+\s|$)"
    match = re.search(pattern, text)
    if match:
        extracted = match.group(1).strip()
        # Remove trailing dashed lines if any
        extracted = re.sub(r"-+\s*", "", extracted).strip()
        return extracted
    return ""


def parse_roles(text: str) -> list:
    """
    Parse the list of roles from the "⬅️ דרושים:" section.
    Each role line typically starts with "** " in that section.
    Returns a list of roles (or empty list if none).
    """
    pattern = r"⬅️\s*דרושים\s*:\s*([\s\S]*?)(?=\n⬅️|\n-+\s|$)"
    match = re.search(pattern, text)
    if not match:
        return []

    roles_section = match.group(1)
    # Lines that start with '**'
    roles_list = re.findall(r"\*\*\s*(.+)", roles_section)
    return [r.strip() for r in roles_list]


def parse_job_info(post_id: int, html_content: str):
    """
    Given (post_id, raw HTML), parse out all fields.
    Returns a list of row dictionaries (one per role).
    """
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, "html.parser")
    meta_desc = soup.find("meta", {"property": "og:description"})
    if not meta_desc:
        return None

    text_content = meta_desc["content"]

    # -- Parse out fields --
    ad_number = parse_ad_number(text_content)
    sug_yehida = parse_between(text_content, "סוג יחידה")  # סוג יחידה
    area = parse_between(text_content, "אזור בארץ")        # אזור בארץ

    roles = parse_roles(text_content)  # list of roles
    qualifications = parse_section(text_content, "כישורים נדרשים")
    unit_info = parse_section(text_content, "פרטים על היחידה")
    service_terms = parse_section(text_content, "תנאי שירות")

    next_service = parse_between(text_content, "תקופת שירות הקרובה")

    # Check for immediate recruitment ("⏰ גיוס מיידי" or just '⏰')
    immediate = "כן" if "⏰" in text_content else "לא"

    # Check for "זמני או קבוע"
    # We'll do a simple check if "🔊 זמני או קבוע" in text_content
    recruitment_type = "זמני או קבוע" if "🔊 זמני או קבוע" in text_content else ""

    # If no roles, we create a single "No roles" row
    if not roles:
        roles = ["לא צוינו תפקידים"]

    results = []
    for role in roles:
        row = {
            "מספר מודעה": ad_number,
            "תפקיד": role,
            "סוג יחידה": sug_yehida,
            "אזור בארץ": area,
            "כישורים נדרשים": qualifications,
            "פרטים על היחידה": unit_info,
            "תנאי שירות": service_terms,
            "תקופת שירות קרובה": next_service,
            "גיוס מיידי": immediate,
            "סוג גיוס": recruitment_type,
            "קישור": f"{BASE_URL}{post_id}"
        }
        results.append(row)

    return results

# -----------------------------
# SCRAPING WITH MULTITHREADING
# -----------------------------
def download_html(post_id: int):
    """
    Downloads HTML for a given post ID.
    Returns (post_id, html_content) or (post_id, None).
    """
    url = f"{BASE_URL}{post_id}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            return post_id, resp.text
    except:
        pass
    return post_id, None


@st.cache_data
def scrape_jobs_concurrent(start_id: int, end_id: int) -> pd.DataFrame:
    """
    Multithreaded scraping: download HTML for each post_id,
    then parse fields. Returns a combined DataFrame.
    """
    # Step 1: Download concurrently
    data = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        html_results = list(executor.map(download_html, range(start_id, end_id + 1)))

    # Step 2: Parse concurrently
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        parsed_results = list(executor.map(lambda x: parse_job_info(x[0], x[1]), html_results))

    # Flatten
    for result in parsed_results:
        if result:
            data.extend(result)

    df = pd.DataFrame(data)
    return df

# -----------------------------
# MAIN APP
# -----------------------------
st.title("📌 חיפוש הזדמנויות גיוס")

# 1) Scrape data (cache it)
with st.spinner("🔄 טוען מודעות..."):
    df = scrape_jobs_concurrent(START_POST, END_POST)

st.success("✅ כל המודעות נטענו בהצלחה!")

# 2) Filter / Search
st.header("סינון וחיפוש")

search_query = st.text_input("🔎 חיפוש חופשי (בכל השדות):", "")

filtered_df = df.copy()
if search_query.strip():
    # We'll do a simple substring match across row values
    mask = filtered_df.apply(
        lambda row: search_query.lower() in " ".join(str(v).lower() 
                                                     for v in row.values),
        axis=1
    )
    filtered_df = filtered_df[mask]

# Filter 1: אזור בארץ
all_areas = ["(הכל)"] + sorted(set(filtered_df["אזור בארץ"].dropna()))
selected_area = st.selectbox("סינון לפי אזור בארץ:", all_areas, index=0)
if selected_area != "(הכל)":
    filtered_df = filtered_df[filtered_df["אזור בארץ"] == selected_area]

# Filter 2: סוג יחידה
all_units = ["(הכל)"] + sorted(set(filtered_df["סוג יחידה"].dropna()))
selected_unit = st.selectbox("סינון לפי סוג יחידה:", all_units, index=0)
if selected_unit != "(הכל)":
    filtered_df = filtered_df[filtered_df["סוג יחידה"] == selected_unit]

# Filter 3: גיוס מיידי
immediate_opts = ["(הכל)", "כן", "לא"]
selected_immediate = st.selectbox("סינון לפי גיוס מיידי:", immediate_opts, index=0)
if selected_immediate != "(הכל)":
    filtered_df = filtered_df[filtered_df["גיוס מיידי"] == selected_immediate]

st.write(f"נמצאו {len(filtered_df)} תוצאות:")

# Display final table
st.dataframe(filtered_df)

# (Optionally show each row with expanders)
# for idx, row in filtered_df.iterrows():
#     with st.expander(f"📌 {row['תפקיד']} (מודעה #{row['מספר מודעה']})"):
#         for col in ["תפקיד", "סוג יחידה", "אזור בארץ", "כישורים נדרשים",
#                     "פרטים על היחידה", "תנאי שירות", "תקופת שירות קרובה",
#                     "גיוס מיידי", "סוג גיוס", "קישור"]:
#             st.write(f"**{col}**: {row[col]}")
