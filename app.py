import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from fuzzywuzzy import fuzz

# -----------------------------
# LOAD SECRETS
# -----------------------------
BASE_URL = st.secrets["TELEGRAM_BASE_URL"]
START_POST = int(st.secrets["START_POST"])
END_POST = int(st.secrets["END_POST"])
MAX_THREADS = 10  # concurrency level

# -----------------------------
# PAGE CONFIG & STYLING
# -----------------------------
st.set_page_config(page_title="📌 חיפוש הזדמנויות גיוס", page_icon="🔍", layout="wide")

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
# ESCAPE THE '⬅️' CHARACTER
# -----------------------------
arrow_escaped = re.escape("⬅️")

# -----------------------------
# NORMALIZE HEBREW (for fuzzy search)
# -----------------------------
def normalize_hebrew(text: str) -> str:
    """
    1) NFKC normalization
    2) Remove quotes (״, ", ')
    """
    text = unicodedata.normalize('NFKC', text)
    for ch in ['״', '"', "'"]:
        text = text.replace(ch, "")
    return text

# -----------------------------
# PARSE SERVICE PERIOD MONTHS
# -----------------------------
def parse_service_period(text: str) -> (str, str):
    """
    If text looks like "מרץ - אפריל", return ("מרץ", "אפריל").
    Otherwise ("", "").
    """
    text = text.strip()
    pattern = r"^\s*(\S+)\s*-\s*(\S+)\s*$"
    match = re.search(pattern, text)
    if match:
        return match.group(1), match.group(2)
    return "", ""

# -----------------------------
# REGEX PARSING
# -----------------------------
def parse_ad_number(text: str) -> str:
    match = re.search(r"מודעה\s*מספר\s*#(\d+)", text)
    return match.group(1) if match else "לא נמצא"

def parse_between(text: str, start_marker: str) -> str:
    pattern = rf"{start_marker}\s*:\s*([\s\S]*?)(?=\n-+\s|\n{arrow_escaped}|$)"
    m = re.search(pattern, text)
    return m.group(1).strip() if m else ""

def parse_section(text: str, section_title: str) -> str:
    pattern = rf"{arrow_escaped}\s*{section_title}\s*:\s*([\s\S]*?)(?=\n{arrow_escaped}|\n-+\s|$)"
    m = re.search(pattern, text)
    if not m:
        return ""
    extracted = re.sub(r"-+\s*", "", m.group(1)).strip()
    return extracted

def parse_roles(text: str) -> list:
    pattern = rf"{arrow_escaped}\s*דרושים\s*:\s*([\s\S]*?)(?=\n{arrow_escaped}|\n-+\s|$)"
    m = re.search(pattern, text)
    if not m:
        return []
    roles_section = m.group(1)
    roles_list = re.findall(r"\*\*\s*(.+)", roles_section)
    return [r.strip() for r in roles_list]

# -----------------------------
# parse_job_info
# -----------------------------
def parse_job_info(post_id: int, html_content: str):
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, "html.parser")
    meta_desc = soup.find("meta", {"property": "og:description"})
    if not meta_desc:
        return None

    text_content = meta_desc["content"]
    ad_number = parse_ad_number(text_content)
    if ad_number == "לא נמצא":
        # skip if no ad number
        return None

    sug_yehida = parse_between(text_content, "סוג יחידה")
    area = parse_between(text_content, "אזור בארץ")
    roles = parse_roles(text_content)
    qualifications = parse_section(text_content, "כישורים נדרשים")
    unit_info = parse_section(text_content, "פרטים על היחידה")
    service_terms = parse_section(text_content, "תנאי שירות")

    # parse service period
    service_period_raw = parse_between(text_content, "תקופת שירות הקרובה")
    month_start, month_end = parse_service_period(service_period_raw)

    immediate = "כן" if "⏰" in text_content else "לא"
    recruitment_type = "זמני או קבוע" if "🔊 זמני או קבוע" in text_content else ""

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
            "תקופת שירות (Raw)": service_period_raw,
            "חודש התחלה": month_start,
            "חודש סיום": month_end,
            "גיוס מיידי": immediate,
            "סוג גיוס": recruitment_type,
            "קישור": f"{BASE_URL}{post_id}"
        }
        results.append(row)
    return results

# -----------------------------
# DOWNLOAD HTML
# -----------------------------
def download_html(post_id: int):
    url = f"{BASE_URL}{post_id}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            return post_id, resp.text
    except:
        pass
    return post_id, None

# -----------------------------
# SCRAPE (Multithread)
# -----------------------------
@st.cache_data
def scrape_jobs_concurrent(start_id: int, end_id: int) -> pd.DataFrame:
    data = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        html_results = list(executor.map(download_html, range(start_id, end_id+1)))

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        parsed_lists = list(executor.map(lambda x: parse_job_info(x[0], x[1]), html_results))

    for plist in parsed_lists:
        if plist:
            data.extend(plist)

    return pd.DataFrame(data)

# -----------------------------
# FUZZY MATCH HELPER
# -----------------------------
def fuzzy_score_row(row, query):
    row_text = " ".join(str(v) for v in row.values)
    row_text = normalize_hebrew(row_text)
    query = normalize_hebrew(query)
    return fuzz.partial_ratio(query.lower(), row_text.lower())

# -----------------------------
# MAIN APP
# -----------------------------
st.title("📌 חיפוש הזדמנויות גיוס")

with st.spinner("🔄 טוען מודעות..."):
    df = scrape_jobs_concurrent(START_POST, END_POST)

st.success("✅ כל המודעות נטענו בהצלחה!")

st.header("סינון וחיפוש")

search_query = st.text_input("🔎 חיפוש חופשי (בכל השדות):", "")

# existing filters
all_areas = ["(הכל)"] + sorted(set(df["אזור בארץ"].dropna()))
selected_area = st.selectbox("סינון לפי אזור בארץ:", all_areas, index=0)

all_units = ["(הכל)"] + sorted(set(df["סוג יחידה"].dropna()))
selected_unit = st.selectbox("סינון לפי סוג יחידה:", all_units, index=0)

# new month filters
all_start_months = ["(הכל)"] + sorted(set(df["חודש התחלה"].dropna()))
selected_month_start = st.selectbox("סינון לפי חודש התחלה:", all_start_months, index=0)

all_end_months = ["(הכל)"] + sorted(set(df["חודש סיום"].dropna()))
selected_month_end = st.selectbox("סינון לפי חודש סיום:", all_end_months, index=0)

# check if user applied any filter or typed search
filters_used = (
    search_query.strip() != "" or
    selected_area != "(הכל)" or
    selected_unit != "(הכל)" or
    selected_month_start != "(הכל)" or
    selected_month_end != "(הכל)"
)

if not filters_used:
    st.info("אנא הזן חיפוש או הגדר סינון כדי לראות תוצאות.")
    st.stop()

# -----------------------------
# Apply filters
# -----------------------------
filtered_df = df.copy()

# 1) Fuzzy search
if search_query.strip():
    threshold = 70
    scores = filtered_df.apply(lambda r: fuzzy_score_row(r, search_query), axis=1)
    filtered_df = filtered_df[scores >= threshold]

# 2) Dropdown filters
if selected_area != "(הכל)":
    filtered_df = filtered_df[filtered_df["אזור בארץ"] == selected_area]

if selected_unit != "(הכל)":
    filtered_df = filtered_df[filtered_df["סוג יחידה"] == selected_unit]

if selected_month_start != "(הכל)":
    filtered_df = filtered_df[filtered_df["חודש התחלה"] == selected_month_start]

if selected_month_end != "(הכל)":
    filtered_df = filtered_df[filtered_df["חודש סיום"] == selected_month_end]

# -----------------------------
# Show results
# -----------------------------
st.write(f"נמצאו {len(filtered_df)} תוצאות:")

if len(filtered_df) == 0:
    st.warning("לא נמצאו תפקידים במערכת התואמים לחיפוש / סינון שלך.")
else:
    for idx, row in filtered_df.iterrows():
        ad_number = row["מספר מודעה"]
        role = row["תפקיד"]
        link = row["קישור"]
        st.markdown(f"- **{role}** (מודעה #{ad_number}): [קישור לפרטים]({link})")
