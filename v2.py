import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
from fuzzywuzzy import process
from concurrent.futures import ThreadPoolExecutor

# -----------------------------------------------------------
# LOAD SECRETS
# -----------------------------------------------------------
BASE_URL = st.secrets["TELEGRAM_BASE_URL"]
START_POST = int(st.secrets["START_POST"])
END_POST = int(st.secrets["END_POST"])
MAX_THREADS = 10  # Number of concurrent requests

# -----------------------------------------------------------
# PAGE CONFIG & CUSTOM STYLE
# -----------------------------------------------------------
st.set_page_config(
    page_title="📌 חיפוש הזדמנויות גיוס",
    page_icon="🔍",
    layout="wide"
)

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

# -----------------------------------------------------------
# HELPER: ESCAPE THE '⬅️' CHARACTER FOR REGEX
# -----------------------------------------------------------
# Some Telegram posts contain "⬅️" (arrow + variation selector).
# Python 3.12 can misinterpret it as an inline "global" flag if not escaped.
arrow_escaped = re.escape("⬅️")

# -----------------------------------------------------------
# REGEX PARSING FUNCTIONS
# -----------------------------------------------------------

def parse_ad_number(text: str) -> str:
    """
    Extract 'מודעה מספר #XXXX' from text.
    Return 'לא נמצא' if not found.
    """
    match = re.search(r"מודעה\s*מספר\s*#(\d+)", text)
    return match.group(1) if match else "לא נמצא"


def parse_between(text: str, start_marker: str) -> str:
    """
    Extract single/multi-line fields like "<start_marker>: VALUE"
    until a dashed line, arrow, or end of string.
    We use [\s\S] to match everything including newlines.
    """
    # Example: "סוג יחידה: חי\"ר" up to next "\n- - -", or "\n⬅️", or end
    pattern = rf"{start_marker}\s*:\s*([\s\S]*?)(?=\n-+\s|\n{arrow_escaped}|$)"
    match = re.search(pattern, text)
    if match:
        return match.group(1).strip()
    return ""


def parse_section(text: str, section_title: str) -> str:
    """
    Extract a multi-line section that starts with:
      "⬅️ <section_title>:"
    and continues until the next arrow or dashed line or end.
    """
    pattern = rf"{arrow_escaped}\s*{section_title}\s*:\s*([\s\S]*?)(?=\n{arrow_escaped}|\n-+\s|$)"
    match = re.search(pattern, text)
    if not match:
        return ""
    extracted = match.group(1).strip()
    # Remove trailing dashed lines if any:
    extracted = re.sub(r"-+\s*", "", extracted).strip()
    return extracted


def parse_roles(text: str) -> list:
    """
    Extract roles listed after "⬅️ דרושים:" as lines starting with "** ".
    Returns a list of role strings, or an empty list if none found.
    """
    pattern = rf"{arrow_escaped}\s*דרושים\s*:\s*([\s\S]*?)(?=\n{arrow_escaped}|\n-+\s|$)"
    match = re.search(pattern, text)
    if not match:
        return []

    roles_section = match.group(1)
    # Lines that start with "**"
    roles_list = re.findall(r"\*\*\s*(.+)", roles_section)
    return [r.strip() for r in roles_list]


def parse_job_info(post_id: int, html_content: str):
    """
    Given (post_id, raw HTML), parse all fields from the og:description.
    Returns a list of row dicts (one per role) or None if invalid.
    """
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, "html.parser")
    meta_desc = soup.find("meta", {"property": "og:description"})
    if not meta_desc:
        return None

    text_content = meta_desc["content"]

    # ----- Parse out fields -----
    ad_number = parse_ad_number(text_content)
    sug_yehida = parse_between(text_content, "סוג יחידה")
    area = parse_between(text_content, "אזור בארץ")
    roles = parse_roles(text_content)
    qualifications = parse_section(text_content, "כישורים נדרשים")
    unit_info = parse_section(text_content, "פרטים על היחידה")
    service_terms = parse_section(text_content, "תנאי שירות")
    next_service = parse_between(text_content, "תקופת שירות הקרובה")

    # Check for "⏰" (immediate recruitment)
    immediate = "כן" if "⏰" in text_content else "לא"

    # Check for "🔊 זמני או קבוע"
    recruitment_type = "זמני או קבוע" if "🔊 זמני או קבוע" in text_content else ""

    if not roles:
        # If no roles found, provide a placeholder row
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


# -----------------------------------------------------------
# SCRAPING WITH MULTITHREADING
# -----------------------------------------------------------
def download_html(post_id: int):
    """
    Download HTML for a given post ID, returning (post_id, html_content)
    or (post_id, None) on failure.
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
    Multithreaded: 
      1) Download all HTML pages for post IDs [start_id..end_id]
      2) Parse data 
      3) Combine into a single DataFrame
    """
    data = []

    # Step 1: Download concurrently
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        html_results = list(executor.map(download_html, range(start_id, end_id + 1)))

    # Step 2: Parse concurrently
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        parsed_lists = list(executor.map(lambda x: parse_job_info(x[0], x[1]), html_results))

    # Step 3: Flatten
    for plist in parsed_lists:
        if plist:
            data.extend(plist)

    return pd.DataFrame(data)


# -----------------------------------------------------------
# MAIN APP
# -----------------------------------------------------------
st.title("📌 חיפוש הזדמנויות גיוס")

# 1) Scrape data once (cached)
with st.spinner("🔄 טוען מודעות..."):
    df = scrape_jobs_concurrent(START_POST, END_POST)

st.success("✅ כל המודעות נטענו בהצלחה!")

# 2) Filter / Search UI
st.header("סינון וחיפוש")

search_query = st.text_input("🔎 חיפוש חופשי (בכל השדות):", "")

filtered_df = df.copy()
if search_query.strip():
    # Simple substring search across row values
    mask = filtered_df.apply(
        lambda row: search_query.lower() in " ".join(str(v).lower() for v in row.values),
        axis=1
    )
    filtered_df = filtered_df[mask]

# Optional: filter by אזור בארץ
all_areas = ["(הכל)"] + sorted(set(filtered_df["אזור בארץ"].dropna()))
selected_area = st.selectbox("סינון לפי אזור בארץ:", all_areas, index=0)
if selected_area != "(הכל)":
    filtered_df = filtered_df[filtered_df["אזור בארץ"] == selected_area]

# Optional: filter by סוג יחידה
all_units = ["(הכל)"] + sorted(set(filtered_df["סוג יחידה"].dropna()))
selected_unit = st.selectbox("סינון לפי סוג יחידה:", all_units, index=0)
if selected_unit != "(הכל)":
    filtered_df = filtered_df[filtered_df["סוג יחידה"] == selected_unit]

# Optional: filter by גיוס מיידי
immediate_opts = ["(הכל)", "כן", "לא"]
selected_immediate = st.selectbox("סינון לפי גיוס מיידי:", immediate_opts, index=0)
if selected_immediate != "(הכל)":
    filtered_df = filtered_df[filtered_df["גיוס מיידי"] == selected_immediate]

# Show results
st.write(f"נמצאו {len(filtered_df)} תוצאות:")
st.dataframe(filtered_df)

# Optionally, show expanders for each row:
# for idx, row in filtered_df.iterrows():
#     with st.expander(f"📌 {row['תפקיד']} (מודעה #{row['מספר מודעה']})"):
#         for col in [
#             "תפקיד", "סוג יחידה", "אזור בארץ", "כישורים נדרשים",
#             "פרטים על היחידה", "תנאי שירות", "תקופת שירות קרובה",
#             "גיוס מיידי", "סוג גיוס", "קישור"
#         ]:
#             st.write(f"**{col}:** {row[col]}")
