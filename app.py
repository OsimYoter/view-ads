import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
from fuzzywuzzy import process
from concurrent.futures import ThreadPoolExecutor

# ---------------------------------------------------------------
# LOAD SECRETS
# ---------------------------------------------------------------
BASE_URL = st.secrets["TELEGRAM_BASE_URL"]
START_POST = int(st.secrets["START_POST"])
END_POST = int(st.secrets["END_POST"])
MAX_THREADS = 10  # Number of concurrent requests

# ---------------------------------------------------------------
# PAGE CONFIG & CUSTOM RTL STYLE
# ---------------------------------------------------------------
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

# ---------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------

def download_html(post_id: int):
    """
    Downloads HTML from a given post ID.
    Returns (post_id, html_content) or (post_id, None) if failed.
    """
    url = f"{BASE_URL}{post_id}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            return post_id, response.text
    except requests.exceptions.RequestException:
        pass
    return post_id, None


def parse_ad_number(text_content: str) -> str:
    """
    Extract 'מודעה מספר #XXXX' from the text.
    Return 'לא נמצא' if not found.
    """
    match = re.search(r"מודעה\s*מספר\s*#(\d+)", text_content)
    if match:
        return match.group(1)
    return "לא נמצא"


def parse_between(text: str, start_marker: str) -> str:
    """
    Extract text from `start_marker:` up to either the next '⬅️', a line of dashes, or the end.
    If not found, return empty string.
    Example usage for single-line fields like "סוג יחידה: חי״ר".
    """
    # Regex:  start_marker: (.*?)(?=\n- - -|\n⬅️|$)
    # Use DOTALL (?s) so '.' can match newlines
    pattern = rf"{start_marker}\s*:\s*(.*?)(?=\n-+\s|\n⬅️|$)"
    match = re.search(pattern, text, flags=re.UNICODE | re.DOTALL)
    return match.group(1).strip() if match else ""


def parse_section(text: str, section_title: str) -> str:
    """
    Extract lines from `⬅️ <section_title>:` until the next '⬅️' or triple-dash line.
    Used for multi-line sections like כישורים נדרשים, פרטים על היחידה, תנאי שירות, etc.
    """
    # Example pattern: "⬅️ כישורים נדרשים:\s*((?s).*?)(?=\n⬅️|\n- - -|$)"
    pattern = rf"⬅️\s*{section_title}\s*:\s*((?s).*?)(?=\n⬅️|\n-+\s|$)"
    match = re.search(pattern, text, flags=re.UNICODE)
    if match:
        # Clean up trailing lines or extra newlines/dashes
        extracted = match.group(1).strip()
        return re.sub(r"-+\s*", "", extracted).strip()
    return ""


def parse_roles(text: str) -> list:
    """
    Parse the list of roles from the '⬅️ דרושים:' section.
    Each role line typically starts with '** '.
    Returns a list of role strings.
    """
    # Identify the entire "⬅️ דרושים:" section
    pattern = r"⬅️\s*דרושים\s*:\s*((?s).*?)(?=\n⬅️|\n-+\s|$)"
    match = re.search(pattern, text, flags=re.UNICODE)
    roles_list = []
    if match:
        roles_section = match.group(1)
        # Now find lines that start with "**"
        # E.g. "** חובש קרבי", "** פראמדיק", etc.
        roles_list = re.findall(r"\*\*\s*(.+)", roles_section)
        # Strip each role
        roles_list = [r.strip() for r in roles_list]
    return roles_list


def parse_job_info(post_id: int, html_content: str):
    """
    Parse out all relevant fields from the HTML meta 'og:description'.
    Return a list of dicts (one per role) so we can flatten it into rows.
    """
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, "html.parser")
    meta_desc = soup.find("meta", {"property": "og:description"})
    if not meta_desc:
        return None

    text_content = meta_desc["content"]

    # -----------------------------------------------------------
    # Extract fields
    # -----------------------------------------------------------
    ad_number = parse_ad_number(text_content)

    sug_yehida = parse_between(text_content, "סוג יחידה")       # סוג יחידה
    area = parse_between(text_content, "אזור בארץ")            # אזור בארץ

    # Multi-role list
    roles = parse_roles(text_content)

    qualifications = parse_section(text_content, "כישורים נדרשים")  # כישורים נדרשים
    unit_info = parse_section(text_content, "פרטים על היחידה")       # פרטים על היחידה
    service_terms = parse_section(text_content, "תנאי שירות")        # תנאי שירות

    # Single line or short sections:
    next_service = parse_between(text_content, "תקופת שירות הקרובה") # תקופת שירות הקרובה

    # Check for immediate recruitment ⏰ גיוס מיידי
    immediate_recruitment = "כן" if "⏰" in text_content else "לא"

    # Check for "זמני או קבוע" 🔊
    recruitment_type = "זמני או קבוע" if "🔊 זמני או קבוע" in text_content else ""

    # -----------------------------------------------------------
    # Build up results
    # -----------------------------------------------------------
    results = []
    # If no roles found, we still want at least one record
    if not roles:
        roles = ["לא צוינו תפקידים"]

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
            "גיוס מיידי": immediate_recruitment,
            "סוג גיוס": recruitment_type,
            "קישור": f"{BASE_URL}{post_id}"
        }
        results.append(row)

    return results


@st.cache_data
def scrape_jobs_concurrent(start: int, end: int) -> pd.DataFrame:
    """
    Scrapes job posts using multithreading and returns a DataFrame of all fields.
    One row per role.
    """
    data = []

    # Step 1: Download all pages concurrently
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        html_results = list(executor.map(download_html, range(start, end + 1)))

    # Step 2: Parse job data concurrently
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        parsed_results = list(executor.map(lambda x: parse_job_info(x[0], x[1]), html_results))

    # Step 3: Flatten
    for result in parsed_results:
        if result:
            data.extend(result)

    df = pd.DataFrame(data)
    return df


# ---------------------------------------------------------------
# MAIN APP
# ---------------------------------------------------------------
st.title("📌 חיפוש הזדמנויות גיוס")

# Step 1: Scrape data (and cache it)
with st.spinner("🔄 טוען מודעות..."):
    df = scrape_jobs_concurrent(START_POST, END_POST)

st.success("✅ כל המודעות נטענו בהצלחה!")

# ---------------------------------------------------------------
# FILTERS / SEARCH UI
# ---------------------------------------------------------------
st.header("סינון וחיפוש")

# --- 1) Text Search across all columns ---
search_query = st.text_input("🔎 חיפוש חופשי (בכל השדות):", "")

filtered_df = df.copy()
if search_query.strip():
    # We'll do a "contains" check on *all* text columns.
    # Combine row into a single string, then fuzzy match or substring match.
    # For a simpler substring approach across all fields:
    mask = filtered_df.apply(
        lambda row: search_query.lower() in " ".join(
            str(v).lower() for v in row.values
        ), 
        axis=1
    )
    filtered_df = filtered_df[mask]

# --- 2) Optional drop-down filter by "אזור בארץ" ---
all_areas = ["(הכל)"] + sorted(set(filtered_df["אזור בארץ"].dropna().unique()))
selected_area = st.selectbox("סינון לפי אזור בארץ:", all_areas, index=0)
if selected_area != "(הכל)":
    filtered_df = filtered_df[filtered_df["אזור בארץ"] == selected_area]

# --- 3) Optional drop-down filter by "סוג יחידה" ---
all_units = ["(הכל)"] + sorted(set(filtered_df["סוג יחידה"].dropna().unique()))
selected_unit = st.selectbox("סינון לפי סוג יחידה:", all_units, index=0)
if selected_unit != "(הכל)":
    filtered_df = filtered_df[filtered_df["סוג יחידה"] == selected_unit]

# --- 4) Optional filter by "גיוס מיידי" ---
immediate_options = ["(הכל)", "כן", "לא"]
selected_immediate = st.selectbox("סינון לפי גיוס מיידי:", immediate_options, index=0)
if selected_immediate != "(הכל)":
    filtered_df = filtered_df[filtered_df["גיוס מיידי"] == selected_immediate]

# Show the final, filtered results
st.write(f"נמצאו {len(filtered_df)} תוצאות:")

# Display the table or clickable expanders
# For a quick table:
st.dataframe(filtered_df)

# Alternatively, you could present them as expanders:
# for idx, row in filtered_df.iterrows():
#     with st.expander(f"📌 {row['תפקיד']} (מודעה #{row['מספר מודעה']})"):
#         for col in ["תפקיד", "סוג יחידה", "אזור בארץ", "כישורים נדרשים",
#                     "פרטים על היחידה", "תנאי שירות", "תקופת שירות קרובה",
#                     "גיוס מיידי", "סוג גיוס", "קישור"]:
#             st.write(f"**{col}**: {row[col]}")
