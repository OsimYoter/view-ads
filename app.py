import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
from fuzzywuzzy import process
from concurrent.futures import ThreadPoolExecutor

# Load secrets from Streamlit secrets.toml
BASE_URL = st.secrets["TELEGRAM_BASE_URL"]
START_POST = int(st.secrets["START_POST"])
END_POST = int(st.secrets["END_POST"])
MAX_THREADS = 10  # Number of concurrent requests

# Set page config
st.set_page_config(page_title="📌 מחפש עבודה בטלגרם", page_icon="🔍", layout="wide")

# Apply custom RTL style
st.markdown(
    """
    <style>
        body { direction: rtl; text-align: right; }
        .stTextInput > div > div > input { text-align: right; }
        .stDataFrame { direction: rtl; }
        .stButton > button { font-size: 18px; font-weight: bold; background-color: #FF4B4B; color: white; width: 100%; }
    </style>
    """,
    unsafe_allow_html=True,
)

# Function to download the HTML from a Telegram post
def download_html(post_id):
    """Downloads HTML from a given post ID and returns the content."""
    url = f"{BASE_URL}{post_id}"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            return post_id, response.text
    except requests.exceptions.RequestException:
        return post_id, None
    return post_id, None

# Function to parse job ad details
def parse_job_info(post_id, html_content):
    """Parses job ad number and multiple roles from the HTML content."""
    if not html_content:
        return None
    
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        meta_desc = soup.find("meta", {"property": "og:description"})
        if not meta_desc:
            return None

        text_content = meta_desc["content"]

        # Extract job ad number
        ad_number_match = re.search(r"מודעה מספר #(\d+)", text_content)
        ad_number = ad_number_match.group(1) if ad_number_match else "לא נמצא"

        # Extract roles
        roles_section_match = re.search(r"(?:דרושים|דרוש|דרוש/ה)[^\n]*\n((?:\*\* .+\n)+)", text_content)
        roles = []
        if roles_section_match:
            roles_section = roles_section_match.group(1)
            roles = re.findall(r"\*\* (.+)", roles_section)

        return [(ad_number, role, f"{BASE_URL}{post_id}") for role in roles]

    except Exception as e:
        return None

# Function to scrape multiple job posts concurrently
@st.cache_data  # Efficient caching
def scrape_jobs_concurrent(start, end):
    """Scrapes job posts using multithreading and returns a DataFrame."""
    data = []
    
    # Step 1: Download all pages concurrently
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        html_results = list(executor.map(download_html, range(start, end + 1)))

    # Step 2: Parse job data concurrently
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        parsed_results = list(executor.map(lambda x: parse_job_info(x[0], x[1]), html_results))

    # Step 3: Flatten results and create DataFrame
    for result in parsed_results:
        if result:
            data.extend(result)

    df = pd.DataFrame(data, columns=["מספר מודעה", "תפקיד", "קישור"])
    return df

# --- UI ---
st.title("📌 מחפש עבודה בטלגרם")

# Show loading spinner while scraping
with st.spinner("🔄 טוען משרות חדשות..."):
    df = scrape_jobs_concurrent(START_POST, END_POST)

st.success("✅ כל המשרות נטענו בהצלחה!")

# --- Search Section ---
st.header("🔍 חפש תפקידים")
search_query = st.text_input("הכנס שם תפקיד:", "")

if search_query:
    search_results = process.extract(search_query, df["תפקיד"].tolist(), limit=5)
    matched_roles = [match[0] for match in search_results if match[1] > 50]

    if matched_roles:
        st.write(f"🎯 **התוצאות הטובות ביותר עבור '{search_query}':**")
        filtered_df = df[df["תפקיד"].isin(matched_roles)]
        
        for _, row in filtered_df.iterrows():
            with st.expander(f"📌 {row['תפקיד']}"):
                st.write(f"🔢 **מספר מודעה:** {row['מספר מודעה']}")
                st.write(f"🔗 **[פרטי המודעה ופניה למגייס]({row['קישור']})**")

    else:
        st.warning("❌ לא נמצאו תפקידים תואמים.")
