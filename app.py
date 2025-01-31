import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
from fuzzywuzzy import process

# Load secrets from Streamlit secrets.toml
BASE_URL = st.secrets["TELEGRAM_BASE_URL"]
START_POST = int(st.secrets["START_POST"])
END_POST = int(st.secrets["END_POST"])

# Set page config
st.set_page_config(page_title="📌 מחפש עבודה בטלגרם", page_icon="🔍", layout="wide")

# Apply custom RTL style
st.markdown(
    """
    <style>
        body {
            direction: rtl;
            text-align: right;
        }
        .stTextInput > div > div > input {
            text-align: right;
        }
        .stDataFrame {
            direction: rtl;
        }
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

# Function to download the HTML from a Telegram post
def download_html(url):
    """Downloads HTML from a given URL and returns the content."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    return None

# Function to parse job ad details
def parse_job_info(html_content):
    """Parses job ad number and multiple roles from the HTML content."""
    try:
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract meta description content
        meta_desc = soup.find("meta", {"property": "og:description"})
        if not meta_desc:
            return "לא נמצא", []

        text_content = meta_desc["content"]

        # Extract the job ad number
        ad_number_match = re.search(r"מודעה מספר #(\d+)", text_content)
        ad_number = ad_number_match.group(1) if ad_number_match else "לא נמצא"

        # Extract all roles
        roles_section_match = re.search(r"(?:דרושים|דרוש|דרוש/ה)[^\n]*\n((?:\*\* .+\n)+)", text_content)
        roles = []
        if roles_section_match:
            roles_section = roles_section_match.group(1)
            roles = re.findall(r"\*\* (.+)", roles_section)

        return ad_number, roles

    except Exception as e:
        return "שגיאה", [f"❌ שגיאת ניתוח: {str(e)}"]

# Function to scrape multiple job posts
@st.cache_data  # Efficient caching
def scrape_jobs(start, end, base_url):
    """Scrapes job posts and stores results in a DataFrame."""
    data = []
    for post_id in range(start, end + 1):
        url = f"{base_url}{post_id}"
        html_content = download_html(url)
        if html_content:
            ad_number, roles = parse_job_info(html_content)
            for role in roles:
                data.append([ad_number, role, url])

        time.sleep(1)  # Prevent getting blocked

    df = pd.DataFrame(data, columns=["מספר מודעה", "תפקיד", "קישור"])
    return df

# --- UI ---
st.title("📌 מחפש עבודה בטלגרם")

# Show loading spinner while scraping
with st.spinner("🔄 טוען משרות חדשות..."):
    df = scrape_jobs(START_POST, END_POST, BASE_URL)

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
