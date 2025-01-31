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

# Function to download the HTML from a Telegram post
def download_html(url):
    """Downloads HTML from a given URL and returns the content."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.text
    else:
        return None

# Function to parse the job ad number and multiple roles from the HTML content
def parse_job_info(html_content):
    """Parses job ad number and multiple roles from the HTML content."""
    try:
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract text content from meta description (where job ad info is stored)
        meta_desc = soup.find("meta", {"property": "og:description"})
        if not meta_desc:
            return "Not Found", []

        text_content = meta_desc["content"]

        # Extract the job ad number
        ad_number_match = re.search(r"מודעה מספר #(\d+)", text_content)
        ad_number = ad_number_match.group(1) if ad_number_match else "Not Found"

        # Extract all roles
        roles_section_match = re.search(r"(?:דרושים|דרוש|דרוש/ה)[^\n]*\n((?:\*\* .+\n)+)", text_content)
        roles = []

        if roles_section_match:
            roles_section = roles_section_match.group(1)
            roles = re.findall(r"\*\* (.+)", roles_section)

        return ad_number, roles

    except Exception as e:
        return "Error", [f"❌ Parsing error: {str(e)}"]

# Function to scrape multiple job posts in a range
@st.cache_data  # Cache results to avoid redundant API calls
def scrape_jobs(start, end, base_url):
    """Scrapes job posts from a range of Telegram links and stores results in a DataFrame."""
    data = []

    for post_id in range(start, end + 1):
        url = f"{base_url}{post_id}"
        html_content = download_html(url)
        if html_content:
            ad_number, roles = parse_job_info(html_content)

            # Add results to the dataset
            for role in roles:
                data.append([ad_number, role, url])

        # Pause to prevent being blocked
        time.sleep(2)

    # Convert results to DataFrame
    df = pd.DataFrame(data, columns=["Ad Number", "Role", "URL"])
    return df

# Streamlit UI
st.title("📌 Telegram Job Scraper")

st.sidebar.header("Settings")
st.sidebar.write(f"🔗 Scraping from **{BASE_URL}{START_POST}** to **{BASE_URL}{END_POST}**")

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

# Scrape the jobs
st.write("⏳ Scraping jobs, please wait...")
df = scrape_jobs(START_POST, END_POST, BASE_URL)

# Save results to an Excel file
df.to_excel("telegram_jobs.xlsx", index=False)

st.success("✅ Data Scraped Successfully!")
st.write(df)

# Download button for Excel file
st.download_button(
    label="📥 Download Excel",
    data=open("telegram_jobs.xlsx", "rb"),
    file_name="telegram_jobs.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# Role Search Section
st.header("🔍 Search for Roles")

search_query = st.text_input("Enter role name:", "")
if search_query:
    search_results = process.extract(search_query, df["Role"].tolist(), limit=5)
    matched_roles = [match[0] for match in search_results if match[1] > 50]

    if matched_roles:
        st.write(f"🎯 Best Matches for '{search_query}':")
        filtered_df = df[df["Role"].isin(matched_roles)]
        st.write(filtered_df)
    else:
        st.warning("❌ No matching roles found.")
