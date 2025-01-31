import re

def parse_roles(text: str) -> list:
    """
    Parse the list of roles from the '⬅️ דרושים:' section.
    Each role line typically starts with '** '.
    Returns a list of role strings.
    """
    # Instead of "⬅️\s*דרושים\s*:\s*((?s).*?)(?=...)", do this:
    pattern = r"⬅️\s*דרושים\s*:\s*(.*?)(?=\n⬅️|\n-+\s|$)"
    match = re.search(pattern, text, flags=re.DOTALL | re.UNICODE)  
    roles_list = []
    if match:
        roles_section = match.group(1)
        # Now find lines that start with "**"
        # E.g. "** חובש קרבי", "** פראמדיק", etc.
        roles_list = re.findall(r"\*\*\s*(.+)", roles_section)
        roles_list = [r.strip() for r in roles_list]
    return roles_list


def parse_section(text: str, section_title: str) -> str:
    """
    Extract lines from “⬅️ <section_title>:” 
    until the next '⬅️' or triple-dash line.
    """
    # Instead of "⬅️\s*{section_title}\s*:\s*((?s).*?)(?=...)", do this:
    pattern = rf"⬅️\s*{section_title}\s*:\s*(.*?)(?=\n⬅️|\n-+\s|$)"
    match = re.search(pattern, text, flags=re.DOTALL | re.UNICODE)
    if match:
        extracted = match.group(1).strip()
        # Remove extra dashes if any
        return re.sub(r"-+\s*", "", extracted).strip()
    return ""
