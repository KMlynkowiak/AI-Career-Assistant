import re

TECH_DICT = [
    "python","sql","pandas","numpy","scikit-learn","tensorflow","pytorch",
    "aws","gcp","azure","docker","kubernetes","dbt","spark","airflow",
    "hadoop","kafka","redshift","snowflake","postgresql","mysql","git"
]

SENIORITY_PATTERNS = [
    (r"senior|sr\.", "Senior"),
    (r"mid|regular", "Mid"),
    (r"junior|entry", "Junior"),
]

def extract_skills(text: str):
    t = text.lower()
    found = sorted({tech for tech in TECH_DICT if tech in t})
    return found

def infer_seniority(text: str):
    t = text.lower()
    for pat, label in SENIORITY_PATTERNS:
        if re.search(pat, t):
            return label
    return "Unspecified"
