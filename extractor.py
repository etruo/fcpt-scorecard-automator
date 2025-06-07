from pathlib import Path
import pdfplumber, re
from typing import Optional


KW = [
    "LEASE", "RENT", "ACRE", "ADDRESS", "TENANT",
    "CURRENT", "GLA", "CAP RATE", "YEAR BUILT", "DRIVE-THRU", "CARRY-OUT"
]

KW_REGEX = re.compile("|".join(KW), re.I)
TITLE_REGEX = re.compile(r"(PROPERTY OVERVIEW|RENT ROLL|TENANT PROFILE)", re.I)

def looks_like_real_table(table: list[list[str]]) -> bool:
    """
    Reject grids that are mostly one-letter cells.

    • >= 10 non-empty cells
    • average ≥ 1.5 words per non-empty cell
    """
    non_empty = [c for row in table for c in row if c and c.strip()]
    if len(non_empty) < 10:
        return False
    word_ratio = sum(len(c.split()) for c in non_empty) / len(non_empty)
    return word_ratio >= 1.5


def is_good_table(table, keywords):
    """
    Evaluates whether a table meets quality criteria.
    Args:
        table (list): Table extracted by PDFplumber.
        keywords (list): Keywords to look for in the table.
    Returns:
        bool: True if the table meets the criteria, False otherwise.
    """
    if not table or len(table) < 2:  # Ensure the table has at least 2 rows
        return False

    # Flatten the table into a single string for keyword matching
    flat_table = " ".join(" ".join(cell.strip() if cell else "" for cell in row) for row in table)

    # Check if the table contains any of the keywords
    if any(keyword.lower() in flat_table.lower() for keyword in keywords):
        return True
    
    return False

def extract_tables(pdf_path: Path,
                   settings_list: list[dict],
                   keywords: list[str]) -> list[dict]:
    """
    Try each settings-combo on each page **only until we find ≥1 good table**.
    """
    kw_regex = re.compile("|".join(re.escape(k) for k in keywords), re.I)
    good_tables = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_no, page in enumerate(pdf.pages, start=1):
            # Skip pages that don't mention any keyword → big speed win
            page_text = (page.extract_text() or "").upper()
            if not kw_regex.search(page_text):
                continue

            for opts in settings_list:
                tables = page.extract_tables(
                    {
                        "vertical_strategy": opts["vertical_strategy"],
                        "horizontal_strategy": opts["horizontal_strategy"],
                        "intersection_x_tolerance": opts["intersection_x_tolerance"],
                        "intersection_y_tolerance": opts["intersection_y_tolerance"],
                    }
                ) or []

                for tbl in tables:
                    if looks_like_real_table(tbl):
                        good_tables.append(
                            {"page": page_no, "settings": opts, "table": tbl}
                        )
                        # Early exit: stop once we have 2–3 high-quality tables
                        if len(good_tables) >= 3:
                            return good_tables
            # (optional) break if we already have something after this page
            if good_tables:
                return good_tables

    return good_tables  # may be empty

def extract_plain_text(pdf_path: Path) -> str:
    """Pull all visible text from every page, collapsed into paragraphs."""
    text_pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text_pages.append(page.extract_text() or "")
    return "\n\n".join(text_pages)

def keyword_window(text: str, window=2) -> str:
    lines = text.upper().splitlines()
    keep = set()
    for i, ln in enumerate(lines):
        if KW_REGEX.search(ln):
            keep.update(range(max(0, i - window), min(len(lines), i + window + 1)))
    return "\n".join(lines[i] for i in sorted(keep))

def get_best_payload(source: str | Path, *, settings_list=None, keywords=None) -> str:
    """
    Get the best text payload from either a text string or PDF file.
    
    Args:
        source: Either a string of text or a Path to a PDF file
        settings_list: Optional list of table extraction settings
        keywords: Optional list of keywords to look for
        
    Returns:
        str: The extracted text payload
    """
    if isinstance(source, str):
        return keyword_window(source) or source
    elif isinstance(source, Path):
        full = extract_plain_text(source)
        return keyword_window(full) or full
    else:
        raise TypeError(f"Expected str or Path, got {type(source)}")

def parse_deal(*, plain_text: Optional[str] = None, pdf_path: Optional[Path] = None) -> dict:
    """
    Extract deal information from either plain text or a PDF file.
    
    Args:
        plain_text (str, optional): Plain text input (e.g., from email)
        pdf_path (Path, optional): Path to PDF file
        
    Returns:
        dict: Extracted fields
    """
    if plain_text is not None:
        return get_best_payload(plain_text)
    elif pdf_path is not None:
        return get_best_payload(pdf_path)
    else:
        raise ValueError("Must provide either plain_text or pdf_path")