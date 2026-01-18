#!/usr/bin/env python3
import argparse
import csv
import os
import re
import sys
from html.parser import HTMLParser
from urllib.request import Request, urlopen


DEFAULT_URL = "https://en.wikipedia.org/wiki/List_of_UFC_champions"
KEYWORDS = [
    "retire",
    "retired",
    "retirement",
    "vacat",
    "vacant",
    "injur",
    "stripp",
    "suspend",
    "suspension",
]
REASON_KEYWORDS = {
    "retirement": ["retire", "retired", "retirement"],
    "strip": ["stripp"],
    "vacancy": ["vacat", "vacant"],
}


class WikiNoteParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._table_stack = []
        self._fighter_stack = []
        self._in_heading = False
        self._heading_buffer = []
        self._last_heading = ""
        self._in_caption = False
        self._caption_buffer = []
        self._in_tr = False
        self._in_cell = False
        self._cell_buffer = []
        self._cell_colspan = 1
        self._cell_is_header = False
        self._row_cells = []
        self.notes = []
        self.current_weight_class = ""
        self.last_fighter = ""
        self.current_champion_col = None

    def handle_starttag(self, tag, attrs):
        if tag in ("h2", "h3", "h4"):
            self._in_heading = True
            self._heading_buffer = []

        if tag == "table":
            class_attr = ""
            for k, v in attrs:
                if k == "class":
                    class_attr = v or ""
                    break
            classes = class_attr.split()
            is_wikitable = "wikitable" in classes
            self._table_stack.append(is_wikitable)
            if is_wikitable:
                self._fighter_stack.append([])
                if self._last_heading:
                    self.current_weight_class = self._last_heading
            return

        if tag == "caption" and self._in_wikitable():
            self._in_caption = True
            self._caption_buffer = []

        if tag == "tr" and self._in_wikitable():
            self._in_tr = True
            self._row_cells = []

        if tag in ("td", "th") and self._in_tr:
            self._in_cell = True
            self._cell_buffer = []
            self._cell_colspan = 1
            self._cell_is_header = tag == "th"
            for k, v in attrs:
                if k == "colspan":
                    try:
                        self._cell_colspan = int(v)
                    except ValueError:
                        self._cell_colspan = 1
                    break

    def handle_endtag(self, tag):
        if tag in ("h2", "h3", "h4") and self._in_heading:
            text = "".join(self._heading_buffer)
            self._heading_buffer = []
            self._in_heading = False
            cleaned = _normalize_text(text)
            if cleaned:
                self._last_heading = cleaned

        if tag == "caption" and self._in_caption:
            text = "".join(self._caption_buffer)
            self._caption_buffer = []
            self._in_caption = False
            cleaned = _normalize_text(text)
            if cleaned:
                self.current_weight_class = cleaned

        if tag in ("td", "th") and self._in_cell:
            text = "".join(self._cell_buffer)
            self._cell_buffer = []
            self._in_cell = False
            cleaned = _normalize_text(text)
            self._row_cells.append((cleaned, self._cell_colspan, self._cell_is_header))

        if tag == "tr" and self._in_tr:
            self._handle_row(self._row_cells)
            self._in_tr = False
            self._row_cells = []

        if tag == "table" and self._table_stack:
            was_wikitable = self._table_stack.pop()
            if was_wikitable and self._fighter_stack:
                self._fighter_stack.pop()
            if not self._table_stack:
                self.current_weight_class = ""
                self.last_fighter = ""
                self.current_champion_col = None

    def handle_data(self, data):
        if self._in_heading:
            self._heading_buffer.append(data)
        elif self._in_caption:
            self._caption_buffer.append(data)
        elif self._in_cell:
            self._cell_buffer.append(data)

    def _in_wikitable(self):
        return any(self._table_stack)

    def _handle_row(self, row_cells):
        if not row_cells:
            return

        if _is_note_row(row_cells):
            note = row_cells[0][0]
            if note:
                self.notes.append(
                    {
                        "note": note,
                        "weight_class": self.current_weight_class,
                        "fighter": self.last_fighter,
                        "fighters": list(self._fighter_stack[-1])
                        if self._fighter_stack
                        else [],
                    }
                )
            return

        if _is_header_row(row_cells):
            champion_col = _extract_champion_column(row_cells)
            if champion_col is not None:
                self.current_champion_col = champion_col
            return

        fighter = _extract_fighter(row_cells, self.current_champion_col)
        if fighter:
            self.last_fighter = fighter
            if self._fighter_stack:
                self._fighter_stack[-1].append(fighter)
        else:
            for candidate in _collect_fighters_from_row(row_cells):
                if self._fighter_stack:
                    self._fighter_stack[-1].append(candidate)


def _normalize_text(text):
    text = re.sub(r"\[[^\]]*\]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _split_sentences(text):
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [p.strip() for p in parts if p.strip()]


def _keyword_regex(keywords):
    escaped = [re.escape(k) for k in keywords]
    return re.compile(r"(" + "|".join(escaped) + r")", re.IGNORECASE)


def _infer_reason(statement):
    lowered = statement.lower()
    for reason, tokens in REASON_KEYWORDS.items():
        for token in tokens:
            if token in lowered:
                return reason
    return ""


def _infer_fighter_from_note(note, fighters):
    if not fighters:
        return ""
    lowered = note.lower()
    for fighter in fighters:
        if fighter and fighter.lower() in lowered:
            return fighter
    last_name_matches = []
    for fighter in fighters:
        parts = fighter.split()
        if len(parts) < 2:
            continue
        last = parts[-1]
        if re.search(rf"\\b{re.escape(last)}\\b", note):
            last_name_matches.append(fighter)
    if len(last_name_matches) == 1:
        return last_name_matches[0]
    return ""


def _infer_fighter_from_statement(note):
    if not note:
        return ""
    stopwords = {
        "the",
        "a",
        "an",
        "on",
        "in",
        "at",
        "after",
        "before",
        "when",
        "while",
        "during",
        "following",
    }
    months = {
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    }
    particles = {
        "da",
        "de",
        "del",
        "della",
        "der",
        "den",
        "di",
        "do",
        "dos",
        "du",
        "la",
        "le",
        "van",
        "von",
        "st",
        "st.",
    }
    words = [w for w in note.split() if w]
    candidates = []
    max_scan = min(8, len(words))
    for i in range(max_scan):
        raw = words[i]
        word = re.sub(r"^[^A-Za-z]+|[^A-Za-z'\\-]+$", "", raw)
        if not word:
            continue
        lowered = word.lower()
        if lowered in stopwords or lowered in months:
            continue
        if word[0].isupper():
            candidates = [word]
        elif lowered in particles and i + 1 < max_scan:
            nxt = re.sub(r"^[^A-Za-z]+|[^A-Za-z'\\-]+$", "", words[i + 1])
            if nxt and nxt[0].isupper():
                candidates = [word, nxt]
        if candidates:
            break
    if not candidates:
        return ""
    for raw in words[i + len(candidates) : max_scan]:
        word = re.sub(r"^[^A-Za-z]+|[^A-Za-z'\\-]+$", "", raw)
        if not word:
            break
        lowered = word.lower()
        if lowered in particles:
            candidates.append(word)
            continue
        if not word[0].isupper():
            break
        candidates.append(word)
        if len(candidates) >= 4:
            break
    return " ".join(candidates)


def _candidate_name_from_text(text):
    if not text or any(ch.isdigit() for ch in text):
        return ""
    lowered = text.lower()
    if "vacant" in lowered or "interim" in lowered:
        return ""
    if "def." in lowered or " vs " in lowered:
        return ""
    if len(text) < 3 or len(text) > 60:
        return ""
    if " " not in text and "-" not in text:
        return ""
    return text


def _collect_fighters_from_row(row_cells):
    candidates = []
    for text, _colspan, _is_header in row_cells:
        candidate = _candidate_name_from_text(text)
        if candidate:
            candidates.append(candidate)
    return candidates


def _is_note_row(row_cells):
    if len(row_cells) == 1 and row_cells[0][1] > 1:
        return True
    for text, colspan, _is_header in row_cells:
        if colspan > 1 and text:
            return True
    return False


def _is_header_row(row_cells):
    return any(is_header for _text, _colspan, is_header in row_cells)


def _extract_champion_column(row_cells):
    keywords = ("champion", "name", "fighter")
    col_index = 0
    for text, colspan, is_header in row_cells:
        if is_header and text:
            lowered = text.lower()
            if any(k in lowered for k in keywords):
                return col_index
        col_index += max(colspan, 1)
    return None


def _extract_fighter(row_cells, champion_col):
    if champion_col is not None:
        col_index = 0
        for text, colspan, _is_header in row_cells:
            if col_index <= champion_col < col_index + max(colspan, 1):
                candidate = _candidate_name_from_text(text)
                if candidate:
                    return candidate
                break
            col_index += max(colspan, 1)

    for text, _colspan, _is_header in row_cells:
        candidate = _candidate_name_from_text(text)
        if candidate:
            return candidate
    return ""


def _extract_date(sentence):
    months = (
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    )
    month_re = "|".join(months)
    patterns = [
        rf"({month_re})\s+\d{{1,2}},\s+\d{{4}}",
        rf"\d{{1,2}}\s+({month_re})\s+\d{{4}}",
        rf"({month_re})\s+\d{{4}}",
    ]
    for pattern in patterns:
        match = re.search(pattern, sentence)
        if match:
            return match.group(0)
    return ""


def fetch_html(url):
    req = Request(
        url,
        headers={"User-Agent": "ufc-data-warehouse/notes-extractor"},
    )
    with urlopen(req) as resp:
        return resp.read().decode("utf-8", errors="replace")


def extract_sentences(html, keywords):
    parser = WikiNoteParser()
    parser.feed(html)

    keyword_re = _keyword_regex(keywords)
    results = []
    seen = set()
    for entry in parser.notes:
        note = entry["note"]
        if not keyword_re.search(note):
            continue
        key = (note, entry["fighter"], entry["weight_class"])
        if key in seen:
            continue
        seen.add(key)
        results.append(
            {
                "date": _extract_date(note),
                "fighter": entry["fighter"]
                or _infer_fighter_from_note(note, entry.get("fighters", []))
                or _infer_fighter_from_statement(note),
                "weight_class": entry["weight_class"],
                "reason": _infer_reason(note),
                "sentence": note,
            }
        )
    return results


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Extract vacancy/retirement/suspension notes from UFC champions tables."
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="Source Wikipedia URL.",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(
            script_dir, "output", "title_vacancies.csv"
        ),
        help="Output file path.",
    )
    parser.add_argument(
        "--keywords",
        default=",".join(KEYWORDS),
        help="Comma-separated keyword list to match.",
    )
    args = parser.parse_args()

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    html = fetch_html(args.url)
    sentences = extract_sentences(html, keywords)

    output_path = args.output
    if not os.path.isabs(output_path):
        output_path = os.path.join(script_dir, output_path)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(
            ["date", "fighter", "weight_category", "reason", "statement"]
        )
        for entry in sentences:
            writer.writerow(
                [
                    entry["date"],
                    entry["fighter"],
                    entry["weight_class"],
                    entry["reason"],
                    entry["sentence"],
                ]
            )

    print(f"Wrote {len(sentences)} lines to {output_path}")


if __name__ == "__main__":
    sys.exit(main())
