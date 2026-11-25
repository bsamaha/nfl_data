"""Words from a chat with an agent, and paths on one machine, do not belong in the code or docs.

A reader has not seen the chat, so "Strategy 1" or "the checkbook" tells them nothing; name what the thing does.
`AGENTS.md` states this rule with examples, so it and this file are the two places the words may appear.
"""

import re
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
# Letters on either side end the match, not underscores, so `a_tier_2_check` in a test name counts too.
CHAT_WORDS = re.compile(
    r"(?<![a-z])(?:strategy|option|tier|stage)s?[\s_]+[0-9a-c](?![a-z0-9])|checkbook", re.IGNORECASE
)
MACHINE_PATHS = re.compile(r"file:///|\b[A-Za-z]:[\\/]Users[\\/]|/c/Users/", re.IGNORECASE)
CHECKED_FILES = ("*.md", "docs/*.md", "catalog/*.yml", "nfl_lake/*.py", "tests/*.py", "pyproject.toml", ".githooks/*")
WHERE_THE_RULE_LIVES = {"AGENTS.md", Path(__file__).name}


def find_chat_words(text: str) -> list[tuple[int, str]]:
    """Every (line, word) that names something from a chat or a path on one machine."""
    return [
        (line_number, match.group(0))
        for line_number, line in enumerate(text.splitlines(), start=1)
        for pattern in (CHAT_WORDS, MACHINE_PATHS)
        for match in pattern.finditer(line)
    ]


def test_code_and_docs_name_what_things_do():
    findings = [
        f"{path.relative_to(REPO_DIR).as_posix()}:{line_number}: {word}"
        for pattern in CHECKED_FILES
        for path in sorted(REPO_DIR.glob(pattern))
        if path.is_file() and path.name not in WHERE_THE_RULE_LIVES
        for line_number, word in find_chat_words(path.read_text(encoding="utf-8"))
    ]
    assert not findings, "\n" + "\n".join(findings)


def test_each_kind_of_word_is_noticed():
    text = "Strategy 1 checks\nthe checkbook\nStage 4 tables\ntest_when_a_tier_2_check_fails\nsee file:///C:/x\nC:\\Users\\me"
    assert [line_number for line_number, _ in find_chat_words(text)] == [1, 2, 3, 4, 5, 6]
