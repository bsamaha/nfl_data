"""The rules for tests in this repo, as a check that fails instead of a paragraph to remember.

A test here asserts what happened: files on disk, what was fetched and built, counts, exit codes. A test that pins
the wording of a message, or replaces part of the code it runs, breaks on a harmless edit and passes on a harmful
one. Each finding says what to do instead.
"""

import ast
from pathlib import Path

PATCHING_METHODS = {"setattr", "delattr", "setitem", "delitem"}
WORDING = "assert what happened instead: files on disk, counts, an exit code"
REPLACING = "pass the collaborator in as an argument instead, as GitHubReleasesSource takes open_url"
PRIVATE = "test through the command or function a user calls"


def _is_phrase(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, str) and " " in node.value


def find_broken_rules(source: str) -> list[tuple[int, str]]:
    """Every (line, what is wrong and what to do instead) in the source of one test file."""
    findings: list[tuple[int, str]] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and any(keyword.arg == "match" for keyword in node.keywords):
            findings.append((node.lineno, f"match= pins the wording of a message; {WORDING}"))
        elif isinstance(node, ast.arg) and node.arg == "caplog":
            findings.append((node.lineno, f"caplog reads log lines, which are wording; {WORDING}"))
        elif isinstance(node, ast.Assert):
            for part in ast.walk(node.test):
                is_substring_test = isinstance(part, ast.Compare) and any(
                    isinstance(operator, (ast.In, ast.NotIn)) for operator in part.ops
                )
                if is_substring_test and any(_is_phrase(side) for side in (part.left, *part.comparators)):
                    findings.append((node.lineno, f"looking for a phrase in text pins its wording; {WORDING}"))
                    break
        elif isinstance(node, ast.Attribute) and node.attr in PATCHING_METHODS:
            findings.append((node.lineno, f".{node.attr}() replaces part of the code under test; {REPLACING}"))
        elif isinstance(node, ast.Attribute) and node.attr.startswith("_") and not node.attr.startswith("__"):
            findings.append((node.lineno, f".{node.attr} is private; {PRIVATE}"))
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            module = getattr(node, "module", None) or ""
            imported = [f"{module}.{alias.name}".lstrip(".") for alias in node.names]
            if any(name == "mock" or name.startswith(("mock.", "unittest.mock")) for name in imported):
                findings.append((node.lineno, f"a mock replaces part of the code under test; {REPLACING}"))
            if module.split(".")[0] == "nfl_lake" and any(alias.name.startswith("_") for alias in node.names):
                findings.append((node.lineno, f"a name starting with _ is private; {PRIVATE}"))
    return sorted(findings)


def test_every_test_file_keeps_the_rules_for_tests():
    findings = [
        f"{test_path.name}:{line}: {finding}"
        for test_path in sorted(Path(__file__).parent.glob("*.py"))
        for line, finding in find_broken_rules(test_path.read_text(encoding="utf-8"))
    ]
    assert not findings, "\n" + "\n".join(findings)


RULE_BREAKING_TEST_FILE = """\
import pytest
from unittest import mock
from nfl_lake.cli import _open_lake

def test_update_says_so(monkeypatch, caplog, source):
    monkeypatch.setattr("nfl_lake.pipeline.run_update", fake_run)
    with pytest.raises(ValueError, match="nothing to update"):
        run()
    assert "nothing was published" in result.stderr
    assert source._open_url is fake_opener
"""


def test_each_rule_is_noticed_when_a_test_breaks_it():
    # one line per rule: a mock, a private import, caplog, a patch, match=, a phrase looked for, a private attribute
    assert [line for line, _ in find_broken_rules(RULE_BREAKING_TEST_FILE)] == [2, 3, 5, 6, 7, 9, 10]
