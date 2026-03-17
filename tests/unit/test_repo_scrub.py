from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _decode_hex(value: str) -> str:
    return bytes.fromhex(value).decode("utf-8")


FORBIDDEN_TEXT = tuple(
    _decode_hex(value)
    for value in (
        "63727970746f2d6f72622d7265736561726368",
        "534f4c55534454",
        "75707472656e645f726576657273696f6e",
        "646f776e7472656e645f726576657273696f6e",
        "646f776e7472656e645f627265616b646f776e",
        "75707472656e645f636f6e74696e756174696f6e",
        "636f6d706172655f666f72776172645f76735f626173656c696e652e7079",
        "726567696d655f616e616c797369732e7079",
        "726f627573746e6573735f7461626c652e7079",
        "74756e655f6d616e69666573742e7079",
        "74756e655f72756e2e7079",
        "74756e655f6c6561646572626f6172642e7079",
        "77616c6b5f666f72776172645f74756e652e7079",
        "77616c6b5f666f72776172645f726567696d655f66696c7465722e7079",
        "77616c6b5f666f72776172645f73776565702e7079",
    )
)
TEXT_EXTENSIONS = {
    ".csv",
    ".json",
    ".md",
    ".py",
    ".txt",
    ".yaml",
    ".yml",
}


def _iter_text_files() -> list[Path]:
    out: list[Path] = []
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in {".git", ".venv", ".pytest_cache", "__pycache__"} for part in path.parts):
            continue
        if path.name == "test_repo_scrub.py":
            continue
        if path.name == ".gitignore" or path.suffix.lower() in TEXT_EXTENSIONS:
            out.append(path)
    return out


def test_repo_text_files_do_not_include_legacy_strategy_identifiers() -> None:
    offenders: list[str] = []
    for path in _iter_text_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        for needle in FORBIDDEN_TEXT:
            if needle in text:
                offenders.append(f"{path.relative_to(REPO_ROOT)}::{needle}")
    assert offenders == []


def test_reports_and_data_scaffolds_do_not_ship_committed_artifacts() -> None:
    expected_reports = {
        Path("reports/.gitkeep"),
        Path("reports/README.md"),
    }
    expected_data = {
        Path("data/README.md"),
        Path("data/manifest.json"),
        Path("data/raw/.gitkeep"),
        Path("data/processed/.gitkeep"),
    }

    report_files = {path.relative_to(REPO_ROOT) for path in (REPO_ROOT / "reports").rglob("*") if path.is_file()}
    data_files = {
        path.relative_to(REPO_ROOT)
        for path in (REPO_ROOT / "data").rglob("*")
        if path.is_file() and "heartbeat" not in path.name
    }

    assert report_files == expected_reports
    assert data_files == expected_data
    assert not (REPO_ROOT / Path(_decode_hex("73747261746567792e7079"))).exists()
