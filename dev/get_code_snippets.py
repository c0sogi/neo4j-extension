import ast
import site
from pathlib import Path

SITE_DIR: str = next(
    s for s in site.getsitepackages() if s.endswith("site-packages")
)


def remove_docstrings_and_comments(source_code: str) -> str:
    """
    Return only the code part of the source code by removing docstrings and comments using AST.
    """
    result: str = ""
    for line in ast.unparse(ast.parse(source_code)).splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        result += line + "\n"
    return result


def get_code_snippet(file_path: str | Path, num_seperators: int = 10) -> str:
    """
    Read the file and return the cleaned code with the path.
    """
    fpath = Path(file_path).resolve()
    if not fpath.is_file():
        print(f"File not found: {file_path}")
        return ""

    with open(file_path, "r", encoding="utf-8") as f:
        source: str = f.read()
    cleaned_code: str = remove_docstrings_and_comments(source)

    if fpath.is_relative_to(SITE_DIR):
        display_path: Path = fpath.relative_to(SITE_DIR)
    elif fpath.is_relative_to(cwd := Path.cwd()):
        display_path = fpath.relative_to(cwd)
    else:
        display_path = fpath.absolute()

    return f"### {display_path}\n{cleaned_code}\n{'=' * num_seperators}\n\n"


if __name__ == "__main__":
    code_paths = list(Path("neo4j_extension").rglob("*.py"))
    code_snippets: str = "".join(
        get_code_snippet(file_path) for file_path in code_paths
    )
    Path(__file__).with_name("code_snippets.txt").write_text(
        code_snippets, encoding="utf-8"
    )
