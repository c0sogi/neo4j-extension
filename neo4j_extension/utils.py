import unicodedata
from typing import List as PyList
from typing import LiteralString, cast

DELIMITERS = {"[", "]", "(", ")", "{", "}", ",", ":", "'", '"'}


def escape_identifier(key: str) -> LiteralString:
    """
    Escape a property key for use in a Cypher query.
    If the key is a valid identifier, it is returned as is.
    Otherwise, it is escaped with backticks.
    """
    if all(
        unicodedata.category(char).startswith(("L", "N")) or char == "_"
        for char in key
    ):
        return cast(LiteralString, key)
    escaped = key.replace("`", "``")
    return cast(LiteralString, f"`{escaped}`")


def get_safe_query(query: LiteralString, **labels: str) -> LiteralString:
    """
    Return a Cypher query with the given labels as parameters.

    The query should contain placeholders for the labels in the format
    `{label_name}`. The labels will be escaped and inserted into the query.

    Example:
    ```python
    query = "MATCH (n:{label}) RETURN n"
    labels = {"label": "Person"}
    print(_get_type_query(query, **labels))
    # MATCH (n:Person) RETURN n
    ```

    Args:
        query: The Cypher query with placeholders for labels.
        **labels: The labels to insert into the query.

    Returns:
        The Cypher query with the labels inserted.
    """

    s = query.format(
        **{key: escape_identifier(value) for key, value in labels.items()}
    )
    return cast(LiteralString, s)


def tokenize_cypher_expression(expr: str) -> PyList[str]:
    """
    Simple Cypher expression tokenizer.
    """
    tokens: PyList[str] = []
    i = 0
    length = len(expr)

    while i < length:
        c = expr[i]

        # 공백
        if c.isspace():
            i += 1
            continue

        if c in ("(", ")", "[", "]", "{", "}", ",", ":"):
            tokens.append(c)
            i += 1
            continue

        if c in ("'", '"'):
            # 문자열 리터럴
            quote_char = c
            start_index = i
            i += 1
            escaped = False
            while i < length:
                if escaped:
                    escaped = False
                    i += 1
                else:
                    if i < length and expr[i] == "\\":
                        escaped = True
                        i += 1
                    elif i < length and expr[i] == quote_char:
                        i += 1
                        break
                    else:
                        i += 1
            str_token = expr[start_index:i]
            tokens.append(str_token)
            continue

        # 식별자/숫자 등
        start_index = i
        while i < length:
            if expr[i].isspace() or expr[i] in DELIMITERS:
                break
            i += 1
        sub = expr[start_index:i]
        tokens.append(sub)

    return tokens


def split_by_comma_top_level(tokens: PyList[str]) -> PyList[str]:
    """
    Split tokens by ',' at the top level.
    """
    result: PyList[str] = []
    current_tokens: PyList[str] = []
    stack: PyList[str] = []
    level = 0
    matching = {"(": ")", "[": "]", "{": "}"}

    for t in tokens:
        if t in ("(", "[", "{"):
            stack.append(t)
            level += 1
            current_tokens.append(t)
        elif t in (")", "]", "}"):
            if not stack:
                raise ValueError(f"Unmatched closing bracket: {t}")
            top = stack.pop()
            level -= 1
            if matching[top] != t:
                raise ValueError(f"Mismatched bracket: {t}")
            current_tokens.append(t)
        elif t == "," and level == 0:
            # split
            result.append("".join(current_tokens).strip())
            current_tokens = []
        else:
            current_tokens.append(t)

    if stack:
        raise ValueError(f"Unclosed bracket(s) in expression: {stack}")

    if current_tokens:
        result.append("".join(current_tokens).strip())

    return result
