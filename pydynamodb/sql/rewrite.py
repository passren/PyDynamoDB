# -*- coding: utf-8 -*-
"""Text-level helpers applied before a statement is parsed."""

import re
from typing import Any, List, Optional

# Quoted text first, so "--" or "/*" inside a literal or identifier is kept.
_SQL_TOKEN = re.compile(
    r"""(?P<quoted>'(?:[^']|'')*'|"(?:[^"]|"")*"|`[^`]*`)"""
    r"|(?P<comment>--[^\n]*|/\*.*?\*/)",
    re.S,
)


def strip_sql_comments(statement: str) -> str:
    """Return ``statement`` with every SQL comment replaced by a space."""
    return _SQL_TOKEN.sub(
        lambda match: " " if match.group("comment") else match.group(0), statement
    )


_TOKEN = re.compile(
    r"""(?P<quoted>'(?:[^']|'')*'|`[^`]*`)"""
    r"""|(?P<identifier>"(?:[^"]|"")+")"""
    r"|(?P<word>[A-Za-z_][A-Za-z0-9_]*)"
    r"|(?P<space>\s+)"
    r"|(?P<other>.)",
    re.S,
)
# Words that can follow a table but are not an alias.
_NOT_AN_ALIAS = {
    "WHERE",
    "ORDER",
    "GROUP",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "UNION",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "OUTER",
    "CROSS",
    "ON",
    "AS",
}


def _identifier(token: Any) -> Optional[str]:
    if token.lastgroup == "word":
        return token.group()
    if token.lastgroup == "identifier":
        return token.group()[1:-1].replace('""', '"')
    return None


def unalias_select(operation: str) -> Optional[str]:
    """``SELECT ... FROM t [AS] a ...`` with the alias resolved, else None.

    PartiQL has no table aliases: DynamoDB rejects them and reads ``a.id`` as
    the path ``id`` inside attribute ``a``. In a single-table SELECT an alias
    only names that table, so dropping the alias and its qualifier (``a.id`` ->
    ``id``, ``a.*`` -> ``*``) is the same query. Anything else (joins,
    subqueries, a second FROM) returns None.
    """
    tokens = list(_TOKEN.finditer(strip_sql_comments(operation).strip()))
    words = [
        (index, token.group().upper())
        for index, token in enumerate(tokens)
        if token.lastgroup == "word"
    ]
    if not words or words[0] != (0, "SELECT"):
        return None
    if any(t.group() == "(" for t in tokens) and any(
        word == "SELECT" for _, word in words[1:]
    ):
        return None
    froms = [index for index, word in words if word == "FROM"]
    if len(froms) != 1 or any(word == "JOIN" for _, word in words):
        return None
    position = froms[0] + 1
    if position >= len(tokens) or tokens[position].lastgroup != "space":
        return None
    position += 1
    table_start = position
    while position < len(tokens) and tokens[position].lastgroup != "space":
        position += 1
    if position == table_start or position + 1 >= len(tokens):
        return None
    table_end = position
    position += 1
    token = tokens[position]
    if token.lastgroup == "word" and token.group().upper() == "AS":
        position += 1
        if position >= len(tokens) or tokens[position].lastgroup != "space":
            return None
        position += 1
    if position >= len(tokens):
        return None
    alias_token = tokens[position]
    alias = _identifier(alias_token)
    if alias is None or (
        alias_token.lastgroup == "word" and alias.upper() in _NOT_AN_ALIAS
    ):
        return None
    after_alias = position + 1
    if after_alias < len(tokens) and tokens[after_alias].lastgroup != "space":
        return None

    def references_alias(token: Any) -> bool:
        name = _identifier(token)
        if name is None:
            return False
        if alias_token.lastgroup == "word" and token.lastgroup == "word":
            return name.upper() == alias.upper()
        return name == alias

    def resolve(part: List[Any]) -> str:
        text = []
        skip = False
        for index, token in enumerate(part):
            if skip:
                skip = False
                continue
            if (
                index + 1 < len(part)
                and part[index + 1].group() == "."
                and (index == 0 or part[index - 1].group() != ".")
                and references_alias(token)
            ):
                skip = True
                continue
            text.append(token.group())
        return "".join(text)

    return "%s%s%s" % (
        resolve(tokens[:table_start]),
        "".join(token.group() for token in tokens[table_start:table_end]),
        resolve(tokens[after_alias:]),
    )
