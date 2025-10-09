import pytest
from poweretl.common.utils.text import TokensReplacer

def test_basic_replacement():
    replacer = TokensReplacer(start="{", end="}", escape="\\")
    text = "Hello {user}, your balance is {amount}."
    replacements = {"user": "Roman", "amount": "100 PLN"}

    result = replacer.replace(text, replacements)
    assert result == "Hello Roman, your balance is 100 PLN."

def test_escaped_token_preserved():
    replacer = TokensReplacer(start="{", end="}", escape="\\")
    text = "Escaped token: \\{user}"
    replacements = {"user": "Roman"}

    result = replacer.replace(text, replacements)
    assert result == "Escaped token: {user}"

def test_multiline_replacement():
    replacer = TokensReplacer(start="{", end="}", escape="\\")
    text = """Hello {user},
Your balance is {amount}.
Escaped: \\{user}"""
    replacements = {"user": "Roman", "amount": "100 PLN"}

    result = replacer.replace(text, replacements)
    assert result == """Hello Roman,
Your balance is 100 PLN.
Escaped: {user}"""

def test_missing_token_raises():
    replacer = TokensReplacer(start="{", end="}", escape="\\")
    text = "Hello {user}, your balance is {amount}."

    with pytest.raises(KeyError, match="Missing replacement for token: 'amount'"):
        replacer.replace(text, tokens={"user": "Roman"})
