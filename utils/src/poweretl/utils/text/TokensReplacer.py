import re
from typing import Dict



class TokensReplacer:
    """Replaces tokens in a text with corresponding values from a dictionary.
    Attributes:
        re_start (str): The starting delimiter for tokens (regex). Default is "{".
        re_end (str): The ending delimiter for tokens (regex). Default is "}".
        re_escape (str): The escape character for tokens (regex). Default is "\\".
    """
    def __init__(self, re_start: str = r"\{", re_end: str = r"\}", re_escape: str = "\\\\"):


        # Match unescaped tokens across multiple lines
        self._pattern = re.compile(
            rf'(?<!{re_escape})({re_start})(?P<token>.*?)({re_end})',
            flags=re.DOTALL
        )

        # Match escaped tokens to clean them up later
        self._escaped_pattern = re.compile(
            rf'{re_escape}({re_start}.*?{re_end})',
            flags=re.DOTALL
        )

    def replace(self, text: str, tokens: Dict[str, str]) -> str:
        def replacer(match):
            token = match.group("token")
            if token not in tokens:
                raise KeyError(f"Missing replacement for token: '{token}'")
            return tokens[token]

        # Replace unescaped tokens
        result = self._pattern.sub(replacer, text)

        # Remove escape character from escaped tokens: \{token} → {token}
        result = self._escaped_pattern.sub(lambda m: m.group(1), result)

        return result