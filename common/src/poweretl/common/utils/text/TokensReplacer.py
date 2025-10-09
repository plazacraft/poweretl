import re
from typing import Dict



class TokensReplacer:
    """Replaces tokens in a text with corresponding values from a dictionary.
    Attributes:
        start (str): The starting delimiter for tokens. Default is "{".
        end (str): The ending delimiter for tokens. Default is "}".
        escape (str): The escape character for tokens. Default is "\\".
    """
    def __init__(self, start: str = "{", end: str = "}", escape: str = "\\"):
        self.start = start
        self.end = end
        self.escape = escape

        # Match unescaped tokens across multiple lines
        self._pattern = re.compile(
            rf'(?<!{re.escape(self.escape)}){re.escape(self.start)}(.*?){re.escape(self.end)}',
            flags=re.DOTALL
        )

        # Match escaped tokens to clean them up later
        self._escaped_pattern = re.compile(
            rf'{re.escape(self.escape)}({re.escape(self.start)}.*?{re.escape(self.end)})',
            flags=re.DOTALL
        )

    def replace(self, text: str, tokens: Dict[str, str]) -> str:
        def replacer(match):
            token = match.group(1)
            if token not in tokens:
                raise KeyError(f"Missing replacement for token: '{token}'")
            return tokens[token]

        # Replace unescaped tokens
        result = self._pattern.sub(replacer, text)

        # Remove escape character from escaped tokens: \{token} → {token}
        result = self._escaped_pattern.sub(lambda m: m.group(1), result)

        return result