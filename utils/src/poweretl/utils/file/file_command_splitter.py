import re
from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass
class CommandEntry:
    file: str
    version: str
    command: str


class FileCommandSplitter:
    """Parses text files that contain VERSION and COMMAND sections.
    It splits files to parts version-command."""

    def __init__(
        self,
        re_version=r"^\s*--\s*VERSION:\s*(?P<version>.+)\s*$",
        re_command=r"^\s*--\s*COMMAND\s*$",
    ):
        self._version_re = re.compile(re_version)
        self._command_re = re.compile(re_command)

    @classmethod
    def version_key(cls, v: str):
        if not v:
            return (0,)
        parts = re.split(r"[^0-9A-Za-z]+", v)
        key = []
        for p in parts:
            if p.isdigit():
                key.append(int(p))
            else:
                key.append(p.lower())
        return tuple(key)

    def _parse_text(self, file_path: str, text: str) -> List[CommandEntry]:
        lines = text.splitlines()
        current_version = ""
        in_command = False
        command_lines: List[str] = []
        results: List[CommandEntry] = []

        for line in lines:
            mver = self._version_re.match(line)
            if mver:
                # If we're inside a command, close it before switching versions
                if in_command:
                    command_text = "\n".join(command_lines).strip()
                    if command_text:
                        results.append(
                            CommandEntry(
                                file=file_path,
                                version=current_version,
                                command=command_text,
                            )
                        )
                    command_lines = []
                    in_command = False
                # set current version
                current_version = mver.group("version").strip()
                continue

            if self._command_re.match(line):
                # When we see a COMMAND marker, close any existing command and
                # start a new one. This makes each COMMAND marker the delimiter
                # that ends the previous command and begins the next.
                if in_command:
                    command_text = "\n".join(command_lines).strip()
                    if command_text:
                        results.append(
                            CommandEntry(
                                file=file_path,
                                version=current_version,
                                command=command_text,
                            )
                        )
                    command_lines = []
                    in_command = True
                else:
                    in_command = True
                continue

            if in_command:
                command_lines.append(line)

        # If file ends while in a command, close it
        if in_command:
            command_text = "\n".join(command_lines).strip()
            if command_text:
                results.append(
                    CommandEntry(
                        file=file_path,
                        version=current_version,
                        command=command_text,
                    )
                )

        return results


    def get_commands(self, files: list[tuple[Path, str]]) -> List[CommandEntry]:
        """Read multiple files and return combined list of (version, command).

        Results are returned in the order files are provided; callers can sort
        them as needed.
        """
        entries: List[CommandEntry] = []
        for file, content in files:
            entries.extend(self._parse_text(file, content))

        # Sort by semantic version ascending, then by file path ascending
        entries.sort(key=lambda e: (self.version_key(e.version), e.file or ""))
        return entries
