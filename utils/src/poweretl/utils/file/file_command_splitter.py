# Mostly AI

import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class CommandEntry:
    file: str
    version: str
    step: int
    command: str


class FileCommandSplitter:
    """Parses text files that contain VERSION and COMMAND sections.
    It splits files to parts version-command.

    Attributes:
        ref_version (str): Regex to define how Version section is parsed
        re_command (str): Regex to define how Command section is parsed
    """

    def __init__(
        self,
        re_version: str = r"^\s*--\s*VERSION:\s*(?P<version>.+)\s*$",
        re_command: str = r"^\s*--\s*COMMAND\s*$",
    ):
        self._version_re = re.compile(re_version)
        self._command_re = re.compile(re_command)

    @classmethod
    def version_key(cls, version: str) -> tuple[str]:
        """Parse version string

        Args:
            version (str): Version to parse.

        Returns:
            tuple[str]: Tuple with version parts.
        """
        if not version:
            return (0,)
        parts = re.split(r"[^0-9A-Za-z]+", version)
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
                                step=0,
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
                                step=0,
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
                        step=0,
                        command=command_text,
                    )
                )

        return results

    def get_commands(
        self,
        files: list[tuple[Path, str]],
        version: Optional[str] = None,
        step: Optional[int] = None,
    ) -> List[CommandEntry]:
        """Read multiple files and return combined list of (version, command),
        optionally filtered to only entries strictly greater than the provided
        version/step marker.

        Results are returned in the order files are provided; callers can sort
        them as needed.

        Args:
            files (list[tuple[Path, str]]): List of files and their contents to process.
            version (str | None, optional): Target version marker. Only entries with
                version greater than this one will be returned, or entries with
                the same version but step greater than ``step``. Defaults to "".
            step (int | None, optional): Target step marker within the target version.
                Only steps strictly greater than this value are returned for the
                same version. Defaults to 0 when None.

        Returns:
            List[CommandEntry]: Ordered list of Files, Commands and their Versions.
        """
        entries: List[CommandEntry] = []
        for file, content in files:
            entries.extend(self._parse_text(file, content))

        # Sort by semantic version ascending, then by file path ascending
        entries.sort(key=lambda e: (self.version_key(e.version), e.file or ""))

        # Assign sequential step numbers within each version (starting from 1)
        version_counters: dict[str, int] = {}
        for e in entries:
            v = e.version or ""
            cnt = version_counters.get(v, 0) + 1
            version_counters[v] = cnt
            e.step = cnt

        # Apply filters only if version or step is provided
        if version is None and step is None:
            return entries

        # Filter by target (version, step): include strictly greater items
        target_key = self.version_key(version or "")
        target_step = step or 0

        def is_greater(e: CommandEntry) -> bool:
            ev = self.version_key(e.version or "")
            if ev > target_key:
                return True
            if ev == target_key and e.step > target_step:
                return True
            return False

        filtered = [e for e in entries if is_greater(e)]

        return filtered
