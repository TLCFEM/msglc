#!/usr/bin/env python3

#  Copyright (C) 2026 Theodore Chang
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

import json
import os
import re
import urllib.request
from datetime import datetime
from pathlib import Path


def version_exists(version: str) -> bool:
    try:
        with urllib.request.urlopen(
            "https://pypi.org/pypi/msglc/json", timeout=5
        ) as response:
            return version in json.load(response).get("releases", {})
    except Exception:  # noqa
        return False


def new_version() -> str:
    candidate = datetime.now().strftime("%y%m%d")

    while version_exists(candidate):
        candidate = str(int(candidate) + 1).zfill(6)

    return candidate


def patch():
    target = Path("pyproject.toml")
    target.write_text(
        re.sub(
            r'^version = "[0-9]{6}"$',
            f'version = "{new_version()}"',
            target.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    os.chdir(Path(__file__).parent.parent)
    patch()
