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

import os
import re
from datetime import datetime
from pathlib import Path


def patch():
    with open("pyproject.toml") as f:
        content = f.read()
    with open("pyproject.toml", "w") as f:
        f.write(
            re.sub(
                r'^version = "[0-9]{6}"$',
                f'version = "{datetime.now().strftime("%y%m%d")}"',
                content,
                flags=re.MULTILINE,
            )
        )


if __name__ == "__main__":
    os.chdir(Path(__file__).parent.parent)
    patch()
