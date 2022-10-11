from __future__ import annotations

import pathlib
import resource
from dataclasses import dataclass


@dataclass
class Page:
    num: int = 0
    data: bytes = b""


class FreeList:
    def __init__(self) -> None:
        self._max_page = 0
        self._freed_pages = []

    def get_next_page(self) -> int:
        if self._freed_pages:
            page_id = self._freed_pages.pop()
            return page_id

        self._max_page += 1
        return self._max_page

    def free_page(self, page: int) -> None:
        self._freed_pages.append(page)


class DataAccessLayer:
    def __init__(self, file: pathlib.Path, page_size: int) -> None:
        self._file = file
        self._page_size = page_size
        self._free_list = FreeList()

    @classmethod
    def create(
        cls, path: str = "nopesql.db", page_size=resource.getpagesize()
    ) -> DataAccessLayer:
        file = pathlib.Path(path)
        return cls(file=file, page_size=page_size)

    def read_page(self, page_num: int) -> Page:
        offset = page_num * self._page_size
        with open(self._file, "rb") as f:
            f.seek(offset)
            data = f.read(self._page_size)

        return Page(num=page_num, data=data)

    def write_page(self, page: Page) -> None:
        offset = page.num * self._page_size

        data = page.data
        padding = offset - len(data)
        payload = data.ljust(padding, b"\0")

        with open(self._file, "ab") as f:
            f.seek(offset)
            f.write(payload)

    def get_next_page(self) -> int:
        return self._free_list.get_next_page()

    def free_page(self, page: int) -> None:
        self._free_list.free_page(page)
