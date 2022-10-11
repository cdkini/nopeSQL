from __future__ import annotations

import pathlib
import resource
from dataclasses import dataclass


@dataclass
class Page:
    """
    The smallest unit of data that is exchanged/stored in the KV store.

    Essentially a byte array that contains data and a certain amount of padding to ensure
    a consistent size (dictated by the DAL, which defaults to the page size set by the
    system architecture - Ex: 4096 bytes macOS).
    """
    num: int = 0
    data: bytes = b""


class FreeList:
    """
    Responsible for the allocation and deallocation of pages.

    As pages become emptied, they must be freed to avoid fragmentation.
    """
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
    """
    Handles all disk operations and manages how data is organized on the disk.
    """
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
            data = f.read(self._page_size) # Will include any \0 byte padding from `write_page`

        return Page(num=page_num, data=data)

    def write_page(self, page: Page) -> None:
        offset = page.num * self._page_size

        # Necessary padding to ensure consistently sized writes
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
