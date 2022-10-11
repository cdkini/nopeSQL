from __future__ import annotations

import pathlib
import resource
from dataclasses import dataclass
from typing import List


@dataclass
class Page:
    num: int
    data: bytes


@dataclass
class FreeList:
    max_page: int
    freed_pages: List[int]

    def get_next_page(self) -> int:
        if self.freed_pages:
            page_id = self.freed_pages.pop()
            return page_id

        self.max_page += 1
        return self.max_page

    def free_page(self, page: int) -> None:
        self.freed_pages.append(page)


@dataclass
class DataAccessLayer:
    file: pathlib.Path
    page_size: int
    free_list: FreeList

    @classmethod
    def create(
        cls, path: str = "nopesql.db", page_size=resource.getpagesize()
    ) -> DataAccessLayer:
        file = pathlib.Path(path)
        free_list = FreeList(max_page=0, freed_pages=[])
        return cls(file=file, page_size=page_size, free_list=free_list)

    def read_page(self, page_num: int) -> Page:
        page = self.allocate_empty_page()

        offset = page_num * self.page_size
        with open(self.file, "rb") as f:
            f.seek(offset)
            data = f.readline()

        page.data = data
        return page

    def write_page(self, page: Page) -> None:
        offset = page.num * self.page_size
        with open(self.file, "ab") as f:
            f.seek(offset)
            f.write(page.data)

    def allocate_empty_page(self) -> Page:
        return Page(num=0, data=b"")

    def get_next_page(self) -> int:
        return self.free_list.get_next_page()

    def free_page(self, page: int) -> None:
        self.free_list.free_page(page)
