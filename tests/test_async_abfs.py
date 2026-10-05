from __future__ import annotations

import gzip
import unittest
from typing import TypedDict

from pydantic import BaseModel, ValidationError
from typing_extensions import TypedDict as ExtensionTypedDict
from typing_extensions import assert_type

from dean_utils.utils.async_abfs import async_abfs


class Item(BaseModel):
    name: str
    count: int


class TypedItem(TypedDict):
    name: str
    count: int


class ExtensionTypedItem(ExtensionTypedDict):
    name: str
    count: int


class MemoryAbfs(async_abfs):
    def __init__(self, data: bytes):
        self.data = data

    async def read(self, path: str) -> bytes:
        return self.data


class ReadJsonTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_decoding(self):
        for data, expected in (
            (b'{"name":"sample","count":2}', {"name": "sample", "count": 2}),
            (b"[1,2]", [1, 2]),
        ):
            with self.subTest(data=data):
                storage = MemoryAbfs(data)
                result = assert_type(
                    await storage.read_json("container/blob"), dict | list
                )
                self.assertEqual(result, expected)
                explicit_none = assert_type(
                    await storage.read_json("container/blob", None), dict | list
                )
                self.assertEqual(explicit_none, expected)

    async def test_model_decoding(self):
        data = b'{"name":"sample","count":"2"}'
        for payload in (data, gzip.compress(data)):
            with self.subTest(compressed=payload != data):
                storage = MemoryAbfs(payload)
                result = assert_type(
                    await storage.read_json("container/blob", Item), Item
                )
                self.assertIsInstance(result, Item)
                self.assertEqual(result, Item(name="sample", count=2))

    async def test_model_list_decoding(self):
        data = b'[{"name":"sample","count":"2"}]'
        for payload in (data, gzip.compress(data), b"[]"):
            with self.subTest(payload=payload):
                storage = MemoryAbfs(payload)
                result = assert_type(
                    await storage.read_json("container/blob", list[Item]), list[Item]
                )
                self.assertEqual(
                    result, [] if payload == b"[]" else [Item(name="sample", count=2)]
                )

    async def test_validation_errors(self):
        storage = MemoryAbfs(b'{"name":"sample","count":"invalid"}')
        with self.assertRaises(ValidationError):
            await storage.read_json("container/blob", Item)
        with self.assertRaises(ValidationError):
            await storage.read_json("container/blob", list[Item])

    async def test_typed_dict_decoding_without_validation(self):
        data = b'{"count":"invalid"}'
        for payload in (data, gzip.compress(data)):
            with self.subTest(compressed=payload != data):
                storage = MemoryAbfs(payload)
                result = assert_type(
                    await storage.read_json("container/blob", TypedItem), TypedItem
                )
                self.assertIsInstance(result, dict)
                self.assertEqual(result, {"count": "invalid"})

    async def test_typed_dict_list_decoding_without_validation(self):
        data = b'[{"count":"invalid"}]'
        for payload in (data, gzip.compress(data), b"[]"):
            with self.subTest(payload=payload):
                storage = MemoryAbfs(payload)
                result = assert_type(
                    await storage.read_json("container/blob", list[TypedItem]),
                    list[TypedItem],
                )
                self.assertEqual(
                    result, [] if payload == b"[]" else [{"count": "invalid"}]
                )

    async def test_extension_typed_dict_decoding_without_validation(self):
        storage = MemoryAbfs(b'{"count":"invalid"}')
        result = assert_type(
            await storage.read_json("container/blob", ExtensionTypedItem),
            ExtensionTypedItem,
        )
        self.assertEqual(result, {"count": "invalid"})
        storage = MemoryAbfs(b'[{"count":"invalid"}]')
        results = assert_type(
            await storage.read_json("container/blob", list[ExtensionTypedItem]),
            list[ExtensionTypedItem],
        )
        self.assertEqual(results, [{"count": "invalid"}])

    async def test_default_gzip_decoding(self):
        storage = MemoryAbfs(gzip.compress(b'{"name":"sample","count":2}'))
        self.assertEqual(
            await storage.read_json("container/blob"), {"name": "sample", "count": 2}
        )


if __name__ == "__main__":
    unittest.main()
