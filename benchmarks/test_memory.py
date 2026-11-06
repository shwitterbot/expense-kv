import ctypes
import io
import os
import sys
import unittest

from memory import Client, RUsageV4, key, linux_memory, process_memory


class MemoryChecks(unittest.TestCase):
    def test_linux_units(self):
        self.assertEqual(linux_memory("Name: test\nVmRSS: 123 kB\nVmHWM: 456 kB\n"),
                         {"rss_bytes": 125952, "rss_high_water_bytes": 466944})

    def test_native_rss(self):
        result = process_memory(os.getpid())
        self.assertGreater(result["rss_bytes"], 0)
        if sys.platform == "darwin":
            self.assertEqual(ctypes.sizeof(RUsageV4), 296)
            self.assertGreater(result["physical_footprint_bytes"], 0)
            self.assertGreaterEqual(result["lifetime_peak_footprint_bytes"], result["physical_footprint_bytes"])

    def test_binary_reply_and_truncation(self):
        client = Client.__new__(Client)
        client.reader = io.BytesIO(b"$4\r\na\0\r\n\r\n:10\r\n+OK\r\n")
        self.assertEqual(client.reply(), b"a\0\r\n")
        self.assertEqual(client.reply(), 10)
        self.assertEqual(client.reply(), b"OK")
        with self.assertRaises(RuntimeError):
            client.reply()
        client.reader = io.BytesIO(b"$4\r\nabc")
        with self.assertRaises(RuntimeError):
            client.reply()

    def test_server_errors_fail_the_measurement(self):
        client = Client.__new__(Client)
        client.reader = io.BytesIO(b"-OOM configured memory limit reached\r\n")
        with self.assertRaisesRegex(RuntimeError, "OOM"):
            client.reply()

    def test_key_length_and_uniqueness(self):
        self.assertEqual(len(key(0)), 14)
        self.assertEqual(len(key(49999)), 14)
        self.assertNotEqual(key(0), key(49999))


if __name__ == "__main__":
    unittest.main()
