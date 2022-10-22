from unittest import TestCase
from fs.memoryfs import MemoryFS
from tilekiln.config import Config, Definition
import yaml


class TestConfig(TestCase):
    def test_properties(self):
        with MemoryFS() as fs:
            c = Config('''{"metadata": {}}''', fs)
            self.assertEqual(c.name, None)
            self.assertEqual(c.description, None)
            self.assertEqual(c.attribution, None)
            self.assertEqual(c.version, None)
            self.assertEqual(c.bounds, None)
            self.assertEqual(c.center, None)

        with MemoryFS() as fs:
            c_str = ('''{"metadata": {"name": "name", '''
                     '''"description":"description", '''
                     '''"attribution":"attribution", "version": "1.0.0",'''
                     '''"bounds": [-180, -85, 180, 85], "center": [0, 0]},'''
                     '''"vector_layers": {"building":{'''
                     '''"description": "buildings",'''
                     '''"fields":{}}}}''')

            # Check the test is valid yaml to save debugging
            yaml.safe_load(c_str)
            c = Config(c_str, fs)
            self.assertEqual(c.name, "name")
            self.assertEqual(c.description, "description")
            self.assertEqual(c.attribution, "attribution")
            self.assertEqual(c.version, "1.0.0")
            self.assertEqual(c.bounds, [-180, -85, 180, 85])
            self.assertEqual(c.center, [0, 0])


class TestDefinition(TestCase):
    def test_attributes(self):
        with MemoryFS() as fs:
            fs.writetext("blank.sql.jinja2", "")
            d = Definition({"minzoom": 1, "maxzoom": 3, "extent": 1024,
                            "buffer": 8, "file": "blank.sql.jinja2"}, fs)
            self.assertEqual(d.minzoom, 1)
            self.assertEqual(d.maxzoom, 3)
            self.assertEqual(d.extent, 1024)
            self.assertEqual(d.buffer, 8)

            d = Definition({"minzoom": 2, "maxzoom": 4,
                            "file": "blank.sql.jinja2"}, fs)
            self.assertEqual(d.minzoom, 2)
            self.assertEqual(d.maxzoom, 4)
            self.assertEqual(d.extent, 4096)
            self.assertEqual(d.buffer, 0)
