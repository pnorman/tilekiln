# Based on https://github.com/pypa/sampleproject/blob/main/setup.py

from importlib.metadata import entry_points
from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

setup (
    name="tilekiln",
    version="0.1.0",
    description="A set of command-line utilities to generate and serve Mapbox Vector Tiles (MVTs)",

    # Use README.md as the long description
    long_description=(here / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    url="https://github.com/pnorman/tilekiln",
    author="Paul Norman",
    author_email="osm@paulnorman.ca",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: GIS"
    ],
    keywords="openstreetmap, mvt, osm",
    packages=find_packages(),
    python_requires=">=3.9, <4",
    install_requires=[
        "Click",
        "pyyaml",
        "fs",
        "Jinja2",
        "fastapi",
        "psycopg"
    ],
    extras_require={
        "test": ["pytest"]
    },
    setup_requires=["flake8"],
    entry_points={
        "console_scripts": ["tilekiln = tilekiln.scripts:cli"]
    },
    project_urls={
        "Bug Reports": "https://github.com/pnorman/tilekiln/issues",
        "Source": "https://github.com/pypa/tilekiln/",
    }
)
