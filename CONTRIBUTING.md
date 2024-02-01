# Contributing

## General guidance

Tilekiln is an open source project and welcomes contributions from others. If it's something big that you want to do, you should open issue first to make sure it will fit in with the plans. Please keep in mind the following guidelines

1. Keep the codebase small and simple. Easier development means more time writing styles.
2. Make writing simple schemas easy and complex ones possible. There are a lot of specialized tasks needed for some styles, and it needs to be possible to implement them, but we want a simple experience for simple styles.
3. Consider if what you want can be done with Jinja2 templates already, and if so if it should be added.
4. Consider adding functionality to PostGIS. If a task that needs doing has wider applicability than just vector tiles, consider if it should be added to PostGIS.
5. Keep all knowledge of the Mapbox Vector Tile format confined to PostGIS. See point #1 above. Trying to write vector tile generation code is fraught with difficulties. That's why Tilekiln doesn't, instead it relies on PostGIS knowing how to generate vector tiles. Because so many people use ST_AsMVT and ST_AsMVTGeom, they are well tested, reliable, and well supported functions.

## Development install

A development install requires Python 3.10+, and normally will require a PostgreSQL 10+ PostGIS 3.1+ server.

For a conventional development install on a machine running a recent common Linux distribution with standard settings,

```bash
python3 -m venv venv
. venv/bin/activate
pip install -e .
pip install flake8 pytest
```

You can then edit code and run the `tilekiln` command.

## Pre-commit checks
Make sure to run pre-commit checks so that your PR won't fail in CI

```sh
flake8 tilekiln tests
mypy tilekiln tests
pytest
```

If you have pytest installed elsewhere on your system, it might not know to use the one associated with the venv. If so, instead run `venv/bin/pytest`.

## Releases

Releases are automatically built from tagged commits. Tag the version number, beginning with `v`. If you want to use a development version, these are automatically built for TestPyPI.
