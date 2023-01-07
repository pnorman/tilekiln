# Tilekiln

## Background

Tilekiln is a set of command-line utilities to generate and serve Mapbox Vector Tiles (MVTs).

Generation relies on the standard method of a PostgreSQL + PostGIS server as a data source, and ST_AsMVT to serialize the MVTs.

The target use-case is vector tiles for OpenStreetMap Carto on openstreetmap.org, a worldwide complex basemap under high load.

Minutely updates are supported with an appropriately updating database.

## Requirements

Tilekiln requires a PostGIS database with data loaded to generate vector tiles.

[OpenStreetMap Carto's](https://github.com/gravitystorm/openstreetmap-carto/blob/master/INSTALL.md#openstreetmap-data) directions are a good starting place for loading OpenStreetMap data into a PostGIS database, but any PostGIS data source in EPSG 3857 will work.

- PostgreSQL 10+
- PostGIS 3.1+

## History

The tilekiln configuration syntax is based on studies and experience with other vector tile and map generation configurations. In particular, it is heavily inspired by Tilezen's use of Jinja2 templates and TileJSON for necessary metadata.

## License

### Code

Copyright © 2022-2023 Paul Norman <osm@paulnorman.ca>

The code is licensed terms of the GNU General Public License as
published by the Free Software Foundation, either version 3 of
the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

### Documentation

The text of the documentation and configuration format specification is licensed under a [Creative Commons Attribution 4.0 International License](https://creativecommons.org/licenses/by/4.0/). However, the use of the specification in products and code is entirely free: there are no royalties, restrictions, or requirements.

### Sample configuration

The sample configuration files are released under the CC0 Public
Domain Dedication, version 1.0, as published by Creative Commons.
To the extent possible under law, the author(s) have dedicated all
copyright and related and neighboring rights to the Software to
the public domain worldwide. The Software is distributed WITHOUT
ANY WARRANTY.

If you did not receive a copy of the CC0 Public Domain Dedication
along with the Software, see
<http://creativecommons.org/publicdomain/zero/1.0/>
