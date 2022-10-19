# Tilekiln Configuration Specification

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).

## Overview

A tilekiln configuration is composed of a REQUIRED YAML configuration file with zero or more OPTIONAL SQL Jinja2 files.

## Configuration

The configuration SHALL be a [YAML](https://yaml.org/spec/1.2/spec.html) document. It MUST NOT be a YAML stream containing multiple documents.

## SQL Files

SQL Jinja files are processed with Jinja2 as documented below. They SHOULD form a valid PostgreSQL SELECT statement with one column which is a PostGIS geometry in the coordinates space of the vector tile. This SHOULD be done with `ST_AsMVTGeom(geom, {{bbox}}, {{extent}}))`.

### Jinja substitutions

#### `{{ zoom }}`

The zoom of the tile being generated.

#### `{{ x }}`

The x coordinate of the tile being generated.

#### `{{ y }}`

The y coordinate of the tile being generated.

#### `{{ bbox }}`

A SQL statement that evaluates to the buffered bounding box of the tile being generated.

#### `{{ unbuffered_bbox }}`

A SQL statement that evaluates to the unbuffered bounding box of the tile being generated.

#### `{{ extent }}`

The tile [extent](https://github.com/mapbox/vector-tile-spec/tree/master/2.1#3-projection-and-bounds) in screen space.

#### `{{ buffer }}`

The tile buffer, in units of tile coordinate space.

#### `{{ tile_length }}`

The side length of the tile being generated in web mercator meters.

#### `{{ tile_area }}`

The area of the tile being generated in square web mercator meters. Equal to `{{ tile_length }}` squared.

#### `{{ coordinate_length }}`

The side length of one unit in the coordinate space of the tile being generated in web mercator meters. Equal to `{{ tile_length }} / {{ extent }}`.

#### `{{ coordinate_area }}`

The area of one unit in the coordinate space of the tile being generated in square web mercator meters. Equal to `{{ coordinate_length }}` squared.
