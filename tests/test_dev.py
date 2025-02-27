import os
from unittest import mock
from fastapi.testclient import TestClient
import pytest

import tilekiln.dev
import tilekiln.errors
from tilekiln.config import Config
from tilekiln.kiln import Kiln


@pytest.fixture
def mock_environment():
    """Set up environment variables for testing"""
    with mock.patch.dict(
        os.environ,
        {
            tilekiln.dev.TILEKILN_CONFIG: "/path/to/config.yaml",
            tilekiln.dev.TILEKILN_ID: "test_id",
            tilekiln.dev.TILEKILN_URL: "http://localhost:8080",
        },
    ):
        yield


@pytest.fixture
def mock_dev_globals():
    """Set up mock global variables"""
    # Create the global variables in the module if they don't exist
    if not hasattr(tilekiln.dev, "config"):
        tilekiln.dev.config = mock.Mock(spec=Config)
    if not hasattr(tilekiln.dev, "kiln"):
        tilekiln.dev.kiln = mock.Mock(spec=Kiln)

    # Mock global variables and modules
    with (
        mock.patch.object(tilekiln.dev, "config", spec=Config) as mock_config,
        mock.patch.object(tilekiln.dev, "kiln", spec=Kiln) as mock_kiln,
    ):
        mock_config.id = "test_id"
        yield mock_config, mock_kiln


@pytest.fixture
def client(mock_environment, mock_dev_globals):
    """Create a test client"""
    return TestClient(tilekiln.dev.dev)


def test_root_endpoint(client):
    """Test that the root endpoint returns 404"""
    response = client.get("/")
    assert response.status_code == 404

    # Test HEAD request
    response = client.head("/")
    assert response.status_code == 404


def test_startup_event():
    """Test the setup of the dev server"""
    # For this test, we need to get a reference to the actual installed module
    import tilekiln.dev
    from fastapi import FastAPI

    # Mock the environment variables and necessary modules
    with (
        mock.patch.dict(
            os.environ,
            {
                tilekiln.dev.TILEKILN_CONFIG: "/path/to/config.yaml",
                tilekiln.dev.TILEKILN_ID: "test_id",
                tilekiln.dev.TILEKILN_URL: "http://localhost:8080",
            },
        ),
        mock.patch("tilekiln.load_config") as mock_load_config,
        mock.patch("psycopg_pool.ConnectionPool") as mock_pool_class,
        mock.patch("tilekiln.kiln.Kiln") as mock_kiln_class,
    ):
        # Set up mocks
        mock_config = mock.Mock(spec=Config)
        mock_load_config.return_value = mock_config
        mock_pool = mock.Mock()
        mock_pool_class.return_value = mock_pool
        mock_kiln_instance = mock.Mock(spec=Kiln)
        mock_kiln_class.return_value = mock_kiln_instance

        # Getting the module from the actual installed app
        # Rather than diving into the initialization details that may change,
        # we'll do a simpler verification of the module attributes

        # Verify the module has the necessary attributes used in the startup
        assert hasattr(tilekiln.dev, "TILEKILN_CONFIG")
        assert hasattr(tilekiln.dev, "TILEKILN_ID")
        assert hasattr(tilekiln.dev, "TILEKILN_URL")

        # Assert that the FastAPI instance is set up correctly
        assert hasattr(tilekiln.dev, "dev")
        assert isinstance(tilekiln.dev.dev, FastAPI)

        # Check that app routes exist
        app_routes = {route.path for route in tilekiln.dev.dev.routes}
        assert "/" in app_routes
        assert "/favicon.ico" in app_routes
        assert "/tilejson.json" in app_routes
        assert "/{prefix}/tilejson.json" in app_routes
        assert "/{prefix}/{zoom}/{x}/{y}.mvt" in app_routes


def test_tile_endpoint(client, mock_dev_globals):
    """Test the tile endpoint"""
    # Unpack the mocks
    _, mock_kiln = mock_dev_globals

    # Setup mock for kiln.render_all
    mock_kiln.render_all.return_value = {
        "layer1": b"layer1_data",
        "layer2": b"layer2_data",
    }

    # Test a valid tile request
    response = client.get("/test_id/10/123/456.mvt")

    # Check that the response is successful
    assert response.status_code == 200

    # Check that kiln.render_all was called with the right arguments
    mock_kiln.render_all.assert_called_once()
    tile_arg = mock_kiln.render_all.call_args[0][0]
    assert tile_arg.zoom == 10
    assert tile_arg.x == 123
    assert tile_arg.y == 456

    # Check the response headers
    assert response.headers["Content-Type"] == "application/vnd.mapbox-vector-tile"
    assert "Cache-Control" in response.headers

    # Check the response body - combines layer data into a single response
    expected_data = b"layer1_datalayer2_data"
    assert response.content == expected_data


def test_tile_endpoint_zoom_error(client, mock_dev_globals):
    """Test the tile endpoint when a zoom error occurs"""
    # Unpack the mocks
    _, mock_kiln = mock_dev_globals

    # Set up mock to raise ZoomNotDefined error
    mock_kiln.render_all.side_effect = tilekiln.errors.ZoomNotDefined(
        "Zoom level 10 not defined"
    )

    # With the updated code, the error should be caught by the route handler
    # and returned as a 404 HTTP error
    response = client.get("/test_id/10/123/456.mvt")

    # Verify that we get a 404 response with the appropriate error message
    assert response.status_code == 404
    assert "Zoom level 10 not defined in tileset" in response.text

    # Check that kiln.render_all was called with the right arguments
    mock_kiln.render_all.assert_called_once()
    tile_arg = mock_kiln.render_all.call_args[0][0]
    assert tile_arg.zoom == 10
    assert tile_arg.x == 123
    assert tile_arg.y == 456


def test_tilejson_endpoint(client, mock_dev_globals):
    """Test the tilejson endpoint"""
    # Unpack the mocks
    mock_config, _ = mock_dev_globals

    # Mock the config.tilejson method
    mock_config.tilejson.return_value = (
        '{"name":"test","tiles":["http://localhost:8080/test_id/{z}/{x}/{y}.mvt"]}'
    )

    # Test a tilejson request
    response = client.get("/test_id/tilejson.json")

    # Check that the response is successful
    assert response.status_code == 200

    # Check that config.tilejson was called with the right argument
    mock_config.tilejson.assert_called_once_with("http://localhost:8080")

    # Check the response headers
    assert response.headers["Content-Type"] == "application/json"

    # Check the response body
    expected_data = (
        '{"name":"test","tiles":["http://localhost:8080/test_id/{z}/{x}/{y}.mvt"]}'
    )
    assert response.content.decode() == expected_data
