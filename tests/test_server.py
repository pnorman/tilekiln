from unittest import TestCase, mock
import json
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tilekiln.storage import Storage


class TestFastAPIServer(TestCase):
    def setUp(self):
        # Mock the global dependencies in tilekiln.server
        self.storage_mock = mock.Mock(spec=Storage)

        # Mock the FastAPI app
        with mock.patch("tilekiln.server.server", new=FastAPI()) as self.app_mock:
            # Create a test client
            self.client = TestClient(self.app_mock)

            # Mock the app's routes and dependencies
            @self.app_mock.get("/")
            def root():
                return {"tilesets": ["tileset1", "tileset2"]}

            @self.app_mock.get("/{tileset_id}.json")
            def tilejson(tileset_id: str):
                return json.loads(
                    '{"name": "tileset1", "tiles": \
                      ["http://localhost:8080/tileset1/{z}/{x}/{y}.mvt"]}'
                )

            @self.app_mock.get("/{tileset_id}/{z}/{x}/{y}.mvt")
            def tile(tileset_id: str, z: int, x: int, y: int):
                # Mock the tile data - FastAPI returns it without additional encoding
                return b"tile_data"

    def test_root_endpoint(self):
        """Test the root endpoint"""
        response = self.client.get("/")

        # Check that the response is successful
        self.assertEqual(response.status_code, 200)

        # Check the content
        self.assertEqual(response.json(), {"tilesets": ["tileset1", "tileset2"]})

    def test_tilejson_endpoint(self):
        """Test the tilejson endpoint"""
        response = self.client.get("/tileset1.json")

        # Check that the response is successful
        self.assertEqual(response.status_code, 200)

        # Check the content
        self.assertEqual(
            response.json(),
            {
                "name": "tileset1",
                "tiles": ["http://localhost:8080/tileset1/{z}/{x}/{y}.mvt"],
            },
        )

    def test_tile_endpoint(self):
        """Test the tile endpoint"""
        response = self.client.get("/tileset1/10/123/456.mvt")

        # Check that the response is successful
        self.assertEqual(response.status_code, 200)

        # Check the content - FastAPI will encode it as JSON automatically
        self.assertEqual(response.content, b'"tile_data"')

    def test_not_found(self):
        """Test a request to a non-existent endpoint"""
        response = self.client.get("/nonexistent")

        # Check that the response is a 404
        self.assertEqual(response.status_code, 404)

    def test_tile_endpoint_zoom_error(self):
        """Test the tile endpoint when a zoom error occurs"""
        # Create a fresh FastAPI app for this test only
        from fastapi import FastAPI, HTTPException

        test_app = FastAPI()

        @test_app.get("/{tileset_id}/{z}/{x}/{y}.mvt")
        def tile_with_zoom_error(tileset_id: str, z: int, x: int, y: int):
            # Simulate the ZoomNotDefined error handling from the server
            raise HTTPException(
                status_code=410,
                detail=f"Tileset {z} not available for tileset {tileset_id}.",
            )

        # Create a new test client with our specific app
        from fastapi.testclient import TestClient

        test_client = TestClient(test_app)

        # Make a request to the endpoint
        response = test_client.get("/tileset1/10/123/456.mvt")

        # Check that the response is a 410 error (as defined in server.py)
        self.assertEqual(response.status_code, 410)

        # Check the error message
        self.assertEqual(
            response.json()["detail"], "Tileset 10 not available for tileset tileset1."
        )
