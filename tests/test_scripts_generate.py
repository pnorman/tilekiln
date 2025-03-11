from unittest import TestCase
from unittest.mock import patch, Mock
from click.testing import CliRunner

from tilekiln.scripts.generate import tiles, zooms


class TestScriptsGenerate(TestCase):
    def setUp(self):
        self.runner = CliRunner()
        self.mock_config = Mock()

    @patch('tilekiln.load_config')
    @patch('tilekiln.generator.generate')
    def test_tiles_no_layers(self, mock_generate, mock_load_config):
        """Test tiles command with no layer option"""
        mock_load_config.return_value = self.mock_config

        # Run the tiles command without layer option
        with self.runner.isolated_filesystem():
            with open('config.yaml', 'w') as f:
                f.write('test config')

            # Create a tiles.txt file for stdin
            with open('tiles.txt', 'w') as f:
                f.write('0/0/0\n1/0/0')

            result = self.runner.invoke(tiles, ['--config', 'config.yaml'],
                                        input='0/0/0\n1/0/0')

        # Verify command completes successfully
        self.assertEqual(result.exit_code, 0)

        # Check that generate was called with empty layer tuple
        mock_generate.assert_called_once()
        # The last arg should be an empty tuple for layers
        self.assertEqual(mock_generate.call_args[0][5], ())

    @patch('tilekiln.load_config')
    @patch('tilekiln.generator.generate')
    def test_tiles_single_layer(self, mock_generate, mock_load_config):
        """Test tiles command with single layer option"""
        mock_load_config.return_value = self.mock_config

        # Run the tiles command with one layer
        with self.runner.isolated_filesystem():
            with open('config.yaml', 'w') as f:
                f.write('test config')

            result = self.runner.invoke(tiles, [
                '--config', 'config.yaml',
                '--layer', 'layer1'
            ], input='0/0/0')

        # Verify command completes successfully
        self.assertEqual(result.exit_code, 0)

        # Check that generate was called with the layer
        mock_generate.assert_called_once()
        self.assertEqual(mock_generate.call_args[0][5], ('layer1',))

    @patch('tilekiln.load_config')
    @patch('tilekiln.generator.generate')
    def test_tiles_multiple_layers(self, mock_generate, mock_load_config):
        """Test tiles command with multiple layer options"""
        mock_load_config.return_value = self.mock_config

        # Run the tiles command with multiple layers
        with self.runner.isolated_filesystem():
            with open('config.yaml', 'w') as f:
                f.write('test config')

            result = self.runner.invoke(tiles, [
                '--config', 'config.yaml',
                '--layer', 'layer1',
                '--layer', 'layer2'
            ], input='0/0/0')

        # Verify command completes successfully
        self.assertEqual(result.exit_code, 0)

        # Check that generate was called with both layers
        mock_generate.assert_called_once()
        self.assertEqual(mock_generate.call_args[0][5], ('layer1', 'layer2'))

    @patch('tilekiln.load_config')
    @patch('tilekiln.generator.generate')
    def test_zooms_no_layers(self, mock_generate, mock_load_config):
        """Test zooms command with no layer option"""
        mock_load_config.return_value = self.mock_config

        # Run the zooms command without layer option
        with self.runner.isolated_filesystem():
            with open('config.yaml', 'w') as f:
                f.write('test config')

            result = self.runner.invoke(zooms, [
                '--config', 'config.yaml',
                '--min-zoom', '0',
                '--max-zoom', '1'
            ])

        # Verify command completes successfully
        self.assertEqual(result.exit_code, 0)

        # Check that generate was called with empty layer tuple
        mock_generate.assert_called_once()
        self.assertEqual(mock_generate.call_args[0][5], ())

    @patch('tilekiln.load_config')
    @patch('tilekiln.generator.generate')
    def test_zooms_multiple_layers(self, mock_generate, mock_load_config):
        """Test zooms command with multiple layer options"""
        mock_load_config.return_value = self.mock_config

        # Run the zooms command with multiple layers
        with self.runner.isolated_filesystem():
            with open('config.yaml', 'w') as f:
                f.write('test config')

            result = self.runner.invoke(zooms, [
                '--config', 'config.yaml',
                '--min-zoom', '0',
                '--max-zoom', '1',
                '--layer', 'layer1',
                '--layer', 'layer3'
            ])

        # Verify command completes successfully
        self.assertEqual(result.exit_code, 0)

        # Check that generate was called with both layers
        mock_generate.assert_called_once()
        self.assertEqual(mock_generate.call_args[0][5], ('layer1', 'layer3'))
