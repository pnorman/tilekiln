import jinja2 as j2


DEFAULT_EXTENT = 4096
DEFAULT_BUFFER = 0


j2Environment = j2.Environment(loader=j2.BaseLoader())


class Definition:
    def __init__(self, definition_yaml, filesystem):
        self.minzoom = definition_yaml["minzoom"]
        self.maxzoom = definition_yaml["maxzoom"]
        self.extent = definition_yaml.get("extent", DEFAULT_EXTENT)
        self.buffer = definition_yaml.get("buffer", DEFAULT_BUFFER)

        self.__template = j2Environment.from_string(
                            filesystem.readtext(definition_yaml["file"]))
