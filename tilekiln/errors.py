'''
Custom exceptions used by tilekiln

Exception (base)
|_ Error
   |_ConfigError
   |_RuntimeError
'''


class Error(Exception):
    pass


class ConfigError(Error):
    pass


class ConfigYAMLError(ConfigError):
    ''' Errors where YAML is invalid, missing, or types are wrong'''
    pass


class ConfigLayerError(ConfigError):
    pass


class DefinitionError(ConfigLayerError):
    pass


class RuntimeError(Error):
    pass


class ZoomNotDefined(RuntimeError):
    pass
