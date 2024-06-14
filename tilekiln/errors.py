'''
Custom exceptions used by tilekiln

Exception
|_ Error
   |_ConfigError
'''


class Error(Exception):
    pass


class ConfigError(Error):
    pass


class ConfigYAMLError(ConfigError):
    ''' Errors where YAML is invalid, missing, or types are wrong'''
    pass
