import os

import fs.osfs

import tilekiln.config


# TODO: Put somewhere else
def load_config(path) -> tilekiln.config.Config:
    '''Loads a config from the filesystem, given a path'''

    full_path = os.path.join(os.getcwd(), path)
    root_path = os.path.dirname(full_path)
    config_path = os.path.relpath(full_path, root_path)
    filesystem = fs.osfs.OSFS(root_path)

    return tilekiln.config.Config(filesystem.open(config_path).read(),
                                  filesystem)
