import tilekiln.config
import fs.osfs

import os


def load_config(path):
    '''Loads a config from the filesystem, given a path'''

    full_path = os.path.join(os.getcwd(), path)
    root_path = os.path.dirname(full_path)
    config_path = os.path.relpath(full_path, root_path)
    filesystem = fs.osfs.OSFS(root_path)

    return tilekiln.config.Config(filesystem.open(config_path).read(),
                                  filesystem)
