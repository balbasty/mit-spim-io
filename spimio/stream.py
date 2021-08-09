import os
import urllib
import json
import h5py
from .dandi import DandiPath


def json_load(path):
    """Load a JSON file, eventually from a remote location.
    :param path: <path_like or file_like> Local or remote path, or file descriptor.
    :return: <dict or list> Loaded JSON object.
    """
    if isinstance(path, DandiPath):
        path = path.download_url
    if isinstance(path, os.PathLike):
        path = str(path)
    if isinstance(path, str) and path.startswith(('http://', 'https://')):
        # remote path
        with urllib.request.urlopen(path) as f:
            return json.load(f)
    elif isinstance(path, str):
        # local path
        with open(path) as f:
            return json.load(f)
    else:
        # file descriptor
        return json.load(path)


def h5_map(path):
    """Map a h5 file, eventually from a remote location.
    :param path: <path_like or file_like> Local or remote path, or file descriptor.
    :return: <h5py.File> Loaded JSON object.
    """
    if isinstance(path, DandiPath):
        path = path.s3_url
        return h5py.File(path, driver='ros3')
    if isinstance(path, os.PathLike):
        path = str(path)
    return h5py.File(path)
