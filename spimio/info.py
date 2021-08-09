from .stream import json_load
from .dandi import DandiPath
from pathlib import Path
from os import PathLike
import h5py


def name_to_keys(filename):
    asdict = {}
    if not isinstance(filename, PathLike):
        filename = Path(filename)
    basename = filename.name
    attributes = basename.split('.')[0].split('_')
    for attr in attributes:
        key, *values = attr.split('-')
        if not values:
            asdict[key] = None
        else:
            asdict[key] = '-'.join(values)
    return asdict


def chunk_info(path):
    """Get info on a chunk

    :param path: path_like
    :return: {SampleStaining, SlabIndex, Subject, PixelSize, Shape, Shapes}
    """
    info = dict()
    if not isinstance(path, PathLike):
        path = Path(path)
    if path.suffix not in ('.h5', '.json'):
        raise ValueError('Expected h5 or json file')
    if path.suffix == '.h5':
        h5path = path
        jsonpath = path.__class__(path.parent, path.stem + '.json')
        if isinstance(path, DandiPath):
            jsonpath.remote = path.dandiset
    else:  # path.suffix == '.json'
        jsonpath = path
        h5path = path.__class__(path.parent, path.stem + '.h5')
        if isinstance(path, DandiPath):
            h5path.remote = path.dandiset
    basepath = path.stem.split('_')[:-1]
    basepath = '_'.join(basepath)
    trfpath = jsonpath.__class__(path.parent, basepath + '_transforms.json')
    if isinstance(path, DandiPath):
        trfpath.remote = path.dandiset
    if jsonpath.exists():
        metadata = json_load(jsonpath)
        info['PixelSize'] = metadata.get('PixelSize', None)
        info['SampleStaining'] = metadata.get('SampleStaining', None)
        metadata = name_to_keys(jsonpath)
        info['Subject'] = metadata['sub']
        info['SlabIndex'] = metadata['sample']
        info['Chunk'] = metadata['chunk']
    if h5path.exists():
        if isinstance(h5path, DandiPath):
            f = h5py.File(h5path.s3_url, driver='ros3')
        else:
            f = h5py.File(h5path)
        nb_levels = len(f.keys())
        info['Shape'] = f['0'].shape
        info['Shapes'] = [f[f'{i}'].shape for i in range(nb_levels)]
        info['DataType'] = f['0'].dtype
        info['DataTypes'] = [f[f'{i}'].dtype for i in range(nb_levels)]
        metadata = name_to_keys(jsonpath)
        info['Subject'] = metadata['sub']
        info['SlabIndex'] = metadata['sample']
        info['Chunk'] = metadata['chunk']
    if trfpath.exists():
        metadata = json_load(trfpath)[0]
        metadata = metadata.get('TransformationParameters', {})
        info['Shift'] = [metadata.get('XOffset', 0.0),
                         metadata.get('YOffset', 0.0),
                         metadata.get('ZOffset', 0.0)]
    return info


def slab_info(path):
    """Get info on a slab.
    This assumes KC's bids-like organization: `path` is a folder that
    contains fata for a single slab and a single subject.

    :param path: path_like
    :return: {Stainings, Chunks, SlabIndex, Subject, PixelSize, Shape}
    """

    info = dict()
    info['Stainings'] = []
    info['Chunks'] = []
    info['SlabIndex'] = None
    info['Subject'] = None
    info['PixelSize'] = None
    info['Shape'] = None

    if not isinstance(path, PathLike):
        path = Path(path)
    h5files = path.glob('*.h5')
    for h5file in h5files:
        file_info = chunk_info(h5file)
        if info['Subject'] and file_info['Subject'] != info['Subject']:
            raise ValueError('Several subjects in the same folder')
        info['Subject'] = file_info['Subject']
        if info['SlabIndex'] and file_info['SlabIndex'] != info['SlabIndex']:
            raise ValueError('Several slabs in the same folder')
        info['SlabIndex'] = file_info['SlabIndex']
        if info['PixelSize'] and file_info['PixelSize'] != info['PixelSize']:
            raise ValueError('Pixel size not consistent')
        info['PixelSize'] = file_info['PixelSize']
        if info['Shape'] and file_info['Shape'] != info['Shape']:
            raise ValueError('Shape not consistent')
        info['Shape'] = file_info['Shape']
        # nb_levels = len(file_info['Shapes'])
        # if info['NbLevels'] and nb_levels != info['NbLevels']:
        #     raise ValueError('Number of levels not consistent')
        # info['NbLevels'] = nb_levels
        info['Stainings'].append(file_info['SampleStaining'])
        info['Chunks'].append(file_info['Chunk'])

    info['Stainings'] = set(info['Stainings'])
    info['Chunks'] = set(info['Chunks'])
    return info





