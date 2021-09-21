import numpy as np
from scipy.ndimage import map_coordinates
import math
from pathlib import Path
from os import PathLike
from .spiminfo import slab_info
from .streamio import h5_map


def slab_recon(path, level=0):

    if not isinstance(path, dict):
        info = slab_info(path)
    else:
        info = path

    # allocate output
    outshape = np.asarray(info['FOV'], dtype='float')
    outshape = np.ceil(outshape / (2 ** level)).astype('int')
    stains = list(info['Stainings'])
    nb_stains = len(stains)
    out = np.zeros([nb_stains, *outshape], dtype='float32')

    for chunk in info['MetaChunks']:

        idx = stains.index(chunk['SampleStaining'])
        f = h5_map(chunk['Path']).get(str(level))
        d = np.asarray(f, dtype='float32')
        d = d.reshape(d.shape[-3:])

        off = [0, 0, 0]
        if 'Shift' in chunk:
            off = chunk['Shift']
        off = [x / (2 ** level) for x in off]
        
        slicer = [slice(math.floor(o+0.5), math.floor(s+o-0.5))
                  for o, s in zip(off, d.shape)]
        view = out[(idx, *slicer)]

        grid = [np.arange(s, dtype='float32').__iadd__(o-math.floor(o+0.5))
                for o, s in zip(off, view.shape[-3:])]
        grid = np.stack(np.meshgrid(*grid, indexing='ij', copy=False))
        view[...] = map_coordinates(d, grid.reshape([3, -1]), order=1).reshape(view.shape)

    return out


def _get_all_slabs(path, level=0, proj=None):
    if isinstance(path, (PathLike, str)):
        if not isinstance(path, PathLike):
            path = Path(path)
        path = sorted(path.glob('*/microscopy/'))

    slabs = []
    slab_indices = []
    for slab in path:

        if not isinstance(slab, dict):
            slab = slab_info(slab)
        slab_indices.append(slab['SlabIndex'])
        print('slab', slab['SlabIndex'])

        slab1 = slab_recon(slab, level=level)
        if proj == 'max':
            slab1 = slab1.max(axis=1, keepdims=True)
        elif proj == 'mean':
            slab1 = slab1.mean(axis=1, keepdims=True)
        elif proj == 'median':
            slab1 = np.median(slab1, axis=1, keepdims=True)
        slabs.append(slab1)
    
    return slabs, slab_indices


def _stack_all_slabs(slabs, indices):
    
    min_index = min(indices)
    max_index = max(indices)
    nb_indices = max_index - min_index + 1

    max_shape = [0, 0]
    for slab in slabs:
        max_shape = [max(x, s) for x, s in zip(max_shape, slab.shape[-2:])]

    batch, depth = slabs[0].shape[:2]
    out = np.zeros([batch, nb_indices*depth, *max_shape])
    for idx, slab in zip(indices, slabs):
        idx = idx - min_index
        slicer = tuple(slice(s) for s in slab.shape[-2:])
        slicer = (slice(idx*depth, (idx+1)*depth), *slicer)
        out[(Ellipsis, *slicer)] = slab
    return out
    

def all_slabs_recon(path, level=0, proj=None):
    slabs, slab_indices = _get_all_slabs(path, level, proj)
    return _stack_all_slabs(slabs, slab_indices)
