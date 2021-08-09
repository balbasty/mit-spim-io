import pathlib
import copy
import requests
import os
import functools
from collections import Sequence
from dandi.dandiapi import DandiAPIClient
from dandi.exceptions import NotFoundError


def _make_uri_dandi(path, dandiset_id, version_id=None):
    path = str(path)
    dandiset = f'dandi://dandiset/{dandiset_id}/'
    if version_id:
        path = dandiset + f'{version_id}/' + path
    else:
        path = dandiset + path
    return bytes(path, 'utf8')


def _notimplemented(*args, **kwargs):
    raise NotImplementedError()


def _get_s3_url(url):
    url = requests.request(url=url, method='head').url
    if '?' in url:
        return url[:url.index('?')]
    return url


#
# Globbing helpers
#


def _is_wildcard_pattern(pat):
    # Whether this pattern needs actual matching using fnmatch, or can
    # be looked up directly as a file.
    return "*" in pat or "?" in pat or "[" in pat


def _make_selector(pattern_path):
    pat = pattern_path.parts[0]
    if isinstance(pattern_path, RemotePath):
        pattern_path = RemotePath.__new__(pattern_path.__class__, *pattern_path.parts[1:], remote=pattern_path.remote)
    else:
        pattern_path = pattern_path.__class__(*pattern_path.parts[1:])
    if pat == '**':
        cls = _RecursiveWildcardSelector
    elif '**' in pat:
        raise ValueError("Invalid pattern: '**' can only be an entire path component")
    elif _is_wildcard_pattern(pat):
        cls = _WildcardSelector
    else:
        cls = _PreciseSelector
    return cls(pat, pattern_path)


if hasattr(functools, "lru_cache"):
    _make_selector = functools.lru_cache()(_make_selector)


class _Selector:
    """A selector matches a specific glob pattern part against the children
    of a given path."""

    def __init__(self, pattern_path):
        self.pattern_path = pattern_path
        if pattern_path.parts:
            self.successor = _make_selector(pattern_path)
            self.dironly = True
        else:
            self.successor = _TerminatingSelector()
            self.dironly = False

    def select_from(self, parent_path):
        """Iterate over all child paths of `parent_path` matched by this
        selector.  This can contain parent_path itself."""
        if not parent_path.is_dir():
            return iter([])
        return self._select_from(parent_path)


class _TerminatingSelector:

    def _select_from(self, parent_path):
        yield parent_path


class _PreciseSelector(_Selector):

    def __init__(self, name, pattern_path):
        self.name = name
        _Selector.__init__(self, pattern_path)

    def _select_from(self, parent_path):
        try:
            path = parent_path._make_child_relpath(self.name)
            if path.is_dir() if self.dironly else path.exists():
                for p in self.successor._select_from(path):
                    yield p
        except PermissionError:
            return


class _WildcardSelector(_Selector):

    def __init__(self, pat, pattern_path):
        self.match = pattern_path._flavour.compile_pattern(pat)
        _Selector.__init__(self, pattern_path)

    def _select_from(self, parent_path):
        try:
            entries = list(parent_path.scandir())
            for entry in entries:
                if self.dironly:
                    if not entry.is_dir():
                        continue
                name = entry.name
                if self.match(name):
                    path = parent_path._make_child_relpath(name)
                    for p in self.successor._select_from(path):
                        yield p
        except PermissionError:
            return


class _RecursiveWildcardSelector(_Selector):

    def __init__(self, pat, pattern_path):
        _Selector.__init__(self, pattern_path)

    def _iterate_directories(self, parent_path):
        yield parent_path
        try:
            entries = list(parent_path.scandir())
            for entry in entries:
                entry_is_dir = entry.is_dir()
                if entry_is_dir:  # removed bit about symlinks since no symlinks on remotes
                    path = parent_path._make_child_relpath(entry.name)
                    for p in self._iterate_directories(path):
                        yield p
        except PermissionError:
            return

    def _select_from(self, parent_path):
        try:
            yielded = set()
            try:
                successor_select = self.successor._select_from
                for starting_point in self._iterate_directories(parent_path):
                    for p in successor_select(starting_point):
                        if p not in yielded:
                            yield p
                            yielded.add(p)
            finally:
                yielded.clear()
        except PermissionError:
            return


# Path objects for remote file systems.
# There's no difference between Pure and Concrete paths anymore since
# we do not depend on the OS.
#
# I am assuming that the remote file system is Posix-like.
# In reality, it's not really a file system and there's no such thing as
# folders, but the file id convention uses a folder-like structure so
# we can mimic a file system. What it means is that a folder can "exist"
# only if it contains at least one file down its hierarchy. There is no such
# thing as an empty folder. At least that's the case for Dandi...

class _RemotePathParents(Sequence):
    """This object provides sequence-like access to the logical ancestors
    of a path.  Don't try to construct it yourself."""
    __slots__ = ('_pathcls', '_drv', '_root', '_parts')

    def __init__(self, path, remote=None):
        # We don't store the instance to avoid reference cycles
        self._pathcls = type(path)
        self._drv = path._drv
        self._root = path._root
        self._parts = path._parts
        self._remote = path.remote

    def __len__(self):
        if self._drv or self._root:
            return len(self._parts) - 1
        else:
            return len(self._parts)

    def __getitem__(self, idx):
        if idx < 0 or idx >= len(self):
            raise IndexError(idx)
        return self._pathcls._from_parsed_parts(self._drv, self._root,
                                                self._parts[:-idx - 1],
                                                remote=self._remote)

    def __repr__(self):
        return "<{}.parents>".format(self._pathcls.__name__)


class RemotePath(pathlib.PurePosixPath):
    """Path into a remote file system"""

    _flavour = copy.deepcopy(pathlib.PurePosixPath._flavour)
    _flavour.is_supported = True
    _flavour.make_uri = _notimplemented
    _flavour.gethomedir = _notimplemented

    def __new__(cls, *args, remote=None):
        self = pathlib.PurePosixPath.__new__(DandiPath, *args)
        self.remote = remote
        return self

    # I have to reimplement all the low-level constructors so that we
    # can transfer the "remote" (e.g., dandiset object) to the new
    # RemotePath object. Everything's just blatantly copied from pathlib.

    @classmethod
    def _parse_args_with_remote(cls, args):
        # This is useful when you don't want to create an instance, just
        # canonicalize some constructor arguments.
        remote = None
        parts = []
        for a in args:
            if isinstance(a, pathlib.PurePath):
                parts += a._parts
                if isinstance(a, RemotePath):
                    remote = remote or a.remote
            else:
                a = os.fspath(a)
                if isinstance(a, str):
                    # Force-cast str subclasses to str (issue #21127)
                    parts.append(str(a))
                else:
                    raise TypeError(
                        "argument should be a str object or an os.PathLike "
                        "object returning str, not %r"
                        % type(a))
        return (remote, *cls._flavour.parse_parts(parts))

    @classmethod
    def _from_parts(cls, args, init=True, remote=None):
        # We need to call _parse_args on the instance, so as to get the
        # right flavour.
        self = object.__new__(cls)
        rem, drv, root, parts = self._parse_args_with_remote(args)
        self._drv = drv
        self._root = root
        self._parts = parts
        self.remote = remote or rem
        if init:
            self._init()
        return self

    @classmethod
    def _from_parsed_parts(cls, drv, root, parts, init=True, remote=None):
        self = object.__new__(cls)
        self._drv = drv
        self._root = root
        self._parts = parts
        self.remote = remote
        if init:
            self._init()
        return self

    def _make_child(self, args):
        rem, drv, root, parts = self._parse_args_with_remote(args)
        drv, root, parts = self._flavour.join_parsed_parts(
            self._drv, self._root, self._parts, drv, root, parts)
        return self._from_parsed_parts(drv, root, parts, remote=self.remote or rem)

    def _make_child_relpath(self, part):
        # This is an optimization used for dir walking.  `part` must be
        # a single part relative to this path.
        parts = self._parts + [part]
        return self._from_parsed_parts(self._drv, self._root, parts, remote=self.remote)

    def with_name(self, name):
        """Return a new path with the file name changed."""
        if not self.name:
            raise ValueError("%r has an empty name" % (self,))
        drv, root, parts = self._flavour.parse_parts((name,))
        if (not name or name[-1] in [self._flavour.sep, self._flavour.altsep]
            or drv or root or len(parts) != 1):
            raise ValueError("Invalid name %r" % (name))
        return self._from_parsed_parts(self._drv, self._root,
                                       self._parts[:-1] + [name],
                                       remote=self.remote)

    def with_suffix(self, suffix):
        """Return a new path with the file suffix changed.  If the path
        has no suffix, add given suffix.  If the given suffix is an empty
        string, remove the suffix from the path.
        """
        f = self._flavour
        if f.sep in suffix or f.altsep and f.altsep in suffix:
            raise ValueError("Invalid suffix %r" % (suffix,))
        if suffix and not suffix.startswith('.') or suffix == '.':
            raise ValueError("Invalid suffix %r" % (suffix))
        name = self.name
        if not name:
            raise ValueError("%r has an empty name" % (self,))
        old_suffix = self.suffix
        if not old_suffix:
            name = name + suffix
        else:
            name = name[:-len(old_suffix)] + suffix
        return self._from_parsed_parts(self._drv, self._root,
                                       self._parts[:-1] + [name],
                                       remote=self.remote)

    def relative_to(self, *other):
        """Return the relative path to another path identified by the passed
        arguments.  If the operation is not possible (because this is not
        a subpath of the other path), raise ValueError.
        """
        # For the purpose of this method, drive and root are considered
        # separate parts, i.e.:
        #   Path('c:/').relative_to('c:')  gives Path('/')
        #   Path('c:/').relative_to('/')   raise ValueError
        if not other:
            raise TypeError("need at least one argument")
        parts = self._parts
        drv = self._drv
        root = self._root
        if root:
            abs_parts = [drv, root] + parts[1:]
        else:
            abs_parts = parts
        to_drv, to_root, to_parts = self._parse_args(other)
        if to_root:
            to_abs_parts = [to_drv, to_root] + to_parts[1:]
        else:
            to_abs_parts = to_parts
        n = len(to_abs_parts)
        cf = self._flavour.casefold_parts
        if (root or drv) if n == 0 else cf(abs_parts[:n]) != cf(to_abs_parts):
            formatted = self._format_parsed_parts(to_drv, to_root, to_parts)
            raise ValueError("{!r} does not start with {!r}"
                             .format(str(self), str(formatted)))
        return self._from_parsed_parts('', root if n == 1 else '',
                                       abs_parts[n:], remote=self.remote)

    def __rtruediv__(self, key):
        try:
            return self._from_parts([key] + self._parts, remote=self.remote)
        except TypeError:
            return NotImplemented

    @property
    def parent(self):
        """The logical parent of the path."""
        drv = self._drv
        root = self._root
        parts = self._parts
        if len(parts) == 1 and (drv or root):
            return self
        return self._from_parsed_parts(drv, root, parts[:-1], remote=self.remote)

    @property
    def parents(self):
        """A sequence of this path's logical parents."""
        return _RemotePathParents(self)

    def glob(self, pattern):
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        """
        if not pattern:
            raise ValueError("Unacceptable pattern: {!r}".format(pattern))
        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))
        if drv or root:
            raise NotImplementedError("Non-relative patterns are unsupported")
        if not isinstance(pattern, os.PathLike):
            pattern = RemotePath.__new__(self.__class__, pattern, remote=self.remote)
        selector = _make_selector(pattern)
        for p in selector.select_from(self):
            yield p

    def rglob(self, pattern):
        """Recursively yield all existing files (of any kind, including
        directories) matching the given relative pattern, anywhere in
        this subtree.
        """
        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))
        if drv or root:
            raise NotImplementedError("Non-relative patterns are unsupported")
        if not isinstance(pattern, os.PathLike):
            pattern = RemotePath.__new__(self.__class__, pattern, remote=self.remote)
        selector = _make_selector(self.__class__("**").joinpath(pattern))
        for p in selector.select_from(self):
            yield p


class DandiPath(RemotePath):
    """Path into a remote dandiset"""

    _flavour = copy.deepcopy(RemotePath._flavour)
    _flavour.make_uri = _make_uri_dandi

    def __new__(cls, *args, dandiset_id=None, version_id=None, client=None, dandiset=None):
        """
        p = DandiPath(<PathLike or str>, [dandiset=<RemoteDandiset>,])
        p = DandiPath(<PathLike or str>, [dandiset_id=<int or str>,] [version_id=<str>,] [client=<DandiAPIClient>,])
        """
        if dandiset:
            if dandiset_id or version_id or client:
                raise ValueError('If `dandiset` is used, it must be the only keyword argument')
            dandiset = dandiset
        elif dandiset_id is None:
            dandiset = None
        else:
            if not isinstance(client, DandiAPIClient):
                client = DandiAPIClient(client)
            if isinstance(dandiset_id, int):
                dandiset_id = f'{dandiset_id:06d}'
            dandiset = client.get_dandiset(dandiset_id, version_id)
        self = RemotePath.__new__(DandiPath, *args, remote=dandiset)
        return self

    # Dandi-specific attributes and methods

    @property
    def dandiset(self):
        """
        Return the dandiset (through dandi API) of this path.
        """
        return self.remote

    @property
    def dandiset_id(self):
        """
        Return the dandiset id of this path.
        """
        return self.dandiset.identifier if self.dandiset else None

    @property
    def version_id(self):
        """
        Return the version id of this path.
        """
        return self.dandiset.version_id if self.dandiset else None

    def as_asset(self):
        """
        Return a file's RemoteAsset object using the dandi API.
        """
        if not self.is_file():
            raise ValueError('Not a file')
        return self.dandiset.get_asset_by_path(str(self))

    @property
    def download_url(self):
        """
        Get URL that allows the remote file to be downloaded.
        """
        return self.as_asset().download_url

    @property
    def s3_url(self):
        """
        Get AWS S3 URL that allows the remote file to be downloaded.
        """
        return _get_s3_url(self.download_url)

    # Overload Path methods

    def as_uri(self):
        """Return the path as a dandiarchive URL."""
        if not self.dandiset:
            raise ValueError('Missing dandiset')
        return self._flavour.make_uri(self, self.dandiset_id, self.version_id)

    def exists(self):
        """
        Whether this path exists.
        """
        if not self.dandiset:
            return False
        for asset in self.dandiset.get_assets_with_path_prefix(str(self)):
            asset_path = DandiPath(asset.path, client=self.dandiset.client)
            if asset_path == self:
                return True  # file
            elif asset_path.is_relative_to(self):
                return True  # dir
        return False

    def is_dir(self):
        """
        Whether this path is a directory.
        """
        if not self.dandiset:
            return False
        path = str(self)
        for asset in self.dandiset.get_assets_with_path_prefix(path):
            if DandiPath(asset.path).is_relative_to(self):
                return True
        return False

    def is_file(self):
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        try:
            self.dandiset.get_asset_by_path(str(self))
            return True
        except NotFoundError:
            return False

    def is_relative_to(self, *other):
        """
        Whether this path is a subpath of another path.
        """
        try:
            rel = self.relative_to(*other)
            return rel != self.__class__('.')
        except ValueError:
            return False

    def iterdir(self):
        """
        Iterate over the files and subdirectories in this directory.
        Yields `str` objects.
        """
        if not self.is_dir():
            raise NotADirectoryError

        subdirs = []
        for asset in self.dandiset.get_assets_with_path_prefix(str(self)):
            asset_path = DandiPath(asset.path)
            if self == asset_path.parent:  # file
                yield asset_path.parts[-1]
            elif asset_path.is_relative_to(self):  # subdir
                base = asset_path.relative_to(self).parts[0]
                if base not in subdirs:
                    subdirs.append(base)
                    yield base

    def scandir(self):
        """
        Iterate over the files and subdirectories in this directory.
        Yields `DandiPath` objects.
        """
        if not self.is_dir():
            raise NotADirectoryError

        subdirs = []
        for asset in self.dandiset.get_assets_with_path_prefix(str(self)):
            asset_path = DandiPath(asset.path, dandiset=self.dandiset)
            if self == asset_path.parent:  # file
                yield asset_path
            elif asset_path.is_relative_to(self):  # subdir
                base = asset_path.relative_to(self).parts[0]
                base = self.joinpath(base)
                if base not in subdirs:
                    subdirs.append(base)
                    yield base

    def _print(self, pad=''):
        print(pad + '-', self.name)
        pad = pad + '  |'
        if self.is_dir():
            for child in self.scandir():
                child._print(pad=pad)

    def tree(self):
        self._print()
