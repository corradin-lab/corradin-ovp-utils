# AUTOGENERATED! DO NOT EDIT! File to edit: 02_datasets_OVPDataset.ipynb (unless otherwise specified).

__all__ = ['OVPDataSetError', 'parse_class', 'SingleFilePathSchema', 'MultipleFilePathSchema',
           'CaseControlFilePathSchema', 'OVPDataset']

# Cell
from typing import Any, Dict, List, Optional, Literal, Union
from enum import Enum
import numpy as np
from kedro.io import AbstractVersionedDataSet
from pydantic import BaseModel
from pydantic.dataclasses import dataclass
from dataclasses import InitVar, asdict

from kedro.io.core import (
    AbstractVersionedDataSet,
    DataSetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)
import fsspec
from copy import deepcopy
from pathlib import Path, PosixPath
from types import SimpleNamespace

# Cell
from . import genetic_file, sample_file

# Cell

#modified from kedro.io.core
from kedro.utils import load_obj

_DEFAULT_PACKAGES = ["kedro.io.", "kedro.extras.datasets.", "corradin_ovp_utils.datasets.", ""]

class OVPDataSetError(Exception):
    pass

def parse_class(key,
    class_obj:str
): #-> Tuple[Type[AbstractDataSet], Dict[str, Any]]:
    """Parse and instantiate a dataset class using the configuration provided.
    Args:
        config: Data set config dictionary. It *must* contain the `type` key
            with fully qualified class name.
        load_version: Version string to be used for ``load`` operation if
                the data set is versioned. Has no effect on the data set
                if versioning was not enabled.
        save_version: Version string to be used for ``save`` operation if
            the data set is versioned. Has no effect on the data set
            if versioning was not enabled.
    Raises:
        DataSetError: If the function fails to parse the configuration provided.
    Returns:
        2-tuple: (Dataset class object, configuration dictionary)
    """

    if isinstance(class_obj, str):
        if len(class_obj.strip(".")) != len(class_obj):
            raise OVPDataSetError(
                f"{key} class path does not support relative "
                "paths or paths ending with a dot."
            )

        class_paths = (prefix + class_obj for prefix in _DEFAULT_PACKAGES)

        trials = (_load_obj(class_path) for class_path in class_paths)
        try:
            class_obj = next(obj for obj in trials if obj is not None)
        except StopIteration as exc:
            raise OVPDataSetError(f"Class `{class_obj}` not found.") from exc

#     if not issubclass(class_obj, AbstractDataSet):
#         raise DataSetError(
#             f"DataSet type `{class_obj.__module__}.{class_obj.__qualname__}` "
#             f"is invalid: all data set types must extend `AbstractDataSet`."
#         )

    return class_obj


def _load_obj(class_path: str) -> Optional[object]:
    mod_path, _, class_name = class_path.rpartition(".")
    try:
        available_classes = load_obj(f"{mod_path}.__all__")
    # ModuleNotFoundError: When `load_obj` can't find `mod_path` (e.g `kedro.io.pandas`)
    #                      this is because we try a combination of all prefixes.
    # AttributeError: When `load_obj` manages to load `mod_path` but it doesn't have an
    #                 `__all__` attribute -- either because it's a custom or a kedro.io dataset
    except (ModuleNotFoundError, AttributeError, ValueError):
        available_classes = None

    try:
        class_obj = load_obj(class_path)
    except (ModuleNotFoundError, ValueError):
        return None
    except AttributeError as exc:
        if available_classes and class_name in available_classes:
            raise DataSetError(
                f"{exc} Please see the documentation on how to "
                f"install relevant dependencies for {class_path}:\n"
                f"https://kedro.readthedocs.io/en/stable/"
                f"04_kedro_project_setup/01_dependencies.html"
            ) from exc
        return None

    return class_obj

# Cell

class SingleFilePathSchema(BaseModel):
    folder: str
    full_file_name: str
    file_name: Optional[str] = None
    extension: Optional[str] = None
    split_by_chromosome: Optional[bool] = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        if self.file_name is None or self.extension is None:
            self.file_name, *_, self.extension = self.full_file_name.split(".")

    def get_full_file_path(self, chrom:Optional[int]=None):
        if self.split_by_chromosome and chrom is None:
            raise ValueError("Need chrom number")
        else:
            formatted_file_name = self.full_file_name.format(chrom_num=chrom)
            return (Path(self.folder)/formatted_file_name)

    @property
    def full_file_path(self):
        if self.split_by_chromosome:
            return {chrom_num: self.get_full_file_path(chrom = chrom_num) for chrom_num in range(1,23)}
        else:
            return self.get_full_file_path()

    @property
    def protocol_and_path(self):

        if self.split_by_chromosome:
            return [None]
        else:
            return get_protocol_and_path(self.full_file_path.as_posix())

    @property
    def protocol(self):
        return self.protocol_and_path[0]

    #validate full file name when split by chrom here
    #throw error when files doesn't have an extension

class MultipleFilePathSchema():
    def __getattr__(self, attr, **kwargs):
        return {
            k: getattr(v, attr) for k, v in self.to_dict().items()
        }

    def to_dict(self):
        return asdict(self)

    def apply_func(self, func, **kwargs):
        print(func)
        print(self.to_dict().items())
        return {
            k: func(v, **kwargs) for k, v in self.to_dict().items()
        }

@dataclass
class CaseControlFilePathSchema(MultipleFilePathSchema):
    case: SingleFilePathSchema
    control: SingleFilePathSchema
    common_folder : InitVar(Optional[str]) = None

    def __post_init__(self, common_folder):
        if common_folder is not None:
            self.case = SingleFilePathSchema(folder=common_folder, **self.case)
            self.control = SingleFilePathSchema(folder=common_folder, **self.control)

    @property
    def protocol(self):
        if self.case.protocol != self.control.protocol:
            raise ValueError(f"Currently only the same file system for case and control file is supported.\n Case is located in {self.case.protocol} system. Control is located in {self.control.protocol} ")
        return self.case.protocol


#     def __post_init_post_parse__(self, common_folder):
#         self.protocol, _ = self.case.protocol_and_path



# Cell
# class FILE_FORMAT_ENUM(Enum):
#     GenFile = genetic_datasets.GenFileFormat
#     SampleFile = sample_file.SampleFileFormat

# class FILE_TYPE_ENUM(Enum):
#     CC = CaseControlFilePathSchema
#     S = SingleFilePathSchema


# Cell

class OVPDataset(AbstractVersionedDataSet):
    def __init__(self,
                 file_type,
                 file_format,
                 file_path,
                 common_folder=None,
                load_args: Dict[str, Any] = None,
                version: Version = None,
                credentials: Dict[str, Any] = None,
                fs_args: Dict[str, Any] = None,
                ):

        self.file_type = file_type
        self._file_path_class = parse_class("file_type", file_type)
        self._file_path = self._file_path_class(**file_path) if common_folder is None else self._file_path_class(**file_path, common_folder = common_folder) #custom file path


        self._version = version

        self._file_format_class = parse_class("file_format", file_format)
        #self._file_format = self._file_format_class(**load_args)

        if self._file_path_class != SingleFilePathSchema:
            self.files = SimpleNamespace(**{single_file : self._file_format_class(filepath = single_file_path, **load_args)\
                                        for single_file, single_file_path in self.full_file_path.items()})#self._file_path.apply_func(self._file_format_class, **load_args)
        else:
            self.files = SimpleNamespace(**{"single_file" : self._file_format_class(filepath = self.full_file_path, **load_args)})

        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _credentials = deepcopy(credentials) or {}

        #protocol, path = get_protocol_and_path(filepath, version)
        if self._file_path.protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        _fs_open_args_save.setdefault("mode", "w")
        self._fs_open_args_load = _fs_open_args_load
        self._fs_open_args_save = _fs_open_args_save

        self._protocol = self._file_path.protocol
        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

#         self._protocol = protocol
#         self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)


    @property
    def full_file_path(self):
        return self._file_path.full_file_path

    def _load(self):
        return self

    def _save(self):
        pass

    def _describe(self):
        pass