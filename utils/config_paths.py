"""Small helpers for resolving paths from SynthFirm config files."""

import os
from pathlib import Path
from typing import Union


PathValue = Union[str, os.PathLike[str]]


def resolve_config_path(value: PathValue, config_file: PathValue) -> Path:
    """Resolve a path value from a SynthFirm configuration file.

    SynthFirm examples often use ``ENVIRONMENT.file_path`` as the root that
    contains ``inputs_<scenario>``, ``outputs_<scenario>``, plots, and the
    parameter directory. This helper keeps checked-in configs portable by
    allowing that value to be relative to the configuration file location.

    Absolute paths keep their normal meaning. Relative paths are interpreted
    from the directory containing ``config_file``, not from the shell's current
    working directory. User-home and environment-variable forms such as ``~``
    and ``$SYNTHFIRM_DATA_ROOT`` are expanded before the absolute/relative
    check.

    Parameters
    ----------
    value : str or os.PathLike
        Path text from the configuration file.
    config_file : str or os.PathLike
        Configuration file whose parent directory anchors relative paths.

    Returns
    -------
    pathlib.Path
        Absolute, resolved path suitable for joining with SynthFirm input,
        output, plot, and parameter subdirectories.
    """
    expanded = Path(os.path.expandvars(os.path.expanduser(os.fspath(value))))
    if expanded.is_absolute():
        return expanded.resolve()

    config_dir = Path(config_file).expanduser().resolve().parent
    return (config_dir / expanded).resolve()
