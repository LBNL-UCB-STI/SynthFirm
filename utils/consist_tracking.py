"""Consist tracking helpers for the SynthFirm teaching slice.

This module keeps the Consist-specific declarations out of ``SynthFirm_run.py``
so the run script can stay focused on model orchestration. The helpers define
where Consist stores provenance, which files each teaching-slice step declares
as inputs and outputs, and which multi-file directories should be recorded as
Consist ``OutputSet`` artifacts.
"""

import os
from dataclasses import replace
from pathlib import Path
from typing import Any

from consist import CacheOptions, ExecutionOptions, OutputSet, Tracker


def _as_path(value: str | os.PathLike[str] | Path) -> Path:
    """Return ``value`` as a ``Path`` without changing existing ``Path`` objects.

    Parameters
    ----------
    value
        Path-like value supplied by the run script or tests.

    Returns
    -------
    pathlib.Path
        The normalized path object.
    """
    return value if isinstance(value, Path) else Path(value)


def _env_path(name: str) -> Path | None:
    """Read an optional path override from an environment variable.

    Parameters
    ----------
    name
        Environment variable name to inspect.

    Returns
    -------
    pathlib.Path or None
        The override path when the variable is set to a non-empty value;
        otherwise ``None``.
    """
    raw = os.environ.get(name)
    if raw:
        return Path(raw)
    return None


def _nonempty(value: Any) -> bool:
    """Return whether a config-derived value is meaningfully populated.

    Parameters
    ----------
    value
        Value from a config field or caller-provided argument.

    Returns
    -------
    bool
        ``True`` when the value is not empty after string conversion.
    """
    return bool(value) and str(value).strip() != ""


def get_consist_storage_paths(
    output_path: str | os.PathLike[str] | Path,
) -> tuple[Path, Path]:
    """Resolve the Consist run directory and provenance database path.

    By default, Consist state lives under the active SynthFirm output
    directory. Operators can override those locations when they need to place
    provenance on a different disk or shared filesystem.

    Parameters
    ----------
    output_path
        SynthFirm output directory for the active scenario.

    Returns
    -------
    tuple[pathlib.Path, pathlib.Path]
        Run directory and DuckDB provenance database path.
    """
    base_path = _as_path(output_path)
    run_dir = _env_path("SYNTHFIRM_CONSIST_RUN_DIR") or (
        base_path / ".consist" / "runs"
    )
    db_path = _env_path("SYNTHFIRM_CONSIST_DB_PATH") or (
        base_path / ".consist" / "provenance.duckdb"
    )
    return run_dir, db_path


def create_consist_tracker(
    output_path: str | os.PathLike[str] | Path,
    *,
    data_root: str | os.PathLike[str] | Path | None = None,
    code_root: str | os.PathLike[str] | Path | None = None,
    profile_file_schema: bool = True,
    file_schema_sample_rows: int | None = 1000,
) -> Tracker:
    """Create the Tracker used by the SynthFirm teaching slice.

    Parameters
    ----------
    output_path
        SynthFirm output directory used to derive default Consist storage paths.
    data_root
        Root directory that contains SynthFirm inputs, outputs, plots, and
        parameters. When omitted, the parent of ``output_path`` is used.
    code_root
        Root directory for the SynthFirm source checkout. When omitted, the
        current working directory is used.
    profile_file_schema
        Whether Consist should automatically capture lightweight schemas for
        tabular file artifacts as they are logged.
    file_schema_sample_rows
        Maximum rows Consist should sample when profiling file schemas. ``None``
        asks Consist to inspect the full file.

    Returns
    -------
    consist.Tracker
        Tracker configured with storage paths and named ``data``/``code``
        mounts so recorded artifact URIs are portable.
    """
    output_root = _as_path(output_path).resolve()
    resolved_data_root = (
        _as_path(data_root).resolve() if data_root is not None else output_root.parent
    )
    resolved_code_root = (
        _as_path(code_root).resolve() if code_root is not None else Path.cwd().resolve()
    )
    run_dir, db_path = get_consist_storage_paths(output_path)
    tracker = Tracker(
        run_dir=run_dir,
        db_path=db_path,
        mounts={
            "data": str(resolved_data_root),
            "code": str(resolved_code_root),
        },
        project_root=str(resolved_code_root),
    )
    tracker.settings = replace(
        tracker.settings,
        schema_profile_enabled=profile_file_schema,
        schema_sample_rows=file_schema_sample_rows,
    )
    return tracker


def build_step1_consist_spec(
    *,
    output_path: str | os.PathLike[str] | Path,
    synthetic_firms_no_location_file: str | os.PathLike[str] | Path | None = None,
    synthetic_enterprise_file: str | os.PathLike[str] | Path | None = None,
    cbp_file: str | os.PathLike[str] | Path,
    mzemp_file: str | os.PathLike[str] | Path,
    mesozone_to_faf_file: str | os.PathLike[str] | Path,
    c_n6_n6io_sctg_file: str | os.PathLike[str] | Path,
    employment_per_firm_file: str | os.PathLike[str] | Path,
    employment_per_firm_gapfill_file: str | os.PathLike[str] | Path,
    zip_to_tract_file: str | os.PathLike[str] | Path,
    config_file: str | os.PathLike[str] | Path,
    assign_enterprises: bool,
    susb_file: str | os.PathLike[str] | Path = "",
    costar_file: str | os.PathLike[str] | Path = "",
    county_to_msa_file: str | os.PathLike[str] | Path = "",
    naics_crosswalk_file: str | os.PathLike[str] | Path = "",
    us_county_map_file: str | os.PathLike[str] | Path = "",
) -> dict[str, Any]:
    """Declare Consist inputs and outputs for Step 1 firm generation.

    Parameters
    ----------
    output_path
        Active SynthFirm output directory.
    synthetic_firms_no_location_file
        Main Step 1 firm output path.
    synthetic_enterprise_file
        Optional enterprise-assignment output path.
    cbp_file, mzemp_file, mesozone_to_faf_file
        Core Step 1 input files from the active scenario input directory.
    c_n6_n6io_sctg_file, employment_per_firm_file
        Parameter files used to synthesize firms.
    employment_per_firm_gapfill_file, zip_to_tract_file
        Additional parameter files used by firm synthesis.
    config_file
        Config file used for this SynthFirm run.
    assign_enterprises
        Whether enterprise assignment is enabled.
    susb_file, costar_file, county_to_msa_file
        Enterprise-assignment input files.
    naics_crosswalk_file, us_county_map_file
        Optional enterprise-assignment support files.

    Returns
    -------
    dict[str, Any]
        Consist run specification pieces consumed by ``Tracker.run``.
    """
    output_root = _as_path(output_path)
    inputs: dict[str, Path] = {
        "cbp_file": _as_path(cbp_file),
        "mzemp_file": _as_path(mzemp_file),
        "mesozone_to_faf_file": _as_path(mesozone_to_faf_file),
        "c_n6_n6io_sctg_file": _as_path(c_n6_n6io_sctg_file),
        "employment_per_firm_file": _as_path(employment_per_firm_file),
        "employment_per_firm_gapfill_file": _as_path(
            employment_per_firm_gapfill_file
        ),
        "zip_to_tract_file": _as_path(zip_to_tract_file),
        "config_file": _as_path(config_file),
    }
    if assign_enterprises:
        inputs["susb_file"] = _as_path(susb_file)
        inputs["costar_file"] = _as_path(costar_file)
        inputs["county_to_msa_file"] = _as_path(county_to_msa_file)
        if _nonempty(naics_crosswalk_file):
            inputs["naics_crosswalk_file"] = _as_path(naics_crosswalk_file)
        if _nonempty(us_county_map_file):
            inputs["us_county_map_file"] = _as_path(us_county_map_file)

    output_paths: dict[str, Path] = {
        "synthetic_firms": (
            _as_path(synthetic_firms_no_location_file)
            if synthetic_firms_no_location_file
            else output_root / "synthetic_firms.csv"
        ),
    }
    if assign_enterprises:
        output_paths["synthetic_enterprise"] = (
            _as_path(synthetic_enterprise_file)
            if synthetic_enterprise_file
            else output_root / "synthetic_enterprise.csv"
        )

    output_sets: dict[str, OutputSet] = {}
    if assign_enterprises:
        output_sets["enterprise_assignment"] = OutputSet(
            root=output_root / "enterprise_assignment",
            include="*",
            kind="enterprise-assignment-diagnostics",
        )

    return {
        "inputs": inputs,
        "output_paths": output_paths,
        "output_sets": output_sets,
        "config": {"assign_enterprises": assign_enterprises},
        "cache_options": CacheOptions(cache_mode="overwrite"),
        "execution_options": ExecutionOptions(input_binding="paths"),
    }


def build_step2_consist_spec(
    *,
    output_path: str | os.PathLike[str] | Path,
    io_summary_file: str | os.PathLike[str] | Path | None = None,
    wholesaler_file: str | os.PathLike[str] | Path | None = None,
    producer_file: str | os.PathLike[str] | Path | None = None,
    io_filtered_file: str | os.PathLike[str] | Path | None = None,
    c_n6_n6io_sctg_file: str | os.PathLike[str] | Path,
    synthetic_firms_no_location_file: str | os.PathLike[str] | Path,
    mesozone_to_faf_file: str | os.PathLike[str] | Path,
    BEA_io_2017_file: str | os.PathLike[str] | Path,
    agg_unit_cost_file: str | os.PathLike[str] | Path,
    prod_by_zone_file: str | os.PathLike[str] | Path,
    sctg_group_file: str | os.PathLike[str] | Path,
    config_file: str | os.PathLike[str] | Path,
    producer_by_sctg_filehead: str | os.PathLike[str] | Path,
) -> dict[str, Any]:
    """Declare Consist inputs and outputs for Step 2 producer generation.

    Parameters
    ----------
    output_path
        Active SynthFirm output directory.
    io_summary_file, wholesaler_file, producer_file, io_filtered_file
        Single-file Step 2 output artifacts.
    c_n6_n6io_sctg_file, synthetic_firms_no_location_file
        Step 2 inputs from parameters and Step 1.
    mesozone_to_faf_file, BEA_io_2017_file, agg_unit_cost_file
        Additional Step 2 input files.
    prod_by_zone_file, sctg_group_file
        Producer allocation and SCTG lookup inputs.
    config_file
        Config file used for this SynthFirm run.
    producer_by_sctg_filehead
        File prefix used by Step 2 to write SCTG-group CSVs.

    Returns
    -------
    dict[str, Any]
        Consist run specification pieces consumed by ``Tracker.run``.
    """
    output_root = _as_path(output_path)
    producer_head = _as_path(producer_by_sctg_filehead)
    return {
        "inputs": {
            "c_n6_n6io_sctg_file": _as_path(c_n6_n6io_sctg_file),
            "synthetic_firms_no_location_file": _as_path(
                synthetic_firms_no_location_file
            ),
            "mesozone_to_faf_file": _as_path(mesozone_to_faf_file),
            "BEA_io_2017_file": _as_path(BEA_io_2017_file),
            "agg_unit_cost_file": _as_path(agg_unit_cost_file),
            "prod_by_zone_file": _as_path(prod_by_zone_file),
            "sctg_group_file": _as_path(sctg_group_file),
            "config_file": _as_path(config_file),
        },
        "output_paths": {
            "io_summary": (
                _as_path(io_summary_file)
                if io_summary_file
                else output_root / "io_summary.csv"
            ),
            "wholesaler": (
                _as_path(wholesaler_file)
                if wholesaler_file
                else output_root / "wholesaler.csv"
            ),
            "producer": (
                _as_path(producer_file)
                if producer_file
                else output_root / "producer.csv"
            ),
            "io_filtered": (
                _as_path(io_filtered_file)
                if io_filtered_file
                else output_root / "io_filtered.csv"
            ),
        },
        "output_sets": {
            "producer_by_sctg": OutputSet(
                root=producer_head.parent,
                include=f"{producer_head.name}*.csv",
                kind="producer-by-sctg",
            )
        },
        "config": {},
        "cache_options": CacheOptions(cache_mode="overwrite"),
        "execution_options": ExecutionOptions(input_binding="paths"),
    }


def build_step3_consist_spec(
    *,
    output_path: str | os.PathLike[str] | Path,
    consumer_file: str | os.PathLike[str] | Path | None = None,
    sample_consumer_file: str | os.PathLike[str] | Path | None = None,
    synthetic_firms_no_location_file: str | os.PathLike[str] | Path,
    mesozone_to_faf_file: str | os.PathLike[str] | Path,
    c_n6_n6io_sctg_file: str | os.PathLike[str] | Path,
    agg_unit_cost_file: str | os.PathLike[str] | Path,
    cons_by_zone_file: str | os.PathLike[str] | Path,
    sctg_group_file: str | os.PathLike[str] | Path,
    wholesaler_file: str | os.PathLike[str] | Path,
    producer_file: str | os.PathLike[str] | Path,
    io_filtered_file: str | os.PathLike[str] | Path,
    config_file: str | os.PathLike[str] | Path,
    consumer_by_sctg_filehead: str | os.PathLike[str] | Path,
    wholesalecostfactor: float,
) -> dict[str, Any]:
    """Declare Consist inputs and outputs for Step 3 consumer generation.

    Parameters
    ----------
    output_path
        Active SynthFirm output directory.
    consumer_file, sample_consumer_file
        Single-file Step 3 output artifacts.
    synthetic_firms_no_location_file, mesozone_to_faf_file
        Step 3 inputs from Step 1 and scenario inputs.
    c_n6_n6io_sctg_file, agg_unit_cost_file, cons_by_zone_file
        Parameter files used by consumer generation.
    sctg_group_file
        SCTG lookup input.
    wholesaler_file, producer_file, io_filtered_file
        Step 2 outputs consumed by Step 3.
    config_file
        Config file used for this SynthFirm run.
    consumer_by_sctg_filehead
        File prefix used by Step 3 to write SCTG-group CSVs.
    wholesalecostfactor
        Wholesale cost factor returned by Step 2 and used by Step 3.

    Returns
    -------
    dict[str, Any]
        Consist run specification pieces consumed by ``Tracker.run``.
    """
    output_root = _as_path(output_path)
    consumer_head = _as_path(consumer_by_sctg_filehead)
    return {
        "inputs": {
            "synthetic_firms_no_location_file": _as_path(
                synthetic_firms_no_location_file
            ),
            "mesozone_to_faf_file": _as_path(mesozone_to_faf_file),
            "c_n6_n6io_sctg_file": _as_path(c_n6_n6io_sctg_file),
            "agg_unit_cost_file": _as_path(agg_unit_cost_file),
            "cons_by_zone_file": _as_path(cons_by_zone_file),
            "sctg_group_file": _as_path(sctg_group_file),
            "wholesaler_file": _as_path(wholesaler_file),
            "producer_file": _as_path(producer_file),
            "io_filtered_file": _as_path(io_filtered_file),
            "config_file": _as_path(config_file),
        },
        "output_paths": {
            "consumer": (
                _as_path(consumer_file)
                if consumer_file
                else output_root / "consumer.csv"
            ),
            "sample_consumer": (
                _as_path(sample_consumer_file)
                if sample_consumer_file
                else output_root / "sample_consumer.csv"
            ),
        },
        "output_sets": {
            "consumer_by_sctg": OutputSet(
                root=consumer_head.parent,
                include=f"{consumer_head.name}*.csv",
                kind="consumer-by-sctg",
            )
        },
        "cache_options": CacheOptions(cache_mode="overwrite"),
        "execution_options": ExecutionOptions(input_binding="paths"),
        "config": {"wholesalecostfactor": wholesalecostfactor},
    }
