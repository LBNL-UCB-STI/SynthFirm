from pathlib import Path
import inspect

from consist import CacheOptions
from consist.models.artifact_schema import ArtifactSchemaObservation
from sqlmodel import Session, select

from utils import consist_tracking


def test_consist_storage_paths_default_and_env_override(monkeypatch, tmp_path):
    monkeypatch.delenv("SYNTHFIRM_CONSIST_RUN_DIR", raising=False)
    monkeypatch.delenv("SYNTHFIRM_CONSIST_DB_PATH", raising=False)

    run_dir, db_path = consist_tracking.get_consist_storage_paths(tmp_path)
    assert run_dir == tmp_path / ".consist" / "runs"
    assert db_path == tmp_path / ".consist" / "provenance.duckdb"

    monkeypatch.setenv("SYNTHFIRM_CONSIST_RUN_DIR", "/tmp/custom-runs")
    monkeypatch.setenv("SYNTHFIRM_CONSIST_DB_PATH", "/tmp/custom.duckdb")

    run_dir, db_path = consist_tracking.get_consist_storage_paths(tmp_path)
    assert run_dir == Path("/tmp/custom-runs")
    assert db_path == Path("/tmp/custom.duckdb")


def test_consist_tracker_uses_data_and_code_mounts(tmp_path):
    data_root = tmp_path / "data"
    code_root = tmp_path / "code"
    output_path = data_root / "outputs_Austin"
    data_root.mkdir()
    code_root.mkdir()
    output_path.mkdir()

    tracker = consist_tracking.create_consist_tracker(
        output_path,
        data_root=data_root,
        code_root=code_root,
    )

    assert tracker.mounts == {
        "data": str(data_root.resolve()),
        "code": str(code_root.resolve()),
    }
    assert (
        tracker.fs.virtualize_path(data_root / "inputs_Austin" / "firms.csv")
        == "data://inputs_Austin/firms.csv"
    )
    assert tracker.fs.virtualize_path(code_root / "SynthFirm_run.py") == (
        "code://SynthFirm_run.py"
    )


def test_consist_tracker_profiles_input_and_output_schemas(tmp_path):
    data_root = tmp_path / "data"
    output_path = data_root / "outputs_Austin"
    input_path = data_root / "inputs_Austin" / "input.csv"
    output_csv = output_path / "output.csv"
    input_path.parent.mkdir(parents=True)
    output_path.mkdir(parents=True)
    input_path.write_text("id,value\n1,10\n2,20\n", encoding="utf-8")

    tracker = consist_tracking.create_consist_tracker(
        output_path,
        data_root=data_root,
        code_root=tmp_path,
    )

    def write_output() -> None:
        output_csv.write_text("id,total\n1,30\n", encoding="utf-8")

    tracker.run(
        write_output,
        inputs={"input_csv": input_path},
        output_paths={"output_csv": output_csv},
        cache_options=CacheOptions(cache_mode="overwrite"),
    )

    with Session(tracker.db.engine) as session:
        observations = session.exec(select(ArtifactSchemaObservation)).all()

    assert len(observations) == 2
    assert {observation.source for observation in observations} == {"file"}


def test_step1_consist_spec_without_enterprises(tmp_path):
    spec = consist_tracking.build_step1_consist_spec(
        output_path=tmp_path,
        cbp_file=tmp_path / "cbp.csv",
        mzemp_file=tmp_path / "mzemp.csv",
        mesozone_to_faf_file=tmp_path / "mesozone_to_faf.csv",
        c_n6_n6io_sctg_file=tmp_path / "crosswalk.csv",
        employment_per_firm_file=tmp_path / "emp.csv",
        employment_per_firm_gapfill_file=tmp_path / "gapfill.csv",
        zip_to_tract_file=tmp_path / "zip.csv",
        config_file=tmp_path / "config.ini",
        assign_enterprises=False,
    )

    assert set(spec["inputs"]) == {
        "cbp_file",
        "mzemp_file",
        "mesozone_to_faf_file",
        "c_n6_n6io_sctg_file",
        "employment_per_firm_file",
        "employment_per_firm_gapfill_file",
        "zip_to_tract_file",
        "config_file",
    }
    assert set(spec["output_paths"]) == {"synthetic_firms"}
    assert "synthetic_enterprise" not in spec["output_paths"]
    assert spec["output_sets"] == {}
    assert spec["execution_options"].input_binding == "paths"
    assert spec["cache_options"].cache_mode == "overwrite"


def test_step1_consist_spec_with_enterprises(tmp_path):
    spec = consist_tracking.build_step1_consist_spec(
        output_path=tmp_path,
        cbp_file=tmp_path / "cbp.csv",
        mzemp_file=tmp_path / "mzemp.csv",
        mesozone_to_faf_file=tmp_path / "mesozone_to_faf.csv",
        c_n6_n6io_sctg_file=tmp_path / "crosswalk.csv",
        employment_per_firm_file=tmp_path / "emp.csv",
        employment_per_firm_gapfill_file=tmp_path / "gapfill.csv",
        zip_to_tract_file=tmp_path / "zip.csv",
        config_file=tmp_path / "config.ini",
        assign_enterprises=True,
        susb_file=tmp_path / "susb.csv",
        costar_file=tmp_path / "costar.csv",
        county_to_msa_file=tmp_path / "county_to_msa.csv",
        naics_crosswalk_file=tmp_path / "naics_xwalk.csv",
        us_county_map_file=tmp_path / "us_county_map.geojson",
    )

    assert set(spec["inputs"]) == {
        "cbp_file",
        "mzemp_file",
        "mesozone_to_faf_file",
        "c_n6_n6io_sctg_file",
        "employment_per_firm_file",
        "employment_per_firm_gapfill_file",
        "zip_to_tract_file",
        "config_file",
        "susb_file",
        "costar_file",
        "county_to_msa_file",
        "naics_crosswalk_file",
        "us_county_map_file",
    }
    assert set(spec["output_paths"]) == {
        "synthetic_firms",
        "synthetic_enterprise",
    }
    assert set(spec["output_sets"]) == {"enterprise_assignment"}


def test_step2_consist_spec_includes_producer_by_sctg_output_set(tmp_path):
    spec = consist_tracking.build_step2_consist_spec(
        output_path=tmp_path,
        c_n6_n6io_sctg_file=tmp_path / "crosswalk.csv",
        synthetic_firms_no_location_file=tmp_path / "synthetic_firms.csv",
        mesozone_to_faf_file=tmp_path / "mesozone_to_faf.csv",
        BEA_io_2017_file=tmp_path / "bea.csv",
        agg_unit_cost_file=tmp_path / "unitcost.csv",
        prod_by_zone_file=tmp_path / "prod_by_zone.csv",
        sctg_group_file=tmp_path / "sctg.csv",
        config_file=tmp_path / "config.ini",
        producer_by_sctg_filehead=tmp_path / "nested" / "prods_sctg",
    )

    assert set(spec["output_paths"]) == {
        "io_summary",
        "wholesaler",
        "producer",
        "io_filtered",
    }
    output_set = spec["output_sets"]["producer_by_sctg"]
    assert output_set.root == tmp_path / "nested"
    assert output_set.include == "prods_sctg*.csv"


def test_step3_consist_spec_includes_consumer_by_sctg_output_set(tmp_path):
    spec = consist_tracking.build_step3_consist_spec(
        output_path=tmp_path,
        synthetic_firms_no_location_file=tmp_path / "synthetic_firms.csv",
        mesozone_to_faf_file=tmp_path / "mesozone_to_faf.csv",
        c_n6_n6io_sctg_file=tmp_path / "crosswalk.csv",
        agg_unit_cost_file=tmp_path / "unitcost.csv",
        cons_by_zone_file=tmp_path / "cons_by_zone.csv",
        sctg_group_file=tmp_path / "sctg.csv",
        wholesaler_file=tmp_path / "wholesaler.csv",
        producer_file=tmp_path / "producer.csv",
        io_filtered_file=tmp_path / "io_filtered.csv",
        config_file=tmp_path / "config.ini",
        consumer_by_sctg_filehead=tmp_path / "nested" / "consumers_sctg",
        wholesalecostfactor=1.25,
    )

    assert set(spec["output_paths"]) == {
        "consumer",
        "sample_consumer",
    }
    output_set = spec["output_sets"]["consumer_by_sctg"]
    assert output_set.root == tmp_path / "nested"
    assert output_set.include == "consumers_sctg*.csv"
    assert spec["config"] == {"wholesalecostfactor": 1.25}


def test_public_consist_helpers_have_docstrings():
    public_helpers = [
        consist_tracking.get_consist_storage_paths,
        consist_tracking.create_consist_tracker,
        consist_tracking.build_step1_consist_spec,
        consist_tracking.build_step2_consist_spec,
        consist_tracking.build_step3_consist_spec,
    ]

    for helper in public_helpers:
        docstring = inspect.getdoc(helper)
        assert docstring is not None
        assert "Parameters" in docstring
