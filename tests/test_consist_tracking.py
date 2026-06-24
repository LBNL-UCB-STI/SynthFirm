from pathlib import Path
import configparser
import inspect

from consist import ArtifactSpec, CacheOptions
from consist.models.artifact_schema import ArtifactSchemaObservation
from sqlmodel import Session, select

from utils import consist_tracking
from utils.consist_schemas import (
    SYNTHFIRM_CONSIST_SCHEMAS,
    ConsumersBySctg,
    IoSummary,
    ProducersBySctg,
    SyntheticConsumers,
    SyntheticFirms,
    SyntheticProducers,
    SyntheticWholesalers,
)


def test_consist_storage_paths_default_and_env_override(monkeypatch, tmp_path):
    monkeypatch.delenv("SYNTHFIRM_CONSIST_RUN_DIR", raising=False)
    monkeypatch.delenv("SYNTHFIRM_CONSIST_DB_PATH", raising=False)
    output_path = tmp_path / "outputs_Austin"

    run_dir, db_path = consist_tracking.get_consist_storage_paths(output_path)
    assert run_dir == tmp_path / "database" / "runs"
    assert db_path == tmp_path / "database" / "provenance.duckdb"

    monkeypatch.setenv("SYNTHFIRM_CONSIST_RUN_DIR", "/tmp/custom-runs")
    monkeypatch.setenv("SYNTHFIRM_CONSIST_DB_PATH", "/tmp/custom.duckdb")

    run_dir, db_path = consist_tracking.get_consist_storage_paths(output_path)
    assert run_dir == Path("/tmp/custom-runs")
    assert db_path == Path("/tmp/custom.duckdb")


def test_consist_storage_paths_use_explicit_storage_root(monkeypatch, tmp_path):
    monkeypatch.delenv("SYNTHFIRM_CONSIST_RUN_DIR", raising=False)
    monkeypatch.delenv("SYNTHFIRM_CONSIST_DB_PATH", raising=False)
    output_path = tmp_path / "somewhere" / "outputs_Austin"
    storage_root = tmp_path / "central_data"

    run_dir, db_path = consist_tracking.get_consist_storage_paths(
        output_path,
        storage_root=storage_root,
    )

    assert run_dir == storage_root / "database" / "runs"
    assert db_path == storage_root / "database" / "provenance.duckdb"


def test_consist_shell_command_quotes_db_path_with_spaces():
    command = consist_tracking.build_consist_shell_command(
        Path("/tmp/SynthFirm outputs/provenance.duckdb")
    )

    assert command == (
        "consist shell --trust-db --db-path "
        "'/tmp/SynthFirm outputs/provenance.duckdb'"
    )


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
    assert tracker.run_dir == data_root / "database" / "runs"
    _, db_path = consist_tracking.get_consist_storage_paths(
        output_path,
        storage_root=data_root,
    )
    assert db_path == data_root / "database" / "provenance.duckdb"
    assert (
        tracker.fs.virtualize_path(data_root / "inputs_Austin" / "firms.csv")
        == "data://inputs_Austin/firms.csv"
    )
    assert tracker.fs.virtualize_path(code_root / "SynthFirm_run.py") == (
        "code://SynthFirm_run.py"
    )


def test_consist_tracker_registers_teaching_slice_schemas(tmp_path):
    output_path = tmp_path / "outputs_Austin"
    output_path.mkdir()

    tracker = consist_tracking.create_consist_tracker(
        output_path,
        data_root=tmp_path,
        code_root=tmp_path,
    )

    expected_schema_names = {
        schema.__name__ for schema in SYNTHFIRM_CONSIST_SCHEMAS
    }

    assert expected_schema_names.issubset(tracker.registered_schemas)


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


def test_artifact_spec_persists_user_provided_output_schema(tmp_path):
    output_path = tmp_path / "outputs_Austin"
    input_path = tmp_path / "inputs_Austin" / "input.csv"
    output_csv = output_path / "synthetic_firms.csv"
    input_path.parent.mkdir(parents=True)
    output_path.mkdir(parents=True)
    input_path.write_text("id\n1\n", encoding="utf-8")

    tracker = consist_tracking.create_consist_tracker(
        output_path,
        data_root=tmp_path,
        code_root=tmp_path,
    )

    def write_output() -> None:
        output_csv.write_text(
            (
                "CBPZONE,FAFZONE,esizecat,Industry_NAICS6_Make,"
                "Commodity_SCTG,Emp,BusID,MESOZONE,ZIPCODE\n"
                "1,2,3,111111,4,5.0,6,7,8\n"
            ),
            encoding="utf-8",
        )

    tracker.run(
        write_output,
        inputs={"input_csv": input_path},
        output_paths={
            "synthetic_firms": ArtifactSpec(
                path=output_csv,
                schema=SyntheticFirms,
                profile_file_schema=True,
            )
        },
        profile_file_schema=True,
        cache_options=CacheOptions(cache_mode="overwrite"),
    )

    with Session(tracker.db.engine) as session:
        observations = session.exec(select(ArtifactSchemaObservation)).all()

    assert sorted(observation.source for observation in observations) == [
        "file",
        "file",
        "user_provided",
    ]


def test_synthfirm_config_payload_uses_config_contents_not_absolute_path(tmp_path):
    config_file = tmp_path / "Austin_local.conf"
    config_file.write_text(
        "[ENVIRONMENT]\nfile_path = .\nscenario_name = Austin\n",
        encoding="utf-8",
    )
    config = configparser.ConfigParser()
    config.read(config_file)

    payload = consist_tracking.build_synthfirm_config_payload(config, config_file)

    assert payload["source_name"] == "Austin_local.conf"
    assert payload["sections"]["ENVIRONMENT"]["file_path"] == "."
    assert payload["sections"]["ENVIRONMENT"]["scenario_name"] == "Austin"
    assert str(tmp_path) not in str(payload)


def test_step1_consist_spec_without_enterprises(tmp_path):
    synthfirm_config = {"source_name": "test.conf", "sections": {}}
    spec = consist_tracking.build_step1_consist_spec(
        output_path=tmp_path,
        cbp_file=tmp_path / "cbp.csv",
        mzemp_file=tmp_path / "mzemp.csv",
        mesozone_to_faf_file=tmp_path / "mesozone_to_faf.csv",
        c_n6_n6io_sctg_file=tmp_path / "crosswalk.csv",
        employment_per_firm_file=tmp_path / "emp.csv",
        employment_per_firm_gapfill_file=tmp_path / "gapfill.csv",
        zip_to_tract_file=tmp_path / "zip.csv",
        synthfirm_config=synthfirm_config,
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
    }
    assert spec["config"]["synthfirm_config"] == synthfirm_config
    assert spec["config"]["assign_enterprises"] is False
    assert set(spec["output_paths"]) == {"synthetic_firms"}
    assert isinstance(spec["output_paths"]["synthetic_firms"], ArtifactSpec)
    assert spec["output_paths"]["synthetic_firms"].schema is SyntheticFirms
    assert "synthetic_enterprise" not in spec["output_paths"]
    assert spec["output_sets"] == {}
    assert spec["execution_options"].input_binding == "paths"
    assert spec["cache_options"].cache_mode == "overwrite"


def test_step1_consist_spec_with_enterprises(tmp_path):
    synthfirm_config = {"source_name": "test.conf", "sections": {}}
    spec = consist_tracking.build_step1_consist_spec(
        output_path=tmp_path,
        cbp_file=tmp_path / "cbp.csv",
        mzemp_file=tmp_path / "mzemp.csv",
        mesozone_to_faf_file=tmp_path / "mesozone_to_faf.csv",
        c_n6_n6io_sctg_file=tmp_path / "crosswalk.csv",
        employment_per_firm_file=tmp_path / "emp.csv",
        employment_per_firm_gapfill_file=tmp_path / "gapfill.csv",
        zip_to_tract_file=tmp_path / "zip.csv",
        synthfirm_config=synthfirm_config,
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
    assert isinstance(spec["output_paths"]["synthetic_firms"], ArtifactSpec)
    assert spec["output_paths"]["synthetic_firms"].schema is SyntheticFirms
    assert not isinstance(spec["output_paths"]["synthetic_enterprise"], ArtifactSpec)
    assert set(spec["output_sets"]) == {"enterprise_assignment"}


def test_step2_consist_spec_includes_producer_by_sctg_output_set(tmp_path):
    synthfirm_config = {"source_name": "test.conf", "sections": {}}
    spec = consist_tracking.build_step2_consist_spec(
        output_path=tmp_path,
        c_n6_n6io_sctg_file=tmp_path / "crosswalk.csv",
        synthetic_firms_no_location_file=tmp_path / "synthetic_firms.csv",
        mesozone_to_faf_file=tmp_path / "mesozone_to_faf.csv",
        BEA_io_2017_file=tmp_path / "bea.csv",
        agg_unit_cost_file=tmp_path / "unitcost.csv",
        prod_by_zone_file=tmp_path / "prod_by_zone.csv",
        sctg_group_file=tmp_path / "sctg.csv",
        synthfirm_config=synthfirm_config,
        producer_by_sctg_filehead=tmp_path / "nested" / "prods_sctg",
    )

    assert set(spec["output_paths"]) == {
        "io_summary",
        "wholesaler",
        "producer",
        "io_filtered",
    }
    assert spec["output_paths"]["io_summary"].schema is IoSummary
    assert spec["output_paths"]["wholesaler"].schema is SyntheticWholesalers
    assert spec["output_paths"]["producer"].schema is SyntheticProducers
    assert not isinstance(spec["output_paths"]["io_filtered"], ArtifactSpec)
    output_set = spec["output_sets"]["producer_by_sctg"]
    assert output_set.root == tmp_path / "nested"
    assert output_set.include == "prods_sctg*.csv"
    assert output_set.schema is ProducersBySctg
    assert spec["config"]["synthfirm_config"] == synthfirm_config


def test_step3_consist_spec_includes_consumer_by_sctg_output_set(tmp_path):
    synthfirm_config = {"source_name": "test.conf", "sections": {}}
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
        synthfirm_config=synthfirm_config,
        consumer_by_sctg_filehead=tmp_path / "nested" / "consumers_sctg",
        wholesalecostfactor=1.25,
    )

    assert set(spec["output_paths"]) == {
        "consumer",
        "sample_consumer",
    }
    assert spec["output_paths"]["consumer"].schema is SyntheticConsumers
    assert spec["output_paths"]["sample_consumer"].schema is SyntheticConsumers
    output_set = spec["output_sets"]["consumer_by_sctg"]
    assert output_set.root == tmp_path / "nested"
    assert output_set.include == "consumers_sctg*.csv"
    assert output_set.schema is ConsumersBySctg
    assert spec["config"]["synthfirm_config"] == synthfirm_config
    assert spec["config"]["wholesalecostfactor"] == 1.25


def test_step4_consist_spec_includes_forecast_year_config(tmp_path):
    synthfirm_config = {"source_name": "test.conf", "sections": {}}
    spec = consist_tracking.build_step4_consist_spec(
        output_path=tmp_path,
        synthetic_firms_no_location_file=tmp_path / "synthetic_firms.csv",
        producer_file=tmp_path / "synthetic_producers.csv",
        consumer_file=tmp_path / "synthetic_consumers.csv",
        prod_forecast_file=tmp_path / "total_commodity_production_2030.csv",
        cons_forecast_file=tmp_path / "total_commodity_attraction_2030.csv",
        mesozone_to_faf_file=tmp_path / "mesozone_to_faf.csv",
        sctg_group_file=tmp_path / "sctg.csv",
        synthfirm_config=synthfirm_config,
        consumer_by_sctg_filehead=tmp_path / "nested" / "consumers_sctg",
        forecast_year="2030",
    )

    assert set(spec["inputs"]) == {
        "synthetic_firms",
        "producer",
        "consumer",
        "prod_forecast_file",
        "cons_forecast_file",
        "mesozone_to_faf_file",
        "sctg_group_file",
    }
    assert spec["config"]["synthfirm_config"] == synthfirm_config
    assert spec["config"]["forecast_year"] == "2030"
    assert spec["config"]["forecast_tonnage_column"] == "tons_2030"
    assert set(spec["output_paths"]) == {
        "forecasted_synthetic_firms",
        "forecasted_producer",
        "forecasted_consumer",
    }
    assert spec["output_paths"]["forecasted_synthetic_firms"].schema is SyntheticFirms
    assert spec["output_paths"]["forecasted_producer"].schema is SyntheticProducers
    assert spec["output_paths"]["forecasted_consumer"].schema is SyntheticConsumers
    output_set = spec["output_sets"]["forecasted_consumer_by_sctg"]
    assert output_set.root == tmp_path / "nested"
    assert output_set.include == "consumers_sctg*.csv"
    assert output_set.schema is ConsumersBySctg
    assert spec["execution_options"].input_binding == "paths"
    assert spec["cache_options"].cache_mode == "overwrite"


def test_public_consist_helpers_have_docstrings():
    public_helpers = [
        consist_tracking.get_consist_storage_paths,
        consist_tracking.build_consist_shell_command,
        consist_tracking.create_consist_tracker,
        consist_tracking.build_synthfirm_config_payload,
        consist_tracking.build_step1_consist_spec,
        consist_tracking.build_step2_consist_spec,
        consist_tracking.build_step3_consist_spec,
        consist_tracking.build_step4_consist_spec,
    ]

    for helper in public_helpers:
        docstring = inspect.getdoc(helper)
        assert docstring is not None
        assert "Parameters" in docstring
