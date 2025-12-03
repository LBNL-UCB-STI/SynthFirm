import os
import pandas as pd
import geopandas as gpd

def boston_bus_location_calibration(taz_file, mzemp_file,
                                    synthetic_firms_with_location_file,
                                    plot_path):
    """
    Boston post-process:
      1. Load firms with lat/lon
      2. Assign TAZ via spatial join
      3. Overwrite the same file with a new 'taz_id' column
      4. Save a simple TAZ employment summary
    """


    print("\n[POST] Boston business location calibration started")

    # -------------------------------------------------------
    # 1. Load firms
    # -------------------------------------------------------
    firms = pd.read_csv(synthetic_firms_with_location_file)
    print(f"[POST] Loaded firms: {len(firms):,} rows")

    if not {"lat", "lon"}.issubset(firms.columns):
        raise ValueError("Columns 'lat' and 'lon' are required.")

    for col in ["taz_id", "index_right", "index_left"]:
        if col in firms.columns:
            print(f"[POST] Dropping existing column: {col}")
            firms = firms.drop(columns=[col])

    firms_gdf = gpd.GeoDataFrame(
        firms,
        geometry=gpd.points_from_xy(firms["lon"], firms["lat"]),
        crs="EPSG:4326"
    )

    # -------------------------------------------------------
    # 2. Load TAZ polygons
    # -------------------------------------------------------
    print(f"[POST] Loading TAZ file: {taz_file}")
    taz = gpd.read_file(taz_file)

    if "taz_id" not in taz.columns:
        raise ValueError("Column 'taz_id' not found in TAZ shapefile.")

    # Ensure CRS matches
    if taz.crs is None:
        print("[POST] Warning: TAZ has no CRS. Setting EPSG:26919 by default.")
        taz = taz.set_crs("EPSG:26919")

    taz = taz.to_crs("EPSG:4326")

    print(f"[POST] TAZ polygons: {len(taz):,}")

    # -------------------------------------------------------
    # 3. Spatial join
    # -------------------------------------------------------
    print("[POST] Assigning TAZ via spatial join...")
    firms_with_taz = gpd.sjoin(
        firms_gdf,
        taz[["taz_id", "geometry"]],
        how="left",
        predicate="intersects"
    )
    firms_with_taz = firms_with_taz.drop(columns=['index_right'])

    missing = firms_with_taz["taz_id"].isna().sum()
    print(f"[POST] Firms without TAZ match: {missing:,}")

    # -------------------------------------------------------
    # 4. Overwrite original firms file
    # -------------------------------------------------------
    df_out = firms_with_taz.drop(columns="geometry")
    print(f"[POST] Total number of firms after adding TAZ ID: {len(df_out)}")

    df_out.to_csv(synthetic_firms_with_location_file, index=False)
    print(f"[POST] Updated file saved: {synthetic_firms_with_location_file}")


    print("[POST] Boston business location calibration completed.\n")
