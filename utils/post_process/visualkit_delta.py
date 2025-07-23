#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 12:08:15 2025

@author: xiaodanxu
"""
from sklearn.metrics import root_mean_squared_error 
from sklearn.metrics import r2_score
import matplotlib.pyplot as plt
import seaborn as sns
import shapely.wkt
import geopandas as gpd
import contextily as cx
import numpy as np
import matplotlib
import pandas as pd

plt.style.use('seaborn-v0_8-white')
sns.set(font_scale=1.4)
sns.set_style("white")

def plot_emp_comparison_scatter(df, x_col, y_col, agg_level,
                        plot_filepath, alpha = 0.3):
    # Calculate metrics
    rmse_emp = root_mean_squared_error(df[x_col], df[y_col])
    r2_emp = r2_score(df[x_col], df[y_col])
    rmse_emp = np.round(rmse_emp, 1)
    r2_emp = np.round(r2_emp, 2)
    
    # Print sums and metrics
    print('The total reported and modeled employment:')
    print(df[x_col].sum(), df[y_col].sum())
    print('The RMSE and R2 values:')
    print(rmse_emp, r2_emp)
        
    # Determine axis limits with a 5% margin
    max_val = max(df[x_col].quantile(0.999), df[y_col].quantile(0.999))
    axis_max = max_val * 1.05
    
    # Plot
    sns.lmplot(
        data=df, x=x_col, y=y_col, 
        height=4.5, aspect=1.2, 
        line_kws={'color': 'grey'}, 
        scatter_kws={'alpha':alpha}
    )
    plt.xlim([0, axis_max])
    plt.ylim([0, axis_max])
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.title(f'Employment by {agg_level}, $R^{{2}}$ = {r2_emp} , RMSE = {rmse_emp}', fontsize=14)
    
    # Save
    plt.savefig(plot_filepath, dpi=200, bbox_inches='tight')
    plt.show()
    

# define bar plot
def plot_emp_comparison_bar(df, x_col, y_col, agg_level,
                        plot_filepath):
    # Calculate metrics
    rmse_emp = root_mean_squared_error(df[x_col], df[y_col])
    r2_emp = r2_score(df[x_col], df[y_col])
    rmse_emp = np.round(rmse_emp, 1)
    r2_emp = np.round(r2_emp, 2)
    
    # Print sums and metrics
    print('The total reported and modeled employment:')
    print(df[x_col].sum(), df[y_col].sum())
    print('The RMSE and R2 values:')
    print(rmse_emp, r2_emp)
    
    # Set plot style
    
    fig, ax = plt.subplots(figsize = (6,4.5))
    ax.set(facecolor = "white")
    df.plot(kind = 'bar',x = agg_level, ax = ax)
    plt.xticks(rotation = 60, ha = 'right')
    plt.xlabel('Industry NAICS code')
    plt.ylabel('Employment')
    
    ax.spines['bottom'].set_color('0.5')
    ax.spines['top'].set_color('0.5')
    ax.spines['right'].set_color('0.5')
    ax.spines['left'].set_color('0.5')
    
    plt.title(f'Employment by {agg_level}, $R^{{2}}$ = {r2_emp} , RMSE = {rmse_emp}', fontsize=14)
    
    # Save
    plt.savefig(plot_filepath, dpi=200, bbox_inches='tight')
    plt.show()
    

# define map plot
def plot_region_map(region_gdf, column, title,
                    filename, add_basemap = True, 
                    vmin=0, vmax=50, alpha=0.5, cmap='viridis'):
    """
    Plots a choropleth map of firm density and adds a basemap.
    
    Parameters:
        region_gdf: GeoDataFrame
            The geodataframe with polygon regions and firm attr column.
        column: str
            Name of the column for density.
        filename: str
            Output filename.
        add_basemap: boolean
            If adding basemap from contexility (not needed for national plot)
        vmin, vmax: float
            Color scale min and max.
        alpha: float
            Polygon alpha.
        cmap: str
            Colormap.
    """

    sns.set(font_scale=1.2)

    ax = region_gdf.plot(
        figsize=(6, 4.5),
        column=column,
        vmin=vmin, vmax=vmax,
        alpha=alpha,
        legend=True,
        cmap=cmap,
        linewidth=0.01,
        legend_kwds={'shrink': 0.8},
        edgecolor='none'
    )
    if add_basemap:
        cx.add_basemap(ax, crs=region_gdf.crs.to_string() if region_gdf.crs else 'EPSG:4326', 
                   source=cx.providers.CartoDB.Positron)
    ax.grid(False)
    ax.axis('off')
    plt.title(title)
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()

# define map plot by county
# define map plot by county
def plot_county_map(region_gdf, column, title,
                    filename, logscale = True, 
                    vmin=0, vmax=50, legend_scale = 0.8, alpha=0.5, cmap='viridis'):
    """
    Plots a choropleth map of firm density and adds a basemap.
    
    Parameters:
        region_gdf: GeoDataFrame
            The geodataframe with polygon regions and firm attr column.
        column: str
            Name of the column for density.
        filename: str
            Output filename.
        logscale: boolean
            If use logscale for color ramp
        vmin, vmax: float
            Color scale min and max.
        alpha: float
            Polygon alpha.
        cmap: str
            Colormap.
    """
    if cmap == 'viridis':
        facecolor ='#440154'
    else:
        facecolor = 'None'
    sns.set(font_scale=1.2)
    ax = region_gdf.plot(facecolor=facecolor, linewidth = 0.01)
    if logscale is True:
        region_gdf.plot(
            figsize=(6, 4.5),
            ax=ax,
            column=column,
            norm=matplotlib.colors.LogNorm(vmin=0.001, vmax=vmax),
            alpha=alpha,
            legend=True,
            cmap=cmap,
            linewidth=0.01,
            legend_kwds={'shrink': legend_scale},
            edgecolor='none'
        )
    else:
        region_gdf.plot(
            figsize=(6, 4.5),
            ax = ax,
            column=column,
            vmin=vmin, vmax=vmax,
            alpha=alpha,
            legend=True,
            cmap=cmap,
            linewidth=0.01,
            legend_kwds={'shrink': legend_scale},
            edgecolor='none')

    ax.grid(False)
    ax.axis('off')
    plt.title(title)
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()

# kde plot for shipment distance
def plot_distance_kde(faf_data,  cfs_data, modeled_data, 
                      mode, filename):
    plt.figure(figsize=(6, 4.5))
    ax = sns.kdeplot(
        data=faf_data, x='Distance', 
        weights='Load', cut=0, color='blue'
    )
    sns.kdeplot(
        data=cfs_data, x='Distance', 
        weights='Load', cut=0, color='orange', ax=ax
    )
    sns.kdeplot(
        data=modeled_data, x='Distance', 
        weights='Load', cut=0, color='green', ax=ax
    )
    ax.set(facecolor="white")
    for spine in ['bottom', 'top', 'right', 'left']:
        ax.spines[spine].set_color('0.5')
    plt.xlim([0, 6000])
    plt.legend(['FAF distance', 'CFS2017 distance', 'Modeled distance'])
    plt.title('Mode = ' + mode)
    plt.xlabel('Distance (km)')
    plt.ylabel('Density')
    plt.savefig(filename, bbox_inches='tight', dpi=200)
    plt.show()  

# bar plot absolute shipment attributes by market segments

def plot_shipment_by_sector_bar(
    faf_outflow, cfs_outflow, modeled_outflow, agg_level, attr_name, attr_unit, filename):
    # Aggregate by SCTG/commodity
    FAF_shipment_by_sctg = \
        faf_outflow.groupby(agg_level)[[attr_name]].sum()
    
    CFS_shipment_by_sctg = \
        cfs_outflow.groupby(agg_level)[[attr_name]].sum()
    
    modeled_shipment_by_sctg = \
        modeled_outflow.groupby(agg_level)[[attr_name]].sum()
    
    # Merge all
    shipment_generation_by_sctg = pd.merge(
        FAF_shipment_by_sctg, CFS_shipment_by_sctg,
        left_index=True, right_index=True, how='left'
    )
    shipment_generation_by_sctg = pd.merge(
        shipment_generation_by_sctg, modeled_shipment_by_sctg,
        left_index=True, right_index=True, how='left'
    )
    
    output_cols = [
        'FAF ' + attr_name, 
        'CFS shipment' + attr_name, 
        'Modeled shipment' + attr_name
    ]
    shipment_generation_by_sctg.columns = output_cols

    # Unit conversion
    # for col in output_cols:
    #     shipment_generation_by_sctg.loc[:, col] *= us_ton_to_ton

    # Plot
    ax = shipment_generation_by_sctg.plot(
        y = output_cols,
        figsize=(6, 4.5), kind='bar',
        title=f'Shipment {attr_name} by {agg_level}'
    )
    ax.set(facecolor="white")
    for spine in ['bottom', 'top', 'right', 'left']:
        ax.spines[spine].set_color('0.5')
    plt.xticks(rotation = 60, ha = 'right')
    plt.xlabel('')
    plt.ylabel(f'Shipment {attr_name} ({attr_unit})')
    plt.legend(fontsize=12)
    plt.savefig(filename, bbox_inches='tight', dpi=300)
    plt.show()
    
# bar plot mode split against CFS
def plot_shipment_by_5mode_bar(
    cfs_outflow,
    modeled_outflow, agg_level, attr_name, attr_unit,
    filename
):
    """
    Compare and plot outbound shipment fractions by aggregated 5-mode categories.

    Parameters:
        cfs_outflow: pd.DataFrame, CFS outbound data
     
        modeled_outflow: pd.DataFrame, modeled outbound data
        agg_level: str, variable name for 5-mode definition
        attr_name: str, modeled shipment attributes
        attr_unit: str, unit
        filename: str, directory + file to save the plot

    """

    # Aggregate CFS
    cfs_outflow_by_5mode = \
        cfs_outflow.groupby([agg_level])[[attr_name]].sum().reset_index()
    
    cfs_outflow_by_5mode['fraction'] = \
        cfs_outflow_by_5mode[attr_name] / cfs_outflow_by_5mode[attr_name].sum()
    
    # Aggregate modeled
    agg_modeled_outflow_by_5mode = \
        modeled_outflow.groupby([agg_level])[[attr_name]].sum().reset_index()
    
    agg_modeled_outflow_by_5mode['fraction'] = \
        agg_modeled_outflow_by_5mode[attr_name] / agg_modeled_outflow_by_5mode[attr_name].sum()  

    # Merge
    compare_outflow_by_5mode = pd.merge(
        agg_modeled_outflow_by_5mode, cfs_outflow_by_5mode,
        on=agg_level, how='left'
    )
    # Rename columns for clarity
    compare_outflow_by_5mode.columns = [
        agg_level,
        'Modeled ' + attr_name, 'Modeled fraction',
        'CFS ' + attr_name, 'CFS fraction'
    ]

    # Plot
    ax = compare_outflow_by_5mode.plot(
        x=agg_level,
        y=['CFS fraction', 'Modeled fraction'],
        figsize=(6, 4.5), kind='bar',
        title=f'Outbound {attr_name} by {agg_level}'
    )
    vals = ax.get_yticks()
    ax.set_yticklabels(['{:,.2%}'.format(x) for x in vals])
    ax.set(facecolor="white")
    for spine in ['bottom', 'top', 'right', 'left']:
        ax.spines[spine].set_color('0.5')
    plt.xticks(rotation = 60, ha = 'right')
    plt.xlabel('')
    plt.ylabel(f'Shipment by {attr_name} ({attr_unit})')
    plt.legend(fontsize = 12)
    plt.savefig(filename, bbox_inches='tight', dpi=300)
    plt.show()

# categorical plot for shipment by zone    
def plot_shipment_comparison_by_zone(
    faf_outflow, 
    cfs_outflow, 
    modeled_outflow, 
    agg_level, agg_level_name, attr_name, attr_unit,
    filename
):
    """
    Compares and plots shipment load by origin zone from FAF, CFS, and modeled data.
    Saves plot and output CSV.
    """
    # FAF aggregation
    agg_faf_outflow_by_zone = \
        faf_outflow.groupby([agg_level])[[attr_name]].sum().reset_index()
    # CFS aggregation
    cfs_outflow_by_zone = \
        cfs_outflow.groupby([agg_level])[[attr_name]].sum()
    # Modeled aggregation
    agg_modeled_outflow_by_zone = \
        modeled_outflow.groupby([agg_level, agg_level_name])[[attr_name]].sum().reset_index()

    # Merge FAF and modeled
    compare_outflow_by_zone = pd.merge(
        agg_faf_outflow_by_zone, cfs_outflow_by_zone,
        on = agg_level,  how='left'
    )
    metric_1 = attr_name + '_x'
    metric_2 = attr_name + '_y'
    compare_outflow_by_zone.rename(columns = {metric_1: 'FAF ' + attr_name,
                                              metric_2: 'CFS ' + attr_name},
                                   inplace = True)
    # Merge CFS
    compare_outflow_by_zone = pd.merge(
        compare_outflow_by_zone, agg_modeled_outflow_by_zone,
        on = agg_level, how='left'
    )

    compare_outflow_by_zone.rename(columns = {attr_name: 'Modeled ' + attr_name},
                                   inplace = True)

    # Melt for plotting
    output_metric = f'{attr_name} ({attr_unit})'
    compare_outflow_by_zone = pd.melt(
        compare_outflow_by_zone,
        id_vars=[agg_level, agg_level_name],
        value_vars=['FAF ' + attr_name, 'Modeled ' + attr_name, 'CFS ' + attr_name],
        var_name='Source', value_name=output_metric, ignore_index=False
    )
    compare_outflow_by_zone = compare_outflow_by_zone.reset_index()
    # Plot
    
    ax = sns.catplot(
        data=compare_outflow_by_zone, kind="bar",
        x="Source", hue="Source", y = output_metric, col = agg_level_name, col_wrap=2,
        alpha=.8, height=4, aspect=1.4
    )
    ax.set_titles("{col_name}")
    for axn in ax.axes.flat:
        for label in axn.get_xticklabels():
            label.set_rotation(30)
    plt.savefig(filename, bbox_inches='tight', dpi=300)
    plt.show()
    
    
def plot_top_OD_bar(
    faf_outflow,    
    cfs_outflow, 
    modeled_outflow, 
    nzones, agg_level, agg_level_name, attr_name, attr_unit,
    filename

):
    """
    Compare and plot outbound shipment fractions by destination zone
    for FAF, CFS, and modeled data (top N zones).
    """
    # FAF aggregation
    faf_flow_by_zone = \
        faf_outflow.groupby([agg_level])[[attr_name]].sum().reset_index()

    # CFS aggregation
    cfs_flow_by_zone = \
    cfs_outflow.groupby([agg_level])[[attr_name]].sum()
    
    # Modeled aggregation
    print(modeled_outflow.columns)
    modeled_flow_by_zone = \
        modeled_outflow.groupby([agg_level, agg_level_name])[[attr_name]].sum().reset_index()
    
    # Merge FAF and modeled
    compare_flow_by_zone = pd.merge(
        faf_flow_by_zone, cfs_flow_by_zone,
        on = agg_level, how='left'
    )
    # Merge CFS
    compare_flow_by_zone = pd.merge(
        compare_flow_by_zone, modeled_flow_by_zone,
        on = agg_level, how='left'
    )
    # Rename columns
    metric_1 = attr_name + '_x'
    metric_2 = attr_name + '_y'
    compare_flow_by_zone.rename(columns={
        metric_1: 'FAF ' + attr_name,
        metric_2: 'CFS ' + attr_name,
        attr_name: 'Modeled ' + attr_name
    }, inplace = True)
    # Normalize to fractions
    compare_flow_by_zone['FAF ' + attr_name] /= compare_flow_by_zone['FAF ' + attr_name].sum()
    compare_flow_by_zone['Modeled ' + attr_name] /= compare_flow_by_zone['Modeled ' + attr_name].sum()
    compare_flow_by_zone['CFS ' + attr_name] /= compare_flow_by_zone['CFS ' + attr_name].sum()
    # Drop missing
    compare_flow_by_zone.dropna(inplace=True)
    # Sort and select top zones
    compare_flow_by_zone = compare_flow_by_zone.sort_values('FAF ' + attr_name, ascending=False)
    compare_flow_by_zone = compare_flow_by_zone.head(nzones)
    # Plot
    ax = compare_flow_by_zone.plot.barh(
        x= agg_level_name,
        y=['FAF ' + attr_name, 'CFS ' + attr_name, 'Modeled ' + attr_name],
        figsize=(7, 10), rot=0
    )
    vals = ax.get_xticks()
    ax.set_xticklabels(['{:,.1%}'.format(x) for x in vals])
    plt.xlabel(f'Fraction of shipment by {attr_name} ({attr_unit})')
    plt.ylabel('')
    plt.savefig(filename, bbox_inches='tight', dpi=200)
    plt.show()