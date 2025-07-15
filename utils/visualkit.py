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

plt.style.use('seaborn-v0_8-white')
sns.set(font_scale=1.4)
sns.set_style("white")

def plot_emp_comparison_scatter(df, x_col, y_col, agg_level,
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
        
    # Determine axis limits with a 5% margin
    max_val = max(df[x_col].quantile(0.99), df[y_col].quantile(0.99))
    axis_max = max_val * 1.05
    
    # Plot
    sns.lmplot(
        data=df, x=x_col, y=y_col, 
        height=4.5, aspect=1.2, 
        line_kws={'color': 'grey'}, 
        scatter_kws={'alpha':0.3}
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
    plt.title(title)
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()

# define map plot by county
def plot_county_map(region_gdf, column, title,
                    filename, logscale = True, 
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
        logscale: boolean
            If use logscale for color ramp
        vmin, vmax: float
            Color scale min and max.
        alpha: float
            Polygon alpha.
        cmap: str
            Colormap.
    """

    sns.set(font_scale=1.2)
    ax = region_gdf.plot( facecolor='#440154', linewidth = 0.01)
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
            legend_kwds={'shrink': 0.8},
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
            legend_kwds={'shrink': 0.8},
            edgecolor='none')

    ax.grid(False)
    plt.title(title)
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.show()