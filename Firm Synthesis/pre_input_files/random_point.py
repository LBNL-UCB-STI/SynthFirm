import pandas as pd
import geopandas as gpd
from os.path import exists as file_exists
from shapely.geometry import Point
from alive_progress import alive_bar
import random

def random_points_in_polygon(polygon):
    temp = polygon.bounds
    finished= False
    while not finished:
        point = Point(random.uniform(temp.minx.values[0], temp.maxx.values[0]), random.uniform(temp.miny, temp.maxy.values[0]))
        finished = polygon.contains(point).values[0]
    return [point.x, point.y]


firm_file= 'synthfirms_all_Sep.csv'
warehouse_file= 'warehouses_all_Sep.csv'
fdir_firms='../../../FRISM_input_output/Sim_inputs/Synth_firm_pop/'
fdir_geo='../../../FRISM_input_output/Sim_inputs/Geo_data/'
CBG_file= 'sfbay_freight.geojson'
list_error_zone=[1047.0, 1959.0, 1979.0, 2824.0, 3801.0, 3897.0, 4303.0, 6252.0, 6810.0, 7273.0, 8857.0, 9702.0] # this only need if there is empty geometry in the data sets


CBGzone_df = gpd.read_file(fdir_geo+CBG_file) # file include, GEOID(12digit), MESOZONE, area
#CBGzone_df=CBGzone_df[['GEOID','CBPZONE','MESOZONE','area']]
CBGzone_df["GEOID"]=CBGzone_df["GEOID"].astype(str)
## Add county id from GEOID
CBGzone_df["County"]=CBGzone_df["GEOID"].apply(lambda x: x[2:5] if len(x)>=12 else 0)
CBGzone_df["County"]=CBGzone_df["County"].astype(str).astype(int)
CBGzone_df["GEOID"]=CBGzone_df["GEOID"].astype(str).astype(int)

firm_file_xy=fdir_firms+"xy"+firm_file
if file_exists(firm_file_xy):
    firms=pd.read_csv(firm_file_xy, header=0, sep=',')
else:
    print ("**** Generating x_y to firms file")        
    firms= pd.read_csv(fdir_firms+firm_file, header=0, sep=',')
    firms=firms[~firms['MESOZONE'].isin(list_error_zone)]
    firms=firms.reset_index()
    firms=firms.rename({'BusID':'SellerID'}, axis='columns')
    firms=firms.reset_index()
    firms['x']=0
    firms['y']=0
    with alive_bar(firms.shape[0], force_tty=True) as bar:
        for i in range(0,firms.shape[0]):
            [x,y]=random_points_in_polygon(CBGzone_df.geometry[CBGzone_df.MESOZONE==firms.loc[i,"MESOZONE"]]) #@Xiaodan, this is the key function, so you can plug in this into your code
            firms.loc[i,'x']=x
            firms.loc[i,'y']=y
            bar()
    firms.to_csv(firm_file_xy, index = False, header=True)

wh_file_xy=fdir_firms+"xy"+warehouse_file
if file_exists(wh_file_xy):
    warehouses=pd.read_csv(wh_file_xy, header=0, sep=',')
else:
    print ("**** Generating x_y to warehouses file")         
    warehouses= pd.read_csv(fdir_firms+warehouse_file, header=0, sep=',')
    warehouses=warehouses[~warehouses['MESOZONE'].isin(list_error_zone)]
    warehouses=warehouses[(warehouses['Industry_NAICS6_Make']=="492000") | (warehouses['Industry_NAICS6_Make']=="484000")]
    warehouses=warehouses.reset_index()
    warehouses['x']=0
    warehouses['y']=0
    with alive_bar(warehouses.shape[0], force_tty=True) as bar:
        for i in range(0,warehouses.shape[0]):
            [x,y]=random_points_in_polygon(CBGzone_df.geometry[CBGzone_df.MESOZONE==warehouses.loc[i,"MESOZONE"]])
            warehouses.loc[i,'x']=x
            warehouses.loc[i,'y']=y
            bar()        
    warehouses.to_csv(wh_file_xy, index = False, header=True)    