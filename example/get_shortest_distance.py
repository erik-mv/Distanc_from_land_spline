import pickle
import pandas as pd
from pandarallel import pandarallel
from pyproj import Geod
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points

with open('linestring_list.list', "rb") as file:
    linestring_list = pickle.load(file)
    
shortest_distance = pd.read_feather('../all_lat_lon.feather')
shortest_distance['points'] = shortest_distance.apply(lambda x: Point(float(x['lat']), float(x['lon'])), axis=1)

geod = Geod(ellps="WGS84")
def closest_line(point):
    distance_list = [geod.geometry_length(LineString(nearest_points(point, line))) for line in linestring_list]
    shortest_distance = min(distance_list)
    return int(round(shortest_distance * 0.01))

pandarallel.initialize(progress_bar=False, nb_workers=30) # nb_workers
shortest_distance['shortest_distance'] = shortest_distance['points'].parallel_apply(closest_line)

shortest_distance.drop(['points'], axis=1, inplace=True)
shortest_distance.to_feather('../shortest_distance.feather')
