import sys
import numpy as np
import pandas as pd
import os
import PIL
from PIL.ExifTags import TAGS, GPSTAGS
from xml.dom.minidom import parse
from collections import Counter
from glob import glob
import matplotlib.pyplot as plt
import folium
from folium import plugins
from folium.plugins import HeatMap
import requests
import webbrowser
from IPython.display import Image, display
import base64
from pyspark.sql import SparkSession, functions, types, Row, Window

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
spark = SparkSession.builder.appName('OSM point of interest extracter').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
spark.conf.set("spark.sql.session.timeZone", "UTC")


amenity_schema = types.StructType([
    types.StructField('lat', types.DoubleType(), nullable=False),
    types.StructField('lon', types.DoubleType(), nullable=False),
    types.StructField('timestamp', types.TimestampType(), nullable=False),
    types.StructField('amenity', types.StringType(), nullable=False),
    types.StructField('name', types.StringType(), nullable=True),
    types.StructField('tags', types.MapType(types.StringType(), types.StringType()), nullable=False),
])


def haversine(lon1, lat1, lon2, lat2, earth_radius=6371):
    """
    Modified from https://stackoverflow.com/questions/43577086/pandas-calculate-haversine-distance-within-each-group-of-rows/43577275
    :param earth_radius: an int
    :param lon1: a float
    :param lat1: a float
    :param lon2: a float
    :param lat2: a float
    :return:
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2) ** 2 + \
        np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0) ** 2

    return earth_radius * 2 * np.arcsin(np.sqrt(a))


def get_data(in_directory, amenity_schema=amenity_schema):
    vancouver_data = spark.read.json(in_directory, schema=amenity_schema)
    vancouver_data = vancouver_data.na.drop(subset=['name'])
    vancouver_data = vancouver_data.withColumn('amenity', functions.lower('amenity'))  
    w = Window.partitionBy('amenity')
    vancouver_data = vancouver_data.withColumn('count', functions.count('amenity').over(w))
    return vancouver_data


def eda(vancouver_data):
    # Dict format of number of amenities
    vancouver_data = vancouver_data.toPandas()
    amenities_list = set(vancouver_data['amenity'])
    counter_list = Counter(list(vancouver_data['amenity'])) 

    # Bar Graph of amenities
    vancouver_data_grouped = vancouver_data.groupby('amenity')['amenity'].agg(count='count')
    ax = vancouver_data_grouped.plot(kind='bar', figsize=(10,6), fontsize=8)
    ax.set_title("Count of Different Amenities in Vancouver")
    ax.set_ylabel("Count")
    plt.subplots_adjust(bottom=0.30)
    plt.savefig('assets/{}'.format("amenities_bar.png"))

    # Find attractions
    mask = vancouver_data['tags'].apply(lambda x: True if 'tourism' in x else False)
    vancouver_data_attr = vancouver_data[mask]
    
    # Heatmap of attractions
    latmean = vancouver_data_attr['lat'].mean()
    lonmean = vancouver_data_attr['lon'].mean()
    heat_df = vancouver_data_attr[['lat', 'lon']]
    heat_df = heat_df.dropna(axis=0)
    
    map = folium.Map(location=[latmean, lonmean])
    HeatMap(heat_df).add_to(folium.FeatureGroup(name='Heat Map').add_to(map))
    folium.LayerControl().add_to(map)
    map.save("map.html")

    webbrowser.open("map.html")


def gpx_read():
    row_data = []
    gpx_files = glob('gpx_data/*.gpx')
    for file in gpx_files:   
        xmldata = parse(file)
        trkpts = xmldata.getElementsByTagName("trkpt")
        for i in trkpts:
            row_data.append([file, float(i.getAttribute('lat')), float(i.getAttribute('lon'))])
    df = pd.DataFrame(row_data, columns=['source', 'gpx_lat', 'gpx_lon'])
    df1 = df[df['source'] == gpx_files[0]]
    return df1


def exif_load():
    pass


def main(in_directory):

    vancouver_data = get_data(in_directory)
    eda(vancouver_data)
    # gpx_data = gpx_read()

    # vancouver_data_merged = gpx_data.merge(vancouver_data, how="cross")
    # vancouver_data_merged['distance'] = vancouver_data_merged.apply(lambda x: haversine(x['gpx_lat'], x['gpx_lon'], x['lat'] , x['lon']), axis=1)
    # set(vancouver_data_merged[vancouver_data_merged['distance'] < 0.5]['amenity'])
    # vancouver_data.write.json(out_directory, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    in_directory = sys.argv[1]
    main(in_directory)
