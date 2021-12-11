import sys
import numpy as np
import pandas as pd
import os
from xml.dom.minidom import parse
import matplotlib.pyplot as plt
from collections import Counter
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


def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation

    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.8f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.8f' % (pt['lon']))
        trkseg.appendChild(trkpt)
    
    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)
    
    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)
    
    with open(output_filename, 'w') as fh:
        doc.writexml(fh, indent=' ')


def get_data(in_directory, amenity_schema=amenity_schema):
    vancouver_data = spark.read.json(in_directory, schema=amenity_schema)
    vancouver_data = vancouver_data.na.drop(subset=['name'])
    # vancouver_data = vancouver_data.drop('timestamp')  # Not used as part of our dataset
    vancouver_data = vancouver_data.withColumn('amenity', functions.lower('amenity'))  
    w = Window.partitionBy('amenity')
    vancouver_data = vancouver_data.withColumn('count', functions.count('amenity').over(w))
    return vancouver_data


def eda(vancouver_data):
    # Dict format of number of amenities
    vancouver_data = vancouver_data.toPandas()
    amenities_list = set(vancouver_data['amenity'])
    counter_list = Counter(list(vancouver_data['amenity'])) 
    print(counter_list)  # We can see various amenities, mainly restaurants

    # Bar Graph of amenities
    vancouver_data_grouped = vancouver_data.groupby('amenity')['amenity'].agg(count='count')
    ax = vancouver_data_grouped.plot(kind='bar', figsize=(10,6), fontsize=8)
    ax.set_title("Count of Different Amenities in Vancouver")
    ax.set_ylabel("Count")
    plt.subplots_adjust(bottom=0.30)
    # plt.show()

    # Find attractions
    mask = vancouver_data['tags'].apply(lambda x: True if 'tourism' in x else False)
    vancouver_data_attr = vancouver_data[mask]
    
    print(vancouver_data_attr)


def main(in_directory, out_directory):

    vancouver_data = get_data(in_directory)
    eda(vancouver_data)

    # vancouver_data.write.json(out_directory, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
