import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row
spark = SparkSession.builder.appName('OSM point of interest extracter').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')


amenity_schema = types.StructType([
    types.StructField('lat', types.DoubleType(), nullable=False),
    types.StructField('lon', types.DoubleType(), nullable=False),
    types.StructField('timestamp', types.TimestampType(), nullable=False),
    types.StructField('amenity', types.StringType(), nullable=False),
    types.StructField('name', types.StringType(), nullable=True),
    types.StructField('tags', types.MapType(types.StringType(), types.StringType()), nullable=False),
])


def main(in_directory, out_directory):

    weather = spark.read.json(in_directory, schema=amenity_schema)
    weather.show()

    # cleaned_data = weather.filter(weather['qflag'].isNull())
    # cleaned_data = cleaned_data.filter(cleaned_data['station'].startswith('CA'))
    # cleaned_data = cleaned_data.filter(cleaned_data['observation'] == 'TMAX')
    # cleaned_data = cleaned_data.withColumn('tmax', cleaned_data['value'] / 10)
    #
    # cleaned_data = cleaned_data.select(
    #     cleaned_data['station'],
    #     cleaned_data['date'],
    #     cleaned_data['tmax'],
    # )
    #
    # cleaned_data.write.json(out_directory, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
