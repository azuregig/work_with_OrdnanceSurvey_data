# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC # Addressbase sample
# MAGIC 
# MAGIC - obtained by signing up here: https://www.ordnancesurvey.co.uk/business-government/products/addressbase
# MAGIC - zip file uploaded to landing blob storage, and then unzipped to bronze zone of datalake using Azure Data Factory.
# MAGIC 
# MAGIC No header in sample. Header downloaded separately from https://www.ordnancesurvey.co.uk/business-government/tools-support/addressbase-support (accessed 2022-04-24)
# MAGIC 
# MAGIC ```
# MAGIC UPRN,	OS_ADDRESS_TOID	UDPRN,	ORGANISATION_NAME,	DEPARTMENT_NAME,	PO_BOX_NUMBER,	SUB_BUILDING_NAME,	BUILDING_NAME,	BUILDING_NUMBER,	DEPENDENT_THOROUGHFARE,	
# MAGIC THOROUGHFARE,	POST_TOWN,	DOUBLE_DEPENDENT_LOCALITY,	DEPENDENT_LOCALITY,	POSTCODE,	POSTCODE_TYPE,	X_COORDINATE,	Y_COORDINATE,	LATITUDE,	LONGITUDE,	
# MAGIC RPC,	COUNTRY,	CHANGE_TYPE,	LA_START_DATE,	RM_START_DATE,	LAST_UPDATE_DATE,	CLASS
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC Technical Specification: https://www.ordnancesurvey.co.uk/documents/product-support/tech-spec/addressbase-technical-specification.pdf
# MAGIC 
# MAGIC #### About the sample
# MAGIC - 42861 records
# MAGIC - Location: Exeter
# MAGIC - CRS (Lat and long columns): ETRS89 (https://epsg.io/4258)
# MAGIC - CRS (X and Y columns): BNG (https://epsg.io/27700)
# MAGIC 
# MAGIC This image shows a 10% sample of the data within the sample bounding box:
# MAGIC 
# MAGIC ![addressbase sample area](https://github.com/azuregig/work_with_OrdnanceSurvey_data/raw/dev/addressbase/explore_data/img/addressbase-10pc.png)
# MAGIC 
# MAGIC This image shows the a small area of the full dataset:
# MAGIC 
# MAGIC ![addressbase](https://github.com/azuregig/work_with_OrdnanceSurvey_data/raw/dev/addressbase/explore_data/img/addressbase-full.png)

# COMMAND ----------

# from azuregigdatalake
dataset = 'csv/OS/addressbase/sample'
dbutils.fs.ls(f'/mnt/bronze/{dataset}')

# COMMAND ----------

from pyspark.sql.types import *

# construct schema from the information in the separate header file
schema = StructType([
    StructField("UPRN",StringType(), True),
    StructField("OS_ADDRESS_TOID",StringType(), True),
    StructField("UDPRN",IntegerType(), True),
    StructField("ORGANISATION_NAME",StringType(), True),
    StructField("DEPARTMENT_NAME",StringType(), True),
    StructField("PO_BOX_NUMBER",StringType(), True),
    StructField("SUB_BUILDING_NAME",StringType(), True),
    StructField("BUILDING_NAME",StringType(), True),
    StructField("BUILDING_NUMBER",IntegerType(), True),
    StructField("DEPENDENT_THOROUGHFARE",StringType(), True),
    StructField("THOROUGHFARE",StringType(), True),
    StructField("POST_TOWN",StringType(), True),
    StructField("DOUBLE_DEPENDENT_LOCALITY",StringType(), True),
    StructField("DEPENDENT_LOCALITY",StringType(), True),
    StructField("POSTCODE",StringType(), True),
    StructField("POSTCODE_TYPE",StringType(), True),
    StructField("X_COORDINATE",DoubleType(), True),
    StructField("Y_COORDINATE",DoubleType(), True),
    StructField("LATITUDE",DoubleType(), True),
    StructField("LONGITUDE",DoubleType(), True),
    StructField("RPC",StringType(), True),
    StructField("COUNTRY",StringType(), True),
    StructField("CHANGE_TYPE",StringType(), True),
    StructField("LA_START_DATE",DateType(), True),
    StructField("RM_START_DATE",DateType(), True),
    StructField("LAST_UPDATE_DATE",DateType(), True),
    StructField("CLASS",StringType(), True)
])

# COMMAND ----------

df = spark.read.csv(f'/mnt/bronze/{dataset}', header=False, schema=schema)

# COMMAND ----------

df.head()

# COMMAND ----------

records = df.count()
print(f'The sample contains {records} records.')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single-threaded exploration of a sample
# MAGIC 
# MAGIC When we call `toPandas()` on a spark dataframe, the data is collected onto the driver node. Any further processing on the pandas dataframe will proceed on the driver node only. We can use standard python libraries such as `folium` and `geopandas` on this sample.

# COMMAND ----------

# collect a random sample for display

pdf = df.sample(0.001).toPandas()
print(f'Collecting {len(pdf)} records to the driver node. ({type(pdf)})')

# COMMAND ----------

# or frame a smaller sample for display
# using non-geospatially aware filtering

# pdf = df.filter(col("POSTCODE").like('EX1 3%')).toPandas()
                 
# print(f'Collecting {len(pdf)} records to the driver node. ({type(pdf)})')

# COMMAND ----------

# or filter naively by lat long (not using any geospatial tooling here)

# ymin, ymax, xmin,xmax = [50.7227, 50.7327, -3.5461, -3.5268]
# pdf = df.filter(col("LATITUDE").between(ymin, ymax) & col("LONGITUDE").between(xmin,xmax)).toPandas()
                 
# print(f'Collecting {len(pdf)} records to the driver node. ({type(pdf)})')

# COMMAND ----------

import folium
import geopandas as gpd

gdf = gpd.GeoDataFrame(
    pdf, geometry=gpd.points_from_xy(pdf.LONGITUDE, pdf.LATITUDE))
gdf.head(1)

# COMMAND ----------

# The CRS will be missing - let's set it. From the docs, we know that the Lat/Long data we used to create the geometry column is in ETRS89 (https://epsg.io/4258)
gdf = gdf.set_crs(4258)
gdf.crs

# COMMAND ----------

# reproject data to wgs84 for folium
points = gdf.to_crs(4326)
points = points.drop(columns=['LA_START_DATE','RM_START_DATE','LAST_UPDATE_DATE'])

x1, y1, x2, y2 = points.total_bounds
bounds = [[y1,x1],[y1,x2], [y2,x2],[y2,x1]]

# COMMAND ----------

m = folium.Map(location=[points.geometry.representative_point().y.mean(), points.geometry.representative_point().x.mean()],
               zoom_start = 15, width = '80%', height = '100%')

# ensure we can click the map to get a lat long
m.add_child(folium.LatLngPopup())


folium.GeoJson(points, 
               marker = folium.CircleMarker(radius = 3, # Radius in metres
                                           weight = 0, #outline weight
                                           fill_color = '#000000', 
                                           fill_opacity = 1),
               style_function = lambda x: {"color":"red", "weight":1, "opacity":1}, 
               name="addressbase", 
               tooltip = folium.features.GeoJsonTooltip(fields=['UPRN', "BUILDING_NUMBER", "THOROUGHFARE", "POSTCODE"])
              ).add_to(m)

folium.vector_layers.Polygon(bounds).add_to(m)    

# COMMAND ----------

m

# COMMAND ----------


