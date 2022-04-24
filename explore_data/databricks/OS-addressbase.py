# Databricks notebook source
# MAGIC %md
# MAGIC # Addressbase sample
# MAGIC 
# MAGIC - obtained by signing up here: https://www.ordnancesurvey.co.uk/business-government/products/addressbase
# MAGIC - zip file uploaded to landing blob storage, and then unzipped to bronze zone of datalake using ADF.
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

# COMMAND ----------

# from azuregigdatalake
dataset = 'csv/OS/addressbase/sample'
dbutils.fs.ls(f'/mnt/bronze/{dataset}')

# COMMAND ----------

from pyspark.sql.types import *

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

# collect a sample for display

pdf = df.sample(0.3).toPandas()
print(f'Collecting {len(pdf)} records to the driver node. ({type(pdf)})')

# COMMAND ----------

import folium
import geopandas as gpd

gdf = gpd.GeoDataFrame(
    pdf, geometry=gpd.points_from_xy(pdf.LONGITUDE, pdf.LATITUDE))
gdf.head()

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

folium.GeoJson(points, 
               marker = folium.CircleMarker(radius = 3, # Radius in metres
                                           weight = 0, #outline weight
                                           fill_color = '#000000', 
                                           fill_opacity = 1),
               style_function = lambda x: {"color":"red", "weight":1, "opacity":1}, 
               name="addressbase", 
               tooltip = folium.features.GeoJsonTooltip(fields=['UPRN', "BUILDING_NUMBER", "THOROUGHFARE"])
              ).add_to(m)

folium.vector_layers.Polygon(bounds).add_to(m)    

# COMMAND ----------

m

# COMMAND ----------


