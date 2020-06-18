"""This script takes a file with records having latitude and longitude fields and tries to find each 
point in a shapefile representing the Census tract boundaries of the state of Pennsylvania. It then
appends any found tract code to the records and outputs the result as a new file. Basically, this is a
really slow, spatially nonindexed solution the only virtue of which is that it avoids having to set up a 
spatial index."""
# This is a brute-force approach.
# Faster better approaches would be based on Rtree spatial indexing:
#   https://snorfalorpagus.net/blog/2014/05/12/using-rtree-spatial-indexing-with-ogr/
# or just PostGIS-based spatial queries.
import csv, sys, time
from pprint import pprint

def write_to_csv(filename, list_of_dicts, keys):
    # Stolen from parking-data util.py file.
    with open(filename, 'w') as g:
        g.write(','.join(keys)+'\n')
    with open(filename, 'a') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        #dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

from osgeo import ogr

#Assumes your points and shapefile are already in the same datum/projection
shapefile_name = '/Users/drw/WPRDC/other/r3-geocoding/cb_2018_42_tract_500k/cb_2018_42_tract_500k.shp'

#This version takes a long_lat_list of the form below and a shapefile name
def getCensusTracts(long_lat_list, shapefile_name):
    driver = ogr.GetDriverByName("ESRI Shapefile")
    dataSource = driver.Open(shapefile_name, 0)
    layer = dataSource.GetLayer()
    results_dict = {}
    i = 0
    for feature in layer:
        geom = feature.GetGeometryRef()
        i += 1
        for pt in long_lat_list:
            gid = pt[0]
            lon = pt[1]
            lat = pt[2]
            point = ogr.Geometry(ogr.wkbPoint)
            point.AddPoint(lon, lat)
            if point.Within(geom) == True:
                feat_id = feature.GetField("fips")
                if gid in results_dict and feat_id not in results_dict[gid]:
                    results_dict[gid].append(feat_id)
                else:
                    results_dict[gid] = [feat_id]
    for pt in long_lat_list:
        gid = pt[0]
        lon = pt[1]
        lat = pt[2]
        if gid not in results_dict:
            results_dict[gid] = ['NA']
    return results_dict

def get_tract(latitude, longitude, shapefile_name):
    driver = ogr.GetDriverByName("ESRI Shapefile")
    dataSource = driver.Open(shapefile_name, 0)
    layer = dataSource.GetLayer()
    results_dict = {}
    i = 0
    for feature in layer:
        geom = feature.GetGeometryRef()
        i += 1
        point = ogr.Geometry(ogr.wkbPoint)
        point.AddPoint(longitude, latitude)
        if point.Within(geom) == True:
            geoid = feature.GetField("geoid")
            return geoid
    return None

def batch_get_tracts_unoptimized(latitudes, longitudes, shapefile_name):
    driver = ogr.GetDriverByName("ESRI Shapefile")
    dataSource = driver.Open(shapefile_name, 0)
    layer = dataSource.GetLayer()
    tracts = []
    for latitude, longitude in zip(latitudes, longitudes):
        so_far = len(tracts)
        if so_far > 0 and so_far % 1000 == 0:
            print(f"Processed {so_far} so far.")
        geoid = None
        if latitude is not None:
            for feature in layer:
                geom = feature.GetGeometryRef()
                point = ogr.Geometry(ogr.wkbPoint)
                point.AddPoint(longitude, latitude)
                if point.Within(geom) == True:
                    geoid = feature.GetField("geoid")
        tracts.append(geoid)

    return tracts

def batch_get_tracts_faster_but_segfaults(latitudes, longitudes, shapefile_name):
    # This version of the function tries to optimize the process by reordering the
    # polygons, but it results in a segmenetation fault, probably because of something
    # dumb that ogr or shapefiles is doing.
    driver = ogr.GetDriverByName("ESRI Shapefile")
    dataSource = driver.Open(shapefile_name, 0)
    layer = dataSource.GetLayer()
    tracts = []
    geoms_by_tract = {}
    counties_by_tract = {}
    for feature in layer:
        geom = feature.GetGeometryRef()
        tract = feature.GetField("geoid")
        county = feature.GetField("countyfp")
        counties_by_tract[tract] = county
        geoms_by_tract[tract] = geom

    print(len(counties_by_tract))
    print(len(geoms_by_tract))

    i = 0
    for latitude, longitude in zip(latitudes, longitudes):
        print(i)
        from pudb import set_trace; set_trace()
        geoid = None
        for tract, county in sorted(counties_by_tract.items(), key=lambda item: item[1]): # Sort tracts by county code
            # to bubble 003 (Allegheny County) to near the top.
            geom = geoms_by_tract[tract]
            point = ogr.Geometry(ogr.wkbPoint)
            point.AddPoint(longitude, latitude)
            if point.Within(geom) == True:
                geoid = tract
        tracts.append(geoid)
        if i % 10000 == 0 and i > 0:
            print(f"Working on record {k}")
        i += 1
    return tracts

test = False
if test:
    pass
else:
    if len(sys.argv) < 2:
        raise ValueError("Please specify the filename as the 1st command-line argument.")

    filename = sys.argv[1]
    output_filename = f"with-tracts-{filename}"
    reader = csv.DictReader(open(filename))

    #reader = csv.DictReader(open(filename))
    headers = reader.fieldnames
    print(headers)

    rows = []
    latitudes = []
    longitudes = []
    for k, row in enumerate(reader):
        if 'latitude' in row and row['latitude'] != '':
            latitudes.append(float(row['latitude']))
            longitudes.append(float(row['longitude']))
        else:
            latitudes.append(None)
            longitudes.append(None)
        rows.append(row)
        #time.sleep(0.0001)
        #if k % 10000 == 0:
        #    print(f"On record {k}")

    print("Getting tracts")
    tracts = batch_get_tracts_unoptimized(latitudes, longitudes, shapefile_name)
    print("Adding tracts to records")
    for row, tract in zip(rows, tracts):
        row['census_tract'] = tract
    
    headers += ['census_tract']

    write_to_csv(output_filename, rows, headers)
