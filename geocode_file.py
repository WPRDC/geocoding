import sys, re, csv, json, time
import ckanapi

from pprint import pprint

def query_resource(site,query,API_key=None):
    # Use the datastore_search_sql API endpoint to query a CKAN resource.


    # Note that this doesn't work for private datasets.
    # The relevant CKAN GitHub issue has been closed.
    # https://github.com/ckan/ckan/issues/1954
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search_sql(sql=query)
    # A typical response is a dictionary like this
    #{u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'_full_text', u'type': u'tsvector'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'records': [{u'_full_text': u"'0001b00010000000':1 '11':2 '13585.47':3",
    #               u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_full_text': u"'0001c00058000000':3 '2':2 '7827.64':1",
    #               u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_full_text': u"'0001c01661006700':3 '1':1 '3233.59':2",
    #               u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}]
    # u'sql': u'SELECT * FROM "d1e80180-5b2e-4dab-8ec3-be621628649e" LIMIT 3'}
    data = response['records']
    return data

def lookup_parcel(parcel_id):
    site = "https://data.wprdc.org"
    resource_id = '23267115-177e-4824-89d9-185c7866270d' #2018 data
    resource_id = "4b68a6dd-b7ea-4385-b88e-e7d77ff0b294" #2016 data
    query = 'SELECT x, y FROM "{}" WHERE "PIN" = \'{}\''.format(resource_id,parcel_id)
    results = query_resource(site,query)
    assert len(results) < 2
    if len(results) == 0:
        return None, None
    elif len(results) == 1:
        return results[0]['y'], results[0]['x']

def standardize_parcel_id(p_id):
    if len(p_id) == 0:
        return None
    p_id = re.sub('-','',p_id)
    if len(p_id) != 16:
        print("Nonstandard parcel ID (length = {}): {}".format(len(p_id),p_id))
        assert len(p_id) == 16
    return p_id

if len(sys.argv) == 1:
    print("The first argument should be the input filename.")
else:
    filename = sys.argv[1]
    name, extension = str(filename).lower().split('.')
    assert extension == 'csv'
    outfilename = "{}-geocoded.{}".format(name,extension)
    rows = []
    with open(filename,'r') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        if '_id' in headers:
            headers.remove('_id')
        for k,row in enumerate(reader):
            parcel_id = standardize_parcel_id(row['parcel_number'])
            if parcel_id is not None:
                latitude, longitude = lookup_parcel(parcel_id)
                if latitude is not None:
                    row['latitude'] = latitude
                    row['longitude'] = longitude
                    rows.append(row)

                time.sleep(0.1)
            if k % 20 == 0:
                print("k = {}".format(k))

    headers += ['latitude', 'longitude']
    with open(outfilename,'w') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
