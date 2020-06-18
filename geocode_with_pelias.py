"""This is a script run on a server with an active Pelias instance, which allows
a CSV file full of addresses to be geocoded, resulting in an output file that 
includes "latitude" and "longitude" fields but a variety of parameters coding
the details of the geocoding process for each record, such as confidence and accuracy."""
# Historcal note: This script was originally called "geocode_it.py".
import requests, csv, sys, os, time
from pprint import pprint

class pyPelias:
    def __init__(self, http_url):
        self.http_url = http_url

    def geocode(self, query_string):
        r = requests.get(self.http_url + '/v1/search?text='+query_string)
        json = r.json()
        try:
            loc = json['features'][0]['geometry']['coordinates']
            return json
        except IndexError:
            raise Exception('could not parse location text')

    def reverse(self, lat_long):
        r = requests.get(self.http_url + '/v1/reverse?point.lon='+lat_long[1]+'&point.lat='+lat_long[0])

        return r.json()

def write_or_append_to_csv(filename, list_of_dicts, keys):
    if not os.path.isfile(filename):
        with open(filename, 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
            dict_writer.writeheader()
    with open(filename, 'a') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writerows(list_of_dicts)

def form_full_address(row):
    maybe_malformed = False
    if 'STREET_ADDRESS' in row:
        street_address = row['STREET_ADDRESS']
    elif 'ADD_LINE_1' in row:
        street_address = row['ADD_LINE_1']
        if 'ADD_LINE_2' in row and row['ADD_LINE_2'] != '':
            if street_address == '':
                street_address = row['ADD_LINE_2']
                maybe_malformed = True
            else:
                street_address += ', ' + row['ADD_LINE_2']
    #if row['CITY'] == '':

    return f"{street_address}, {row['CITY']}, {row['STATE']} {row['ZIP']}"


gisAPI = pyPelias('http://127.0.0.1')
test = True
if test:
    # reverse geocode - get the location from lat long, in format ['lat', 'long']
    loc = gisAPI.reverse(['45.533467', '-122.650095'])

    print(loc)

    # geocode - takes address, city, state, country in any form, raises exception if can't parse address
    geocode = gisAPI.geocode('Alamogordo, New Mexico, United States')

    print(geocode)
else:
    if len(sys.argv) < 2:
        raise ValueError("Please specify the filename as the 1st command-line argument.")

    filename = sys.argv[1]
    output_filename = f"geocoded-{filename}"
    reader = csv.DictReader(open(filename))

    #reader = csv.DictReader(open(filename))
    headers = reader.fieldnames
    parameters = ['name', 'label', 'county', 'localadmin', 'confidence', 'accuracy']
    headers += ['latitude', 'longitude', 'full_address'] + parameters + ['error'] # 'geocoding_response']

    rows = []
    for k, row in enumerate(reader):
        full_address = form_full_address(row)
        row['full_address'] = full_address
        try:
            geocoding_response = gisAPI.geocode(full_address)
        except Exception as e:
            if full_address[:3] != ', ,':
                print(f"Unable to parse {full_address}")
            row['error'] = 'Unable to parse address'
        finally:
            features = geocoding_response['features']
            if k == 0:
                for f in features:
                    pprint(f)
            first_feature = features[0]
            if first_feature['properties']['name'] == 'Clinton County' and first_feature['properties']['confidence'] < 0.5 and row['CITY'].upper() == 'CLINTON':
                # Search through features and find the right one.
                f = None
                for f in features:
                    if 'localadmin' in f['properties'] and f['properties']['localadmin'] == 'Findlay Township':
                        chosen_f = f
                if f is not None:
                    first_feature = chosen_f
            elif first_feature['properties']['confidence'] == 0.6 and first_feature['properties']['accuracy'] == 'centroid' and first_feature['properties']['name'] == 'Pittsburgh':
                # This is the "postal cities" problem (a.k.a., the everybody-puts-Pittsburgh-as-the-mailing-city-though-they-live-outside-Pittsburgh problem).
                # See https://github.com/pelias/lastline for another solution.
                row['CITY'] = ''
                full_address = form_full_address(row)
                row['full_address'] = full_address
                row['error'] = "Routing around the problem of the 'Pittsburgh' city for non-Pittsburgh residents by blanking the city."
                try:
                    geocoding_response = gisAPI.geocode(full_address)
                except Exception as e:
                    if full_address[:3] != ', ,':
                        print(f"Unable to parse {full_address}")
                    row['error'] = 'Unable to parse address'
                finally:
                    features = geocoding_response['features']
                    first_feature = features[0]

                # This solution narrows to a smaller region though about 85% of the time, it's just the ZIP code.
                # However, in 1 of 2 attempts, substituting the inferred name back in as the city name was enough to get point accuracy (while in the other case it resulted in centroid
                # accuracy and a confidence of 0.6 again (though the confidence had been 0.8 in the intermediate step).
                    
            if 'error' not in row or row['error'] != 'Unable to parse address':
                row['latitude'] = first_feature['geometry']['coordinates'][1]
                row['longitude'] = first_feature['geometry']['coordinates'][0]
                for parameter in parameters:
                    if parameter in first_feature['properties']:
                        row[parameter] = first_feature['properties'][parameter]

        rows.append(row)
        time.sleep(0.00001)
        if k % 100 == 0:
            print(f"On record {k}")
            write_or_append_to_csv(output_filename, rows, headers)
            rows = []
    
    write_or_append_to_csv(output_filename, rows, headers)
