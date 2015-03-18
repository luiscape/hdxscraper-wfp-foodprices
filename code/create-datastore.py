# Simple script that manages the creation of
# datastores in CKAN / HDX.

import os
import csv
import json
import scraperwiki
import ckanapi
import urllib
import requests
import sys
import hashlib

# Collecting configuration variables
PATH = 'tool/data/temp.csv'
remote = 'https://test-data.hdx.rwlabs.org'
resource_id = sys.argv[1]
apikey = sys.argv[2]

# ckan will be an instance of ckan api wrapper
ckan = None

# Function to download a resource from CKAN.
def downloadResource(filename):

    print "Downloading file from CKAN."

    # querying
    url = 'https://data.hdx.rwlabs.org/api/action/resource_show?id=' + resource_id
    r = requests.get(url)
    doc = r.json()
    fileUrl = doc["result"]["perma_link"]

    # downloading
    try:
        urllib.urlretrieve(fileUrl, filename)
    except:
        print 'There was an error downlaoding the file.'

def uploadResource(resource_id, apikey, p):
    hdx = ckanapi.RemoteCKAN('https://data.hdx.rwlabs.org',
        apikey=apikey,
        user_agent='CKAN_API/1.0')
    try:
        hdx.action.resource_update(
            id=resource_id,
            upload=open(p),
            format='CSV',
            description='Ebola data in record format with indicator, country, date and value.'
            )

    except ckanapi.errors.ValidationError:
            print 'You have missing parameters. Check the url and type are included.\n'

    except ckanapi.errors.NotFound:
            print 'Resource not found!\n'


# Function that checks for old SHA hash
# and stores as a SW variable the new hash
# if they differ. If this function returns true,
# then the datastore is created.
def checkHash(filename, first_run):
    hasher = hashlib.sha1()
    with open(filename, 'rb') as afile:
        buf = afile.read()
        hasher.update(buf)
        new_hash = hasher.hexdigest()

    # checking if the files are identical or if
    # they have changed
    if first_run:
        scraperwiki.sqlite.save_var('datastore', new_hash)
        new_data = False

    else:
        old_hash = scraperwiki.sqlite.get_var('datastore')
        scraperwiki.sqlite.save_var('datastore', new_hash)
        new_data = old_hash != new_hash

    # returning a boolean
    return new_data

def updateDatastore(filename):
    print "Updating DataStore ..."

    # Checking if there is new data
    new_data = checkHash(filename, first_run = False)
    if (new_data == False):
        print "DataStore Status: No new data. Not updating datastore."
        return

    else:
        print "DataStore Status: New data. Updating datastore."

        # defining the schema
        resources = [
            {
                'resource_id': resource_id,
                'path': filename,
                'schema': {
                    "fields": [
                        { "id" : "ADM0_ID", "type" : "integer" },
                        { "id" : "ADM0_NAME", "type" : "text" },
                        { "id" : "ADM1_ID", "type" : "integer" },
                        { "id" : "ADM1_NAME", "type" : "text" },
                        { "id" : "mkt_id", "type" : "integer" },
                        { "id" : "mkt_name", "type" : "text" },
                        { "id" : "cm_id", "type" : "integer" },
                        { "id" : "cm_name", "type" : "text" },
                        { "id" : "cur_id", "type" : "integer" },
                        { "id" : "cur_name", "type" : "text" },
                        { "id" : "pt_id", "type" : "integer" },
                        { "id" : "pt_name", "type" : "text" },
                        { "id" : "um_id", "type" : "integer" },
                        { "id" : "um_name", "type" : "text" },
                        { "id" : "mp_month", "type" : "text" },
                        { "id" : "mp_year", "type" : "integer" },
                        { "id" : "mp_price", "type" : "float" }
                    ]
                },
                'indexes': ["ADM0_ID","ADM0_NAME","ADM1_ID","ADM1_NAME","mkt_id","mkt_name"]
            }
        ]


        def upload_data_to_datastore(ckan_resource_id, resource):
            # let's delete any existing data before we upload again
            try:
                ckan.action.datastore_delete(resource_id=ckan_resource_id, force=True)
            except:
                pass

            ckan.action.datastore_create(
                    resource_id=ckan_resource_id,
                    force=True,
                    fields=resource['schema']['fields'],
                    primary_key=resource['schema'].get('primary_key'))

            reader = csv.DictReader(open(resource['path']))
            rows = [ row for row in reader ]
            chunksize = 1000
            offset = 0
            print('Uploading data for file: %s' % resource['path'])
            while offset < len(rows):
                rowset = rows[offset:offset+chunksize]
                ckan.action.datastore_upsert(
                        resource_id=ckan_resource_id,
                        force=True,
                        method='insert',
                        records=rowset)
                offset += chunksize
                print('Done: %s' % offset)


        if __name__ == '__main__':
            if len(sys.argv) <= 2:
                usage = '''python scripts/upload.py {resource-id} {api-key}

                e.g.

                python scripts/upload.py RESOURCE_ID API_KEY
                '''
                print(usage)
                sys.exit(1)

            ckan = ckanapi.RemoteCKAN(remote, apikey=apikey)

            resource = resources[0]
            upload_data_to_datastore(resource['resource_id'], resource)

def runEverything():
    # uploadResource(resource_id, apikey, PATH)
    downloadResource(PATH)
    updateDatastore(PATH)


# Error handler for running the entire script
try:
    runEverything()
    # if everything ok
    print "SW Status: Everything seems to be just fine."
    scraperwiki.status('ok')

except Exception as e:
    print e
    scraperwiki.status('error', 'Creating datastore failed')
    os.system("mail -s 'WFP Food Prices data: failed to create datastore on staging.' luiscape@gmail.com")
