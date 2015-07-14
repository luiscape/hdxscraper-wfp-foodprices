#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import csv
import sys
import json
import ckanapi
import requests
import scraperwiki


def FetchSystemArguments():
  '''Fetching arguments from the command line interface.'''

  arguments = {
    'api_key': sys.argv[1],
    'json_path': sys.argv[2],
    'download_temp_path': sys.argv[3],
    'stag_url': 'https://test-data.hdx.rwlabs.org',
    'prod_url': 'https://data.hdx.rwlabs.org'
  }

  #
  # Checking that all arguments have been provided.
  #
  for argument in arguments:

    if argument is None:
      print 'Argument %s is empty. That argument is necessary.' % argument.keys()
      return False

  return arguments


def GetResourcesFromLocalJSON(local_json_path):
  '''Loading resources from a local json file.'''

  try:
    with open(local_json_path) as json_file:
      resources = json.load(json_file)

      #
      # Checking that the json provide contains at least
      # one resource.
      #
      if len(resources) < 1:
        print "Resouces look odd! Please revise"

    return resources

  except Exception as e:
    print e
    return False


def DownloadResourceFromHDX(ckan_url, file_name, resource_id, api_key, verbose = True):
  '''Downloading a resource from CKAN based on its id. Resources need to be
     downloaded in order to be correctly parsed by the CreateDatastore function.'''


  print "Downloading resource file from HDX."
  header = { 'Authorization': api_key }

  #
  # Querying.
  #
  url = ckan_url + '/api/action/resource_show?id=' + resource_id
  r = requests.get(url, headers=header, auth=('dataproject', 'humdata'))
  doc = r.json()
  if doc['success'] is False:
    if verbose:
      print json.dumps(doc)
    print 'Failed to read resource.'
    return False

  else:
    resource_file_url = doc["result"]["url"]


  #
  # Downloading.
  #
  try:
    with open(file_name, 'wb') as handle:
      response = requests.get(resource_file_url, stream=True, headers=header, auth=('dataproject', 'humdata'))

      if not response.ok:
        print "Error: attempt to download resource failed."
        return

      for block in response.iter_content(1024):
        if not block:
          break

        handle.write(block)

  except Exception as e:
    print 'There was an error downlaoding the file.'
    if verbose:
      print e
    return False



def DeleteDatastore(ckan_url, api_key, ckan_resource_id, verbose=True):
  '''Delete a CKAN DataStore.'''

  #
  # Configuring the remote CKAN instance.
  #
  ckan = ckanapi.RemoteCKAN(ckan_url, apikey=api_key)


  try:
    ckan.action.datastore_delete(resource_id=ckan_resource_id, force=True)

  except Exception as e:
    print 'WARN: There was an error deleting an old DataStore.'
    if verbose:
        print e
    pass



def CreateDatastore(ckan_url, api_key, resource_id, file_name, resource, verbose=True):
  '''Creating a CKAN DataStore.'''

  #
  # Configuring the remote CKAN instance.
  #
  ckan = ckanapi.RemoteCKAN(ckan_url, apikey=api_key)

  DeleteDatastore(ckan_url=ckan_url, api_key=api_key, ckan_resource_id=resource_id)
  if DeleteDatastore(
    ckan_url=ckan_url,
    api_key=api_key,
    ckan_resource_id=resource_id) is False:
    return False

  #
  # Creating a DataStore.
  #
  ckan.action.datastore_create(
    resource_id=resource_id,
    force=True,
    fields=resource['schema']['fields'],
    primary_key=resource['schema'].get('primary_key'))

  #
  # Reading CSV file and inserting data.
  #
  reader = csv.DictReader(open(file_name))
  rows = [ row for row in reader ]

  #
  # Hack for managing different encoding data.
  #
  rows_decoded = []
  for row in rows:
    row_encoded = { key:row[key].decode('latin-1') for key in row.keys() }
    rows_decoded.append(row_encoded)

  #
  # Sending N records at a time.
  #
  chunksize = 10000  # N rows per POST request.
  offset = 0
  while offset < len(rows_decoded):
    rowset = rows_decoded[offset:offset+chunksize]
    ckan.action.datastore_upsert(
      resource_id=resource_id,
      force=True,
      method='insert',
      records=rowset)
    offset += chunksize
    complete = str(float(offset)/len(rows_decoded) * 100)[:4] + '%'
    print(' Update successful: %s completed' % complete)



def Main():
  '''Wrapper.'''

  #
  # Fetching arguments and configuring the script.
  #
  p = FetchSystemArguments()

  api_key = p['api_key']
  ckan_url = p['prod_url']
  download_temp_path = p['download_temp_path']

  #
  # Loading resources from a local JSON file.
  #
  resources = GetResourcesFromLocalJSON(p['json_path'])

  #
  # Iterating over each resource provided.
  #
  for r in resources:
    resource_id = r['resource_id']
    print 'Creating DataStore for resource id: ' + resource_id

    try:
      DownloadResourceFromHDX(
        ckan_url=ckan_url,
        file_name=download_temp_path,
        resource_id=resource_id,
        api_key=api_key
        )
      CreateDatastore(
        ckan_url=ckan_url,
        api_key=api_key,
        file_name=download_temp_path,
        resource_id=resource_id,
        resource=r
        )

      print 'All DataStores were created successfully.'

    except Exception as e:
      print 'DataStore creation failed.'
      print e
      return False



if __name__ == '__main__':
  Main()
