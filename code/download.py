import sys
import requests

resource_id = sys.argv[1]
apikey = sys.argv[2]
headers = { 'X-CKAN-API-Key': apikey, 'content-type': 'application/json' }

# Function to download a resource from CKAN.
def downloadResource(filename):

    print "Downloading file from CKAN."

    # Creating a download file function.
    def download_file(url, l):
            local_filename = l
            # NOTE the stream=True parameter
            r = requests.get(url, stream=True, auth=('dataproject', 'humdata'))

            if r.status_code != 200:
            	print "Status code isn't right."

            else: 
	            with open(local_filename, 'wb') as f:
	                for chunk in r.iter_content(chunk_size=1024): 
	                    if chunk: # filter out keep-alive new chunks
	                        f.write(chunk)
	                        f.flush()
	            return local_filename

    # querying
    url = 'https://test-data.hdx.rwlabs.org/api/action/resource_show?id=' + resource_id
    r = requests.get(url, headers=headers, auth=('dataproject', 'humdata'))

    if r.status_code != 200:
    	print "Couldn't reach server."
    	return
    else:
	    doc = r.json()
	    fileUrl = doc["result"]["url"]

    # downloading
    try:
        download_file(fileUrl, filename)
        
    except:
        print 'There was an error downlaoding the file.'

# Command line arguments.
if __name__ == '__main__':
    if len(sys.argv) <= 2:
        usage = '''
        python scripts/upload.py {resource-id} {api-key}

        e.g.

        python scripts/upload.py RESOURCE_ID API_KEY
        '''
        print(usage)
        sys.exit(1)

        downloadResource("tool/data/data.csv")
