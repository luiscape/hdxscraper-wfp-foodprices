import requests

def download_file(url, l):
    local_filename = l
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True, auth=('dataproject', 'humdata'))
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return local_filename

download_file("http://test-data.hdx.rwlabs.org/dataset/idps-data-by-region-in-mali/resource_download/9affe23f-3ce2-40d1-ae6a-1d7060b548a5", "data/data.csv")