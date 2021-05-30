# This script takes an esgf-search output and downloads the files at the specified path
# Features: Download stop/resume, checksum validation and fallback download method

# Settings =================================================================
# Modify the following parameters before running the script

file_list = "/path/file_list.json" # Choose an esgf-search output
save_path = "path/to/download/" # Make sure '/' is there at the end of the path 
# Settings end here =========================================================

# Import dependencies
import json, os, requests, tqdm

# Load file list
with open(file_list, 'r') as f:
    files = json.load(f)

# Define a function for download
def download(url, filename, save_path, checksum, checksum_type):
    r = os.system("aria2c " + url +
                  " --out=" + filename +
                  " --dir=" + save_path +
                  " --check-integrity --checksum=" + checksum_type + "=" + checksum +
                  " --file-allocation=none" +
                  " --max-connection-per-server=8" +
                  " --max-concurrent-downloads=15" +
                  " --optimize-concurrent-downloads")
    return r

# download
for file in files:
    if os.path.isfile(save_path+file["filename"]):
        if os.stat(save_path+file["filename"]).st_size == file["size"]:
            start_download = False
            print("File already exists: "+file["filename"], flush=True)
        else:
            start_download = True
            print("File exists, but not downloaded properly. Downloading again...", flush=True)
    else:
        start_download = True

    if start_download:
        r1 = download(" ".join(file["url"]), file["filename"], save_path, file["checksum"], file["checksum_type"].lower().replace("sha", "sha-"))
        
        if r1:
        # Fallback method for download
            print("Attempting download with fallback method...", flush=True)
            r2 = requests.get(file["url"][0], stream=True)
            block_size = 1024*1024
            with open(save_path+file["filename"], 'wb') as f:
                for data in tqdm.tqdm(r2.iter_content(block_size), total=file["size"]//block_size, unit='MiB', ascii=True):
                    f.write(data)
