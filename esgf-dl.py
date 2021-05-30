# This script takes an esgf-search output and downloads the files at the specified path
# Features: Download stop/resume, checksum validation and fallback download method

# Settings =================================================================
# Modify the following parameters before running the script

file_list = "/path/file_list.json" # Choose an esgf-search output
save_path = "path/to/download/" # Make sure '/' is there at the end of the path 
# Settings end here =========================================================

# Import dependencies
import json, os, requests, hashlib
from tqdm import tqdm

# Load file list
with open(file_list, 'r') as f:
    files = json.load(f)

# Define a download method
def download(url, filename, save_path, checksum, checksum_type):
    r = os.system("aria2c " + url +
                  " --out=" + filename +
                  " --dir=" + save_path +
                  " --check-integrity --checksum=" + checksum_type.lower() + "=" + checksum +
                  " --file-allocation=none" +
                  " --max-connection-per-server=8" +
                  " --max-concurrent-downloads=15" +
                  " --optimize-concurrent-downloads")
    return r

# Define a fallback download method
def download_fallback(url, filename, save_path, filesize, checksum, checksum_type):
    print("Attempting download with fallback method...", flush=True)
    r = requests.get(url, stream=True)
    block_size = 1024*1024 #1 MiB
    with open(save_path+filename, 'wb') as f:
        for block in tqdm(r.iter_content(block_size), total=filesize//block_size, unit='MiB', ascii=True):
            f.write(block)

    validated = False
    if checksum_type.lower() not in hashlib.algorithms_guaranteed:
        print("Hashing algorithm isn't supported. Skipping integrity check...", flush=True)
    else:
        algorithm = getattr(hashlib, checksum_type.lower())()
        print("Validating checksum...", flush = True)
        with open(save_path+filename, 'rb') as f:
            for block in tqdm(iter(lambda: f.read(block_size), b""), total=filesize//block_size, unit='MiB', ascii=True):
                algorithm.update(block)

        if algorithm.hexdigest() == checksum:
            print("File is successfully validated against the hash!", flush=True)
            validated = True
        else:
            print("Error validating file against the hash!", flush=True)

    return r, validated
    
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
            r2, v = download_fallback(file["url"][0],file["filename"], save_path, file["size"], file["checksum"], file["checksum_type"].lower())
