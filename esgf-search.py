# This script queries ESGF nodes and saves all matching files with their download url and
# metadata in a user-defined json file.

# Settings =================================================================
# Modify the following parameters before running the script

savefile = "/path/file_list.json" #This file will later be used as an input in downloading script

# This is an example query. Modify the query as needed
# Check documentation for more information on which keys and values can be used
query_params = {"latest": True,
    "mip_era": "CMIP6",
    "activity_id": "HighResMIP",
    "source_id": "HadGEM3-GC31-HM",
    "experiment_id": "highres-future",
    "member_id": "r1i1p1f1",
    "realm": "atmos",
    "frequency": "day",
    "variable": "ta, tas, ua, uas, va, vas, hus, huss, psl",
    }

# Settings ends here ========================================================

# Import dependencies
from pyesgf.search import SearchConnection
import json, os, operator, itertools

# Search
query = SearchConnection('https://esgf-data.dkrz.de/esg-search', distrib=True).new_context(**query_params)
results = query.search()
print("Search done!\nNow preparing file-list...", flush=True)

# Convert search results into a list of filename and download url
files = []
for i in range(0, len(results)):
    files.extend(list(map(lambda f : {'filename': f.filename, 'url': f.download_url, 'size': f.size, 'checksum': f.checksum, 'checksum_type': f.checksum_type},
                               results[i].file_context().search())))

# Consolidate all duplicate download links into a single entry
files = sorted(files, key=operator.itemgetter("filename"))
files_gr=[]
for i,g in itertools.groupby(files, key=operator.itemgetter("filename")):
    grp = list(g)
    entry = grp[0].copy()
    entry['url'] = tuple(e['url'] for e in grp)
    files_gr.append(entry)
print("File list ready!", flush=True)

# Save file list
with open(savefile, 'w') as f:
    json.dump(files_gr, f, indent = 4)
