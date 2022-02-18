import os
from sys import exit
import json
import shutil
from time import time
import pandas as pd
from zipfile import ZipFile
from datetime import datetime
import pytz
import glob

# TODO:
# No MVP blockers! But nice to have...
#    1. Add CLI parameter entry with argparse 
#    2. Add unit tests
#            [ ] Check each location
#            [ ] Check free disk space on both ends prior to working on large datasets
#    3. Deploy MVP on Windows/Mac
#            [ ] put first version on GitHub
#            [ ] Containerize for Windows/OSx/Linux
#            [ ] Way to return data? 
#    4. Add docstrings to functions... 
#    5. Tend to internal TODO's 

def tstamp():
    _rn = datetime.now(pytz.timezone('US/Eastern'))
    return f"{_rn.year:04d}{_rn.month:02d}{_rn.day:02d}-{_rn.hour:02d}{_rn.minute:02d}{_rn.second:02d}"

def details_to_JSON(fn_details, ntrials, test_set, mountpt, localpt, fn_results, fn_results_delim):
    """
    Print all relevant details to JSON 
    """
    details = { 
        "ntrials" : ntrials,
        "test_set" : test_set,
        "mountpt" : mountpt, 
        "localpt" : localpt,
        "fn_results" : fn_results, 
        "fn_results_delim" : fn_results_delim
    }
    
    with open(fn_details, 'w') as _of:
        json.dump(details, _of)


ntrials=4
# mountpt = os.path.abspath("/mnt/rowley/data-lake/TEST")
mountpt = os.path.abspath(r"Z:\data-lake\TEST")
# localpt = os.path.abspath("/home/patch/rowley/dm-bench")
localpt = os.path.abspath(r"C:\Users\patch\Documents\GitHub\dm-bench")
test_set = glob.glob(os.path.join(mountpt,"*.zip"))
## alternatively, define manually using a list
#test_set = ["10minStaticDataSet_Cu64.zip", "90minDynamicDataSet_Cu64.zip"]
fn_results=os.path.join(localpt,f"results_{tstamp()}.csv")
fn_details=os.path.join(localpt,f"details_{tstamp()}.json")
fn_results_delim='\t'
verbose=False

details_to_JSON(fn_details, ntrials, test_set, mountpt, localpt, fn_results, fn_results_delim)


print()
print("Beginning Data Management Benchmarking")
print()
print("\t[... this might take a while...]")
print()
print("\tWorking with the following datasets:")
print(f"\t . {test_set}")
print()


def endpoints(test_set, mountpt, localpt): 
    """
    Input: Test set name, mount home and local home
    Returns: path on mount and path on local according 
    """
    test_set = os.path.split(test_set)[-1]
    mountpt = os.path.abspath(mountpt)
    _mount = os.path.join(mountpt,test_set)
    localpt = os.path.abspath(localpt)
    if os.path.isdir(localpt):
        print(f"\tFound local test directory at:\n\t{localpt}")
    else:
        print(f"\tCreating local test directory at:\n\t{localpt}")
        os.mkdir(localpt)
    _local = os.path.join(localpt,test_set)
    return _mount, _local

def rename_iter(path,tag="",directory=False):
    """
    Tool to rename file or directory consistently and uniquely across tests
    """
    _p0 = os.path.abspath(path)
    _p1 = '.'
    if directory:
        _p1 = _p0.rstrip('/') + tag
    else:
        _p0 = list(os.path.splitext(_p0))
        _p1 = _p0[0] + tag
        _p1 = _p1 + _p0[1]
    return os.path.abspath(_p1)

def zipname_from_folder(path,tag=""):
    return os.path.abspath(path.rstrip('/') + tag + ".zip")
    
def folder_from_zipname(path,tag=""):
    _p0 = os.path.splitext(path)
    return os.path.abspath(_p0[0] + tag) 
    

def run_copy(source,target):
    _t0 = time()
    if os.path.isdir(source):
        shutil.copytree(source, target)
    elif os.path.isfile(source):
        shutil.copyfile(source, target, follow_symlinks=False)            
    else:
        print("\tERROR: Could not distinguish file or folder.")
    _t1 = time()
    return _t1-_t0


def run_copy_cycle(_b, _tmp, ntrials, path_A, path_B, ZorF='Z'):
    if ZorF == 'Z':
        _dir = False
    elif ZorF == 'F':
        _dir = True
    else:
        print("\tERROR: ZorF should be EITHER \'Z\' or \'F\'. Fix and re-run.")
        exit()
    _new = []
    for _i in range(ntrials):
        # Copy from Mount to Local,  starting with original
        _A = "" 
        if _i == 0:
            _A = path_A
        else:
            _A = rename_iter(path_A,tag=f"_C{_i-1}",directory=False)
        _B = rename_iter(path_B,tag=f"_C{_i}",directory=False)
        
        if verbose:
            print("A:",_A)
            print("B:",_B)

        _t_elapse = run_copy(_A,_B)
        _b[('Copy',ZorF,'Mount','Local')].append(_t_elapse)
        print_report('Copy',_A,_B,_t_elapse)
        _new.append(_B)

        # 1b. Copy zip set from Local to Mount, 
        #   note: path_B stays same, but path_A iterates here
        _B = _B
        _A = rename_iter(path_A,tag=f"_C{_i}",directory=False)
        print("A:",_A)
        print("B:",_B)

        _t_elapse = run_copy(_B,_A)
        _b[('Copy',ZorF,'Local','Mount')].append(_t_elapse)
        print_report('Copy',_B,_A,_t_elapse)
        _new.append(_A)
    _tmp.extend(_new)
    return _new

def run_delete(_f):
    _t0 = time()
    if os.path.isdir(_f):
        shutil.rmtree(_f)
    elif os.path.isfile(_f):
        os.remove(_f)            
    _t1 = time()
    return _t1-_t0

def run_delete_cycle(_b,_tmp):
    print("\tDeleting the following test files generated for benchmarking:")
    for _f in _tmp:
        _loc = ""
        _type = ""
        if mountpt in _f:
            _loc = 'Mount'
        elif localpt in _f:
            _loc = 'Local'
        else:
            print(f"\tERROR: {_f} not found in current schema! Fix and re-run.")
            exit()
        if '.zip' in _f:
            _type = 'Z'
        else:
            _type = 'F'
            
        print(f"\t... {_f}")

        _t_elapse = run_delete(_f)
        _b[('Delete',_type,_loc,_loc)].append(_t_elapse)
        print_report('Delete',_f,"[Null]",_t_elapse)

    print("... done.")


def unzip_item(item_path,outdir="."):
    _ip = os.path.abspath(item_path)  
    with ZipFile(_ip, 'r') as zip:
        zip.printdir()
        print('\tExtracting...')
        zip.extractall(path=outdir)
    print('\t... done.')

def run_extract(source,target):
    _t0 = time()
    unzip_item(source, outdir=target)
    _t1 = time()
    return _t1-_t0

def run_extract_cycle(_b,_tmp,zips):
    print("\tExtracting from the following zip files:")
    _new = []
    for _f in zips:
        _loc = ""
        if mountpt in _f:
            _loc = 'Mount'
        elif localpt in _f:
            _loc = 'Local'
        else:
            print(f"\tERROR: {_f} not found in current schema! Fix and re-run.")
            exit()
        print(f"\t... {_f}")
        _out = folder_from_zipname(_f,tag="_UNZIPPED")
        _t_elapse = run_extract(_f,_out)
        _b[('Extract','Z',_loc,_loc)].append(_t_elapse)
        print_report('Extract',_f,_out,_t_elapse)
        _new.append(_out)
    _tmp.extend(_new)
    print("\t... done.")
    return _new


def zip_collect_files(item_path):
    _dir = os.path.abspath(item_path)
    _files = []
    for root, directories, filepaths in os.walk(item_path):
        for _fp in filepaths:
            _files.append(os.path.join(root, _fp))
    return _files        
        
def zip_item(item_path,output):
    _ip = os.path.abspath(item_path)
    output = os.path.abspath(output)
    _files = zip_collect_files(_ip)
    print('\tCompressing...')
    with ZipFile(output,'w') as zip:
        for _f in _files:
            print(f"\t... {_f}")
            zip.write(_f)
    print('\t... done.')

def run_compress(source,target):
    _t0 = time()
    zip_item(source,target)
    _t1 = time()
    return _t1-_t0

def run_compress_cycle(_b,_tmp,folders):
    print("\tCompressiong the following folders to `.zip`:")
    _new = []
    for _f in folders:
        _loc = ""
        if mountpt in _f:
            _loc = 'Mount'
        elif localpt in _f:
            _loc = 'Local'
        else:
            print(f"\tERROR: {_f} not found in current schema! Fix and re-run.")
            exit()
        print(f"\t... {_f}")
        _out = zipname_from_folder(_f,tag="_ZIPPED")
        _t_elapse = run_compress(_f,_out)
        _b[('Compress','F',_loc,_loc)].append(_t_elapse)
        print_report('Compress',_f,_out,_t_elapse)
        _new.append(_out)
    _tmp.extend(_new)
    print("\t... done.")
    return _new


def print_report(action,source,target,t_elapsed):
    print()
    print(f"\t{action}")
    print(f"\t\tSource: {source}")
    print(f"\t\tTarget: {target}")
    print(f"\t\tTime: {t_elapsed:.1f} seconds")
    print()

def clean_up_folder_names(df):
    """
    bench keys are set by zip filename, so need to convert using Type/ZorF column
    """
    for _i in range(len(df)):
        if df.at[_i,'Type'] == 'F':
            df.at[_i,'Item'] = folder_from_zipname(df.at[_i,'Item'])

# TODO: MAKE THE FOLLOWING MAIN ROUTINE
#       SO THAT YOU CAN START REFERENCING FUNCTIONS AS WELL
# Dict `bench` stores results across all trial sets
bench = {}
for _t in test_set:
    print(f"\tTest Set:\t{_t}")

    # Dict `_b` logs all benchmarking details for this set
    _b = {('Copy',    'Z', 'Mount','Local'): [],
          ('Copy',    'Z', 'Local','Mount'): [],
          ('Copy',    'F', 'Mount','Local'): [],
          ('Copy',    'F', 'Local','Mount'): [],
          ('Extract', 'Z', 'Local','Local'): [],
          ('Extract', 'Z', 'Mount','Mount'): [],
          ('Compress','F', 'Local','Local'): [],
          ('Compress','F', 'Mount','Mount'): [],
          ('Delete',  'Z', 'Local','Local'): [],
          ('Delete',  'Z', 'Mount','Mount'): [],
          ('Delete',  'F', 'Local','Local'): [],
          ('Delete',  'F', 'Mount','Mount'): []}
    # List `_tmp` stores list of every item generated during tests
    _tmp = []

    if verbose:
        print("mountpt: ",mountpt)
        print("localpt: ",localpt)

    # Initialize base Folder copy by extracting from base Zip
    _m0, _l0 = endpoints(_t, mountpt, localpt)
    _unzipped = folder_from_zipname(_m0,tag="")
    _t_elapse = run_extract(_m0,_unzipped) 
    _b[('Extract','Z','Mount','Mount')].append(_t_elapse)
    print_report('Extract',_m0,_unzipped,_t_elapse)

    if verbose:
        print("_m0:",_m0)
        print("_l0:",_l0)

    # Copy Zipfiles
    zip_copies = run_copy_cycle(_b,_tmp, ntrials, _m0, _l0, ZorF='Z')
   
    # Copy Folders
    _ext = os.path.split(_unzipped)
    _m1, _l1 = endpoints(_ext[1], mountpt, localpt)
    folder_copies = run_copy_cycle(_b,_tmp, ntrials, _m1, _l1, ZorF='F')

    # Extract from Copied Zipfiles to '_UNZIPPED' Folders
    folder_expansions = run_extract_cycle(_b,_tmp,zip_copies)

    # Compress from Copied Folders to '_ZIPPED' Zipfiles
    zip_compressions = run_compress_cycle(_b,_tmp,folder_copies)

    # TODO: Add sanity test to see if files are same size and pass diff check
    #       with their counterparts

    # Delete all of the generated files
    run_delete_cycle(_b,_tmp)

    # Store results in `bench` under `_t` 
    for _k in _b.keys():
        bench[_t,_k[0],_k[1],_k[2],_k[3]] = _b[_k]
    # NOTE: This could be improved by distinguishing 
    #       actions on Zip from actions on Folders


df_bench = pd.Series(bench).reset_index()
df_bench.columns = ['Item','Action','Type','Source','Target','Trials']
clean_up_folder_names(df_bench)
df_bench.to_csv(fn_results,sep=fn_results_delim)
print(df_bench)
print()
print("... done.")
