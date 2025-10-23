import glob
import polars as pl
from tqdm import tqdm
import os
import shutil

from .download import get_list_of_pubmed_files, get_files_between_date, download_file_list, download_and_check, check_folder_capacity, get_ftp_connection
from .parser import xml_to_df, clean_df, xml_to_parquet
from .filter import filter_date
from .mapper import get_files_for_pmid


def get_baseline_data(output_folder:str, max_retries:int, override:bool) -> None:
    """Download the content of baseline pubmed folder into output folder
    Can take a while, a lot of files to download

    Args:
        - output_folder (str) : name of the folder to store downloaded files parsed as parquet
        - max_retries (int) : number of attempts authorized to download files
        - override (bool) : if True clean output folder if exist, if False just download the missing files from output folder
    """

    # parameters
    ncbi_server_address = "ftp.ncbi.nlm.nih.gov"
    folder_location = "/pubmed/baseline/"

    # clean output folder if it already exist and override is set to True
    if override and os.path.isidir(output_folder):
        shutil.rmtree(output_folder)

    # init output folder
    dl_files = []
    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)
    else:
        processed_files = glob.glob(f"{output_folder}/*.parquet")
        for pf in processed_files:
            dl_files.append(pf.replace(".parquet", ".xml.gz").split("/")[-1])

    # check volume capacity
    if not check_folder_capacity(output_folder, 16):
        print(f"[!] Not enough space to write in folder {output_folder}")
        return None

    # create ftp connection
    ftp = get_ftp_connection(ncbi_server_address, folder_location)

    # get list of files to download
    file_list = []
    all_files = get_list_of_pubmed_files(ftp)
    for af in all_files:
        if af not in dl_files:
            file_list.append(af)

    # init loop parameter
    to_retry = file_list
    attempts = 0

    # collect data
    while to_retry and attempts < max_retries:
        failed = []
        for gz_file in tqdm(to_retry, desc=f"[Attempt {attempts+1}] Extracting Baseline Data"):
            if download_and_check(gz_file, output_folder, ftp):
                xml_to_parquet(f"{output_folder}/{gz_file}", f"{output_folder}/{gz_file.replace('.xml.gz', '.parquet')}", True)
            else:
                failed.append(gz_file)

        # update loop parameter
        to_retry = failed
        attempts +=1

    # display missing files
    if to_retry:
        print(f"[!]Failed to download the following files after {attempts} attempts:")
        for gz_file in to_retry:
            print(f"\t- {gz_file}")

    # close ftp connection
    ftp.close()
    
    # display coverage
    coverage = float( (len(all_files) - len(to_retry)) / len(all_files) ) *100.0
    print(f"[*] Extract {coverage} % of baseline articles")



def get_updatefiles_data(output_folder:str, max_retries:int, override:bool) -> None:
    """Download the content of updatefiles pubmed folder into output folder
    Can take a while, a lot of files to download

    Args:
        - output_folder (str) : name of the folder to store downloaded files parsed as parquet
        - max_retries (int) : number of attempts authorized to download files
        - override (bool) : if True clean output folder if exist, if False just download the missing files from output folder
    """

    # parameters
    ncbi_server_address = "ftp.ncbi.nlm.nih.gov"
    folder_location = "/pubmed/updatefiles/"

    # clean output folder if it already exist and override is set to True
    if override and os.path.isidir(output_folder):
        shutil.rmtree(output_folder)

    # init output folder
    dl_files = []
    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)
    else:
        processed_files = glob.glob(f"{output_folder}/*.parquet")
        for pf in processed_files:
            dl_files.append(pf.replace(".parquet", ".xml.gz").split("/")[-1])

    # check volume capacity
    if not check_folder_capacity(output_folder, 16):
        print(f"[!] Not enough space to write in folder {output_folder}")
        return None

    # create ftp connection
    ftp = get_ftp_connection(ncbi_server_address, folder_location)

    # get list of files to download
    file_list = []
    all_files = get_list_of_pubmed_files(ftp)
    for af in all_files:
        if af not in dl_files:
            file_list.append(af)

    # init loop parameter
    to_retry = file_list
    attempts = 0

    # collect data
    while to_retry and attempts < max_retries:
        failed = []
        for gz_file in tqdm(to_retry, desc=f"[Attempt {attempts+1}] Extracting UpdateFiles Data"):
            if download_and_check(gz_file, output_folder, ftp):
                xml_to_parquet(f"{output_folder}/{gz_file}", f"{output_folder}/{gz_file.replace('.xml.gz', '.parquet')}", True)
            else:
                failed.append(gz_file)

        # update loop parameter
        to_retry = failed
        attempts +=1

    # display missing files
    if to_retry:
        print(f"[!]Failed to download the following files after {attempts} attempts:")
        for gz_file in to_retry:
            print(f"\t- {gz_file}")

    # close ftp connection
    ftp.close()
    
    # display coverage
    coverage = float( (len(all_files) - len(to_retry)) / len(all_files) ) *100.0
    print(f"[*] Extract {coverage} % of baseline articles")


def get_pmid_data(pmid_list:list, output_folder:str, max_retries:int, map_file:str):
    """ """

    # parameters
    ncbi_server_address = "ftp.ncbi.nlm.nih.gov"
    updatefiles_folder = "/pubmed/updatefiles/"
    baseline_folder = "/pubmed/baseline/"

    # init output folder
    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)

    # get list of files to download
    files = get_files_for_pmid(pmid_list, map_file)

    #----------#
    # BASELINE #
    #----------#
    # create ftp connection - baseline
    ftp = get_ftp_connection(ncbi_server_address, baseline_folder)

    # get list of files to download
    file_list = files['baseline']

    # init loop parameter
    to_retry = file_list
    attempts = 0

    # collect data
    while to_retry and attempts < max_retries:
        failed = []
        for gz_file in tqdm(to_retry, desc=f"[Attempt {attempts+1}] Extracting Baseline Data"):
            if download_and_check(gz_file, output_folder, ftp):
                xml_to_parquet(f"{output_folder}/{gz_file}", f"{output_folder}/{gz_file.replace('.xml.gz', '.parquet')}", True)
            else:
                failed.append(gz_file)

        # update loop parameter
        to_retry = failed
        attempts +=1

    # display missing files
    if to_retry:
        print(f"[!][BASELINE]Failed to download the following files after {attempts} attempts:")
        for gz_file in to_retry:
            print(f"\t- {gz_file}")

    # close ftp connection
    ftp.close()

    #-------------#
    # UPDATEFILES #
    #-------------#
    # create ftp connection - updatefiles
    ftp = get_ftp_connection(ncbi_server_address, updatefiles_folder)

    # get list of files to download
    file_list = files['updatefiles']

    # init loop parameter
    to_retry = file_list
    attempts = 0

    # collect data
    while to_retry and attempts < max_retries:
        failed = []
        for gz_file in tqdm(to_retry, desc=f"[Attempt {attempts+1}] Extracting UpdateFiles Data"):
            if download_and_check(gz_file, output_folder, ftp):
                xml_to_parquet(f"{output_folder}/{gz_file}", f"{output_folder}/{gz_file.replace('.xml.gz', '.parquet')}", True)
            else:
                failed.append(gz_file)

        # update loop parameter
        to_retry = failed
        attempts +=1

    # display missing files
    if to_retry:
        print(f"[!][UPDATE]Failed to download the following files after {attempts} attempts:")
        for gz_file in to_retry:
            print(f"\t- {gz_file}")

    # close ftp connection
    ftp.close()


def run(date_min:str, date_max:str, result_file:str) -> None:
    """Run the download, parsing and filter of the articles

    Args:
        - date_min (str) : min date for article publication in the format d/m/Y
        - date_max (str) : max date for article publication in the format d/m/Y        
        - result_file (str) : path to the result (parquet file)
    
    """

    # parameters
    ncbi_server_address = "ftp.ncbi.nlm.nih.gov"
    pubmed_emplacement = "/pubmed/updatefiles/"
    output_folder = "/tmp/pub2csv"

    # download files
    target_file_list = get_list_of_pubmed_files(ncbi_server_address, pubmed_emplacement)
    target_file_list = get_files_between_date(target_file_list, date_min, date_max)
    download_file_list(target_file_list, ncbi_server_address, pubmed_emplacement, output_folder)
    dl_files = glob.glob(f"{output_folder}/*.gz")

    # parse downloaded files
    df_list = []
    for f in dl_files:
        df = xml_to_df(f)
        df = clean_df(df)
        df_list.append(df)
    df = pl.concat(df_list)

    # filter on date
    filter_date(df, date_min, date_max)
    
    # save
    df.write_parquet(result_file)


    
if __name__ == "__main__":

    # run("12/09/2025", "14/09/2025", "/tmp/machin.parquet")
    # get_baseline_data("/tmp/pubfetch2", 3, False)
    # get_updatefiles_data("/tmp/pubfetch4", 3, False)
    get_pmid_data("/tmp/pmid", 3, "/tmp/pubmap.parquet"):
