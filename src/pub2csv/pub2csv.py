import glob
import polars as pl
from tqdm import tqdm
import os

from .download import get_list_of_pubmed_files, get_files_between_date, download_file_list, download_and_check, check_folder_capacity
from .parser import xml_to_df, clean_df, xml_to_parquet
from .filter import filter_date


def get_baseline_data(output_folder:str) -> None:
    """Download the content of baseline pubmed folder into output folder
    Can take a while, a lot of files to download

    Args:
        - output_folder (str) : name of the folder to store downloaded files parsed as parquet
    """

    # parameters
    ncbi_server_address = "ftp.ncbi.nlm.nih.gov"
    folder_location = "/pubmed/baseline/"

    # init output folder
    if not os.path.isdir(output_folder):
        os.mkdir(output_folder)

    # check volume capacity
    if not check_folder_capacity(output_folder, 15):
        print(f"[!] Not enough space to write in folder {output_folder}")
        return None

    # get list of files
    file_list = get_list_of_pubmed_files(ncbi_server_address, folder_location)

    # collect data
    for gz_file in tqdm(file_list, desc="Extracting Baseline Data"):
        if download_and_check(ncbi_server_address, folder_location, gz_file, output_folder):
            xml_to_parquet(f"{output_folder}/{gz_file}", f"{output_folder}/{gz_file.replace('.xml.gz', '.parquet')}", True)

    

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
    get_baseline_data("/tmp/pubfetch")
