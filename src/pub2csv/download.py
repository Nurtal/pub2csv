from ftplib import FTP
import os
from tqdm import tqdm
import glob
from datetime import datetime



def get_list_of_pubmed_files(ncbi_server_address:str, pubmed_emplacement:str) -> dict:
    """Conncect to the NCBI server and return a list of available gz files and their date of last modification

    Args:
        - ncbi_server_address (str) : ftp adress of ncbi server
        - pubmed_emplacement (str) : place where files are stored on the ftp server

    Returns:
        - (dict) : target file to their last modification date
       
    """

    # connect to the NCBI server
    target_files = []
    ftp = FTP(ncbi_server_address)
    ftp.login(user="", passwd="")

    # navigate to the pubmed directory
    ftp.cwd(pubmed_emplacement)

    # list files present in the ftp folder
    try:
        files = ftp.nlst()
    except:
        print("[*] No files to list or connection denied")

    # prepare list of gz file to download
    file_to_date = {}
    for elt in files:
        elt_str = elt.split(".")
        if elt_str[-1] == "gz":
            target_files.append(elt)
            resp = ftp.sendcmd(f"MDTM {elt}")
            d = resp.split(' ')[1]
            d = datetime.strptime(d, "%Y%m%d%H%M%S")
            file_to_date[elt] = d

    # return list of files
    return file_to_date


def get_files_between_date(file_to_date:dict, date_min:str, date_max:str):
    """Get files updated between min and max date, retrun list of files

    Args:
        - file_to_date (dict) : filename to last updated date
        - date_min (str) : min date in d/m/Y format
        - date_max (str) : max date in d/m/Y format

    Returns:
        - (list) : list of selected files
    
    """

    # preprocess date
    date_min = datetime.strptime(date_min, "%d/%m/%Y")
    date_max = datetime.strptime(date_max, "%d/%m/%Y")

    # select candidates file
    target_file = []
    for f in file_to_date:
        d = file_to_date[f]
        if d >= date_min and d <= date_max:
            target_file.append(f)
            
    return target_file

def download_file_list(file_list:list, ncbi_server_address:str, pubmed_emplacement:str, download_folder:str) -> None:
    """Download a specific list of files from the ncbri to a download folder

    Args:
        - file_list (list) : list of file to dl
        - ncbi_server_address (str) : ftp server adress
        - pubmed_emplacement (str) : folder where to fond the files on the ftp server
        - download_folder (str) : local download where files are dl
    
    """

    # if dl folder does not exist, create it
    if not os.path.isdir(download_folder):
        os.mkdir(download_folder)

    # connect to the NCBI server
    ftp = FTP(ncbi_server_address)
    ftp.login(user="", passwd="")

    # navigate to the pubmed directory
    ftp.cwd(pubmed_emplacement)

    # loop over target list
    for gz_file in file_list:

        # download file
        gz_local_file = open(download_folder + "/" + str(gz_file), "wb")
        ftp.retrbinary("RETR " + str(gz_file), gz_local_file.write, 1024)
        gz_local_file.close()
    


if __name__ == "__main__":

    # parameters
    ncbi_server_address = "ftp.ncbi.nlm.nih.gov"
    pubmed_emplacement = "/pubmed/updatefiles/"

    m = get_list_of_pubmed_files(ncbi_server_address, pubmed_emplacement)
    m = get_files_between_date(m, "12/09/2025", "25/09/2025")
    download_file_list(m, ncbi_server_address, pubmed_emplacement, "/tmp/pub2csv")
    
