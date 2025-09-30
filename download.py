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




if __name__ == "__main__":

    # parameters
    ncbi_server_address = "ftp.ncbi.nlm.nih.gov"
    pubmed_emplacement = "/pubmed/updatefiles/"

    m = get_list_of_pubmed_files(ncbi_server_address, pubmed_emplacement)
    print(m)
    

    
