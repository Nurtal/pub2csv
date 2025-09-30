from ftplib import FTP
import os
from tqdm import tqdm
import glob



def get_list_of_pubmed_files(ncbi_server_address, pubmed_emplacement):
    """Conncect to the NCBI server and return a list of available gz files"""

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
    for elt in files:
        elt_str = elt.split(".")
        if elt_str[-1] == "gz":
            target_files.append(elt)

    # return list of files
    return target_files




if __name__ == "__main__":

    # parameters
    ncbi_server_address = "ftp.ncbi.nlm.nih.gov"
    pubmed_emplacement = "/pubmed/baseline/"

    m = get_list_of_pubmed_files(ncbi_server_address, pubmed_emplacement)
    print(m)
    

    
