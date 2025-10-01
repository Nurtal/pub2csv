import download
import parser
import glob
import polars as pl

def run(date_min, date_max):
    """ """

    # parameters
    ncbi_server_address = "ftp.ncbi.nlm.nih.gov"
    pubmed_emplacement = "/pubmed/updatefiles/"
    output_folder = "/tmp/pub2csv"
    result_file = "/tmp/test.parquet"

    # download files
    target_file_list = download.get_list_of_pubmed_files(ncbi_server_address, pubmed_emplacement)
    target_file_list = download.get_files_between_date(target_file_list, date_min, date_max)
    download.download_file_list(target_file_list, ncbi_server_address, pubmed_emplacement, output_folder)
    dl_files = glob.glob(f"{output_folder}/*.gz")

    # parse downloaded files
    df_list = []
    for f in dl_files:
        df = parser.xml_to_df(f)
        df = parser.clean_df(df)
        df_list.append(df)
    df = pl.concat(df_list)
    df.write_parquet(result_file)


    
if __name__ == "__main__":

    run("12/09/2025", "14/09/2025")
