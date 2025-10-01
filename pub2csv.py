import download
import parser
import filter
import glob
import polars as pl

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

    # filter on date
    filter.filter_date(df, date_min, date_max)
    
    # save
    df.write_parquet(result_file)


    
if __name__ == "__main__":

    run("12/09/2025", "14/09/2025", "/tmp/machin.parquet")
