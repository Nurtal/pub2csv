import glob
import polars as pl

def extract_map(baseline_folder:str, updatefiles_folder:str, map_file:str) -> None:
    """Exctract data from parquet file to build a map file
    
    Args:
        - baseline_folder (str) : path to the folder containing baseline pubmed.xml.gz
        - updatefiles_folder (str) : path to the folder containing updatefiles pubmed.xml.gz
        - map_file (str) : name of the map file to generate (should be a .parquet)
    
    """

    # init data
    data = []

    # load baseline
    for pf in glob.glob(f"{baseline_folder}/*.parquet"):
        df = pl.read_parquet(pf)['PMID', 'PublicationDate']
        for row in df.iter_rows():
            vector = {
                'PMID':row[0],
                'PublicationDate':row[1],
                'Source':'baseline',
                'SourceFile':pf.replace('.parquet', '.gz.xml').split('/')[-1]
            }
            data.append(vector)

    # load updatefiles
    for pf in glob.glob(f"{updatefiles_folder}/*.parquet"):
        df = pl.read_parquet(pf)['PMID', 'PublicationDate']
        for row in df.iter_rows():
            vector = {
                'PMID':row[0],
                'PublicationDate':row[1],
                'Source':'updatefiles',
                'SourceFile':pf.replace('.parquet', '.gz.xml').split('/')[-1]
            }
            data.append(vector)

    # assemble and save
    df_baseline = pl.DataFrame(data)
    df.write_parquet(map_file)



if __name__ == "__main__":

    extract_map("/tmp/pubfetch2", "/tmp/pubfetch3", "/tmp/pubmap.parquet")
