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
                'SourceFile':pf.replace('.parquet', '.xml.gz').split('/')[-1]
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
                'SourceFile':pf.replace('.parquet', '.xml.gz').split('/')[-1]
            }
            data.append(vector)

    # assemble and save
    df = pl.DataFrame(data)
    df.write_parquet(map_file)


def get_files_for_pmid(pmid_list:list, map_file:str) -> dict:
    """Get files containing infos for pmid in pmid list
    If information is available both in baseline and updatefiles for a given
    pmid, keep only the updatefiles

    Args:
        - pmid_list (list) : list of pmid (str)
        - map_file (str) : path to the map file (should be a .parquet)

    Returns:
        - (dict) : baseline and updatefiles containing infos for given pmid
    
    """

    # init file list
    baseline_file = []
    updatefiles_file = []

    # load data
    df = pl.read_parquet(map_file).filter(pl.col('PMID').is_in(pmid_list))

    # check for associated file
    for pmid in pmid_list:
        
        # filter dataframe
        dfpmid = df.filter(pl.col('PMID') == pmid)
        dfup = dfpmid.filter(pl.col('Source') == 'updatefiles')
        dfba = dfpmid.filter(pl.col('Source') == 'baseline')

        # Check update files
        if dfup.shape[0] > 0:
            for x in list(dfup['SourceFile']):
                if x not in updatefiles_file:
                    updatefiles_file.append(x)

        # if no updatefile, go for baseline file
        else:
            for x in list(dfba['SourceFile']):
                if x not in baseline_file:
                    baseline_file.append(x)

    return {'baseline':baseline_file, 'updatefiles':updatefiles_file}


if __name__ == "__main__":

    # extract_map("/tmp/pubfetch2", "/tmp/pubfetch3", "/tmp/pubmap.parquet")
    m = get_files_for_pmid(['38273473', '39096929'], "/home/drfox/workspace/pub2csv/ressources/pubmap.parquet")
    print(m)
