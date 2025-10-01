import polars as pl
import datetime as dt


def filter_date(df:pl.DataFrame, date_min:str, date_max:str) -> pl.DataFrame:
    """Filter dataframe according to publication date

    Args:
        - df (pl.DataFrame) : polars dataframe containing article
        - date_min (str) : min date fo filter, formated as d/m/Y
        - date_max (str) : max date fo filter, formated as d/m/Y

    Returns:
        - (pl.DataFrame) : filtered dataframe
    
    """

    # preprocess dates
    date_min = dt.datetime.strptime(date_min, "%d/%m/%Y").date()
    date_max = dt.datetime.strptime(date_max, "%d/%m/%Y").date()

    # apply filter
    df = df.filter((pl.col("PublicationDate") >= date_min) & (pl.col("PublicationDate") <= date_max) )

    # return df
    return df



if __name__ == "__main__":

    df = pl.read_parquet("/tmp/test.parquet")
    df = filter_date(df, "12/09/2025", "17/09/2025")
    print(df)
    
