import polars as pl

def filter_date(df):
    """ """

    print(df)


if __name__ == "__main__":

    df = pl.read_parquet("/tmp/test.parquet")
    filter_date(df)
    
