import gzip
import xml.etree.ElementTree as ET
import polars as pl
import re
import os

def xml_to_df(file_path:str) -> pl.DataFrame:
    """Parse xml.gz file into a polars dataframe

    Args:
        - file_path (str) : path to pubmedxxxx.xml.gz file to parse

    Returns:
        - (pl.DataFrame) : article dataframe
    
    """


    records = []

    with gzip.open(file_path, 'rb') as f:
        tree = ET.parse(f)
        root = tree.getroot()

        for article in root.findall('PubmedArticle'):
            data = {}

            # PMID
            pmid_elem = article.find('.//PMID')
            data['PMID'] = pmid_elem.text if pmid_elem is not None else None

            # Titre
            title_elem = article.find('.//ArticleTitle')
            data['Title'] = title_elem.text if title_elem is not None else None

            # Abstract (peut avoir plusieurs <AbstractText>)
            abstract_elems = article.findall('.//Abstract/AbstractText')
            if abstract_elems:
                data['Abstract'] = " ".join([el.text for el in abstract_elems if el.text])
            else:
                data['Abstract'] = None

            # Date de publication (Year-Month-Day si dispo)
            pub_date = article.find('.//Article/Journal/JournalIssue/PubDate')
            if pub_date is not None:
                year = pub_date.findtext('Year') or ""
                month = pub_date.findtext('Month') or ""
                day = pub_date.findtext('Day') or ""
                data['PublicationDate'] = f"{year}-{month}-{day}".strip("-")
            else:
                data['PublicationDate'] = None

            # Date de révision / update
            revision_date = article.find('.//MedlineCitation/DateRevised')
            if revision_date is not None:
                year = revision_date.findtext('Year') or ""
                month = revision_date.findtext('Month') or ""
                day = revision_date.findtext('Day') or ""
                data['RevisionDate'] = f"{year}-{month}-{day}".strip("-")
            else:
                data['RevisionDate'] = None

            # MeSH Terms
            mesh_terms = [mh.findtext('DescriptorName') for mh in article.findall('.//MeshHeading')]
            data['MeSHTerms'] = "; ".join([m for m in mesh_terms if m])

            # Keywords
            keywords = [kw.text for kw in article.findall('.//Keyword')]
            data['Keywords'] = "; ".join([k for k in keywords if k])

            # Auteurs
            authors = []
            for auth in article.findall('.//Author'):
                last = auth.findtext('LastName') or ""
                fore = auth.findtext('ForeName') or ""
                initials = auth.findtext('Initials') or ""
                fullname = " ".join([fore, last]).strip()
                if fullname:
                    authors.append(fullname)
                elif initials and last:  # fallback
                    authors.append(f"{initials} {last}")
            data['Authors'] = "; ".join(authors)

            # Journal
            journal_title = article.findtext('.//Journal/Title')
            data['Journal'] = journal_title if journal_title else None

            records.append(data)

    return pl.DataFrame(records)



def normalize_pubdate(date_str: str) -> str | None:
    """Convert str date parsed from original xml data to proper datetime

    Args:
        - date_str (str) : strange date format, e.g 2025-sep-14
    
    """
    if not date_str:
        return None

    # params
    month_map = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "sept": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12"
    }

    # capture année, mois, jour éventuel
    parts = re.split(r"[- ]", date_str.strip())
    if len(parts) == 1:
        # cas: juste l'année
        year = parts[0]
        return f"{year}-01-01"
    elif len(parts) == 2:
        # cas: année + mois
        year, month = parts
        month_num = month_map.get(month.lower(), "01")
        return f"{year}-{month_num}-01"
    elif len(parts) >= 3:
        # cas: année + mois + jour
        year, month, day = parts[:3]
        month_num = month_map.get(month.lower(), "01")
        # pad le jour à 2 chiffres
        day = day.zfill(2)
        return f"{year}-{month_num}-{day}"
    return None


def clean_df(df:pl.DataFrame) -> pl.DataFrame:
    """Clean extracted df

    Args:
        - df (pl.DataFrame) : original extracted dataframe

    Returns:
        - (pl.DataFrame) : cleaned dataframe
    
    """

    # deal with publication date
    df = df.with_columns(
        pl.col("PublicationDate")
        .map_elements(normalize_pubdate, return_dtype=pl.Utf8)
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("PublicationDate")
    )

    # deal with revision date
    df = df.with_columns(
        pl.col("RevisionDate")
        .map_elements(normalize_pubdate, return_dtype=pl.Utf8)
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("RevisionDate")
    )

    # return cleaned df
    return df


def xml_to_parquet(pubmed_file:str, parquet_file:str, drop:bool) -> None:
    """Convert xml file to parquet

    Args:
        - pubmed_file (str) : xml.gz file containing data
        - parquet_file (str) : path to save parquet file
        - drop (bool) : if set to True delete xml.gz and md5 file if exists
    
    """

    # extract dataframe
    df = xml_to_df(pubmed_file)

    # clean df
    df = clean_df(df)

    # save to parquet
    df.write_parquet(parquet_file)

    # drop xml file
    if drop:
        os.remove(pubmed_file)
        if os.path.isfile(f"{pubmed_file}.md5"):
            os.remove(f"{pubmed_file}.md5")
        
        
def xml_to_csv(pubmed_file:str, csv_file:str, drop:bool) -> None:
    """Convert xml file to csv

    Args:
        - pubmed_file (str) : xml.gz file containing data
        - csv_file (str) : path to save csv file
        - drop (bool) : if set to True delete xml.gz and md5 file if exists
    
    """

    # extract dataframe
    df = xml_to_df(pubmed_file)

    # clean df
    df = clean_df(df)

    # save to parquet
    df.write_csv(csv_file)

    # drop xml file
    if drop:
        os.remove(pubmed_file)
        if os.path.isfile(f"{pubmed_file}.md5"):
            os.remove(f"{pubmed_file}.md5")
        
    
    


if __name__ == "__main__":

    # df = xml_to_df("/tmp/pub2csv/pubmed25n1567.xml.gz")
    # df = clean_df(df)
    # print(df)

    xml_to_parquet("/tmp/pubfetch/pubmed25n1539.xml.gz", "/tmp/pubfetch/test.parquet", False)
    xml_to_csv("/tmp/pubfetch/pubmed25n1539.xml.gz", "/tmp/pubfetch/test.csv", False)

    
