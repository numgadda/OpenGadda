from sqlite3 import connect
from pandas import read_csv, read_sql, DataFrame
from json import load
import pandas as pd

class RelationalProcessor:
    def __init__(self, dbPath=None):
        self.dbPath = dbPath

    def getDbPath(self):
        return self.dbPath

    def setDbPath(self, path):
        if path != '':
            self.dbPath = path
            return True
        else:
            return False
class RelationalDataProcessor (RelationalProcessor):
    def __init__(self, dbPath=None):
        super().__init__(dbPath)
    def uploadData (self, path):
        import pandas as pd
        from pandas import DataFrame
        from sqlite3 import connect
        from pandas import Series
        if path !='':
            if path.endswith(".csv"):
                from pandas import read_csv
            
                df_rel_publications=read_csv (path,
                                    keep_default_na=False,
                                    encoding="utf-8",
                                    dtype={
                                "id": "string",
                                "title": "string",
                                "type": "string",
                                "publication_year": "int",
                                "issue": "string",
                                "volume": "string",
                                "chapter": "string",
                                "publication_venue": "string",
                                "venue_type": "string",
                                "publisher":"string",
                                "event":"string"            
                                }) 
                # Create a new column with internal identifiers for each publication
                publication_internal_id = []
                for idx, row in df_rel_publications.iterrows():
                    publication_internal_id.append("publication-" + str(row["id"]))
                df_rel_publications.insert(0, "publicationInternalId", Series(publication_internal_id, dtype="string"))
                publisher_internal_id = []
                for idx, row in df_rel_publications.iterrows():
                    publisher_internal_id.append ("publisher-" + str(row["publisher"]))
                del df_rel_publications["publisher"]
                df_rel_publications.insert(10, "publisher", Series(publisher_internal_id, dtype="string"))

                df_rel_publications= df_rel_publications.rename(columns={"id":"doi", "publication_year":"publicationYear", 
                "chapter":"chapterNumber", "publication_venue":"publicationVenue"})
                # print(df_rel_publications["publisher"])

                with connect(self.dbPath) as con:
                    df_rel_publications.to_sql("Publications", con, if_exists="replace", index=False)
                    con.commit()
                return True

            if path.endswith(".json"):
                from json import load 
                with open(path, "r", encoding="utf-8") as f:
                    rel_data = load(f)

                #authors' dataframe
                findex=0
                for x in rel_data["authors"]:
                    for y in rel_data["authors"][x]:
                        findex+=1

                df_author=pd.DataFrame(columns=["doi","family","given","orcid"],index=range(findex))#dataframe with authors information
                ind=0
                for x in rel_data["authors"]:
                    for y in rel_data["authors"][x]:
                        df_author.iloc[ind] = (x,y["family"],y["given"],y["orcid"])
                        ind+=1
                publication_internal_id = []
                for idx, row in df_author.iterrows():
                    publication_internal_id.append("publication-" + str(row["doi"]))
                df_author.insert(4, "publicationInternalId", Series(publication_internal_id, dtype="string"))
                author_internal_id = []
                for idx, row in df_author.iterrows():
                    author_internal_id.append ("author-" + str(row["orcid"]))
                df_author.insert(0, "authorInternalId", Series(author_internal_id, dtype = "string"))
                    
                df_author = df_author[["authorInternalId", "family", "given", "orcid", "publicationInternalId"]]
                df_author= df_author.rename(columns={"given":"givenName", "family":"familyName","orcid":"authorID"})

                #dataframe with publications'doi and venues id
                findex = 0
                for x in rel_data["venues_id"]:
                    findex += 1
                df_pub_venues = pd.DataFrame(columns = ["doi","venues_id"], index= range(findex))
                ind =0
                for x in rel_data["venues_id"]:
                    df_pub_venues.iloc[ind] = (x, str(rel_data["venues_id"][x]))
                    ind +=1
                publication_internal_id =[]
                for idx, row in df_pub_venues.iterrows():
                    publication_internal_id.append("publication-" + str(row["doi"]))
                df_pub_venues.insert(0, "publicationInternalId", Series(publication_internal_id, dtype="string"))
                venue_internal_id= []
                for idx, row in df_pub_venues.iterrows():
                    venue_internal_id.append("venue-" + str(idx))
                df_pub_venues.insert(0, "venueInternalId", Series(venue_internal_id, dtype="string"))

                df_pub_venues = df_pub_venues[["venueInternalId", "venues_id", "publicationInternalId"]]
                df_pub_venues=df_pub_venues.rename(columns={"venues_id":"venueID"})

                #dataframe for publishers 
                findex = 0
                for x in rel_data["publishers"]:
                    findex+=1
                df_publishers = pd.DataFrame(columns=["publisher_id", "name"], index=range(findex))
                ind=0 
                for x in rel_data["publishers"]:
                    df_publishers.iloc[ind] = (rel_data["publishers"][x]["id"], rel_data["publishers"][x]["name"])
                    ind +=1
                publisher_internal_id = []
                for idx, row in df_publishers.iterrows():
                    publisher_internal_id.append ("publisher-" + str(row["publisher_id"]))
                df_publishers.insert(0, "publisherInternalId", Series(publisher_internal_id, dtype = "string"))
                df_publishers=df_publishers.rename(columns={"publisher_id":"organizationID"})
                
                findex_references=0
                for x in rel_data["references"]:
                    for y in rel_data["references"][x]:
                        findex_references +=1
                cited_publications = pd.DataFrame(columns=["id", "id_references"], index=range(findex_references))
                ind = 0
                for x in rel_data["references"]:
                    for y in rel_data["references"][x]:
                        cited_publications.iloc[ind]= (x, y)
                        ind += 1
                cited_publications = cited_publications.rename(columns={"id":"doi","id_references":"CitedPublications"})
            
                #references 
                findex=0
                for x in rel_data["references"]:
                    findex+=1
                df_references = pd.DataFrame(columns=["id", "id_references"], index=range(findex))
                ind = 0
                for x in rel_data["references"]:
                    df_references.iloc[ind]= (x, str(rel_data["references"][x]))
                    ind += 1        
                        
                for idx, row in df_references.iterrows():
                    publication_internal_id.append("publication-" + str(row["id"]))
                df_references.insert(2, "publicationInternalId", Series(publication_internal_id, dtype="string"))
                
                df_references = df_references[["id_references", "publicationInternalId"]]
                df_references=df_references.rename(columns={"id_references":"id_all_references"})
                # print(df_references)
                # Cites
            
            
                with connect(self.dbPath) as con:
                    
                    df_author.to_sql("Authors", con, if_exists="replace", index=False)
                    df_publishers.to_sql("Publishers", con, if_exists="replace", index=False)
                    df_pub_venues.to_sql("Venues", con, if_exists="replace", index=False)
                    df_references.to_sql("CitedPublications", con, if_exists="replace", index=False)
                    cited_publications.to_sql("CitedPublications1", con, if_exists="replace", index=False)
                    con.commit()
                return True
            else:
                return False
        else:
            return False
class RelationalQueryProcessor(RelationalProcessor):
    def __init__(self, dbPath=None):
        super().__init__(dbPath)

    def getPublicationsPublishedInYear(self, input_year):
        with connect(self.dbPath) as con:
            query = """SELECT Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, CitedPublications.id_all_references, 
            GROUP_CONCAT(Authors.authorID)
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE Publications.publicationYear = '{0}'
            GROUP BY Publications.publicationInternalId;""".format(input_year)
            publication_year_df = read_sql(query, con)
            publication_year_df = publication_year_df.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id"})
        return publication_year_df
rel_path = "relationalDatabase.db"
rel_dp = RelationalDataProcessor(rel_path)
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("/Users/martinapensalfini/Documents/data science/relational_publications.csv")
rel_dp.uploadData("/Users/martinapensalfini/Documents/data science/relational_other_data.json")
rel_qp = RelationalQueryProcessor(rel_path)
rel_qp.setDbPath(rel_path)

print("1) getPublicationsPublishedInYear:\n",rel_qp.getPublicationsPublishedInYear(2020))
# print("-----------------")