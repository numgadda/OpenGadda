class RelationalProcessor(object): 
    def __init__(self, dbPath=None):
        self.dbPath=dbPath  
    def getDbPath(self):
        return self.dbPath
    def setDbPath(self, path):
        if path!='':
            self.dpPath=path
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
            print(list(df_rel_publications.columns))

            with connect(self.dbPath) as con:
                df_rel_publications.to_sql("Publications", con, if_exists="replace", index=False)
                con.commit()

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
            print(df_author)

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
            print(df_pub_venues)

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
            print(list(cited_publications.columns))

    
        
            
            with connect(self.dbPath) as con:
                
                df_author.to_sql("Authors", con, if_exists="replace", index=False)
                df_publishers.to_sql("Publishers", con, if_exists="replace", index=False)
                df_pub_venues.to_sql("Venues", con, if_exists="replace", index=False)
                df_references.to_sql("CitedPublications", con, if_exists="replace", index=False)
                cited_publications.to_sql("CitedPublications1", con, if_exists="replace", index=False)
                con.commit()
                
            

from sqlite3 import connect
from pandas import read_sql

class RelationalQueryProcessor (RelationalProcessor):
    def __init__(self, dbpath):
        super().__init__(dbpath)


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
    def getPublicationsByAuthorId(self,authorID):
        with connect(self.dbPath) as con:
            query =  """SELECT Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, CitedPublications.id_all_references, 
            GROUP_CONCAT(Authors.authorID)
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE Authors.authorID = '{0}'
            GROUP BY Publications.publicationInternalId;""".format(authorID)
            authorID_df=read_sql(query, con)
            authorID_df = authorID_df.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id"})
        return authorID_df
    def getPublicationsByAuthorId2(self,authorID):
        with connect(self.dbPath) as con:
            query =  """SELECT Publications.publicationInternalId
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE Authors.authorID = '{0}'
            GROUP BY Publications.publicationInternalId;""".format(authorID)
            publication_author_df = read_sql(query, con)
            
            query_2="""SELECT  GROUP_CONCAT(Authors.authorID), Publications.publicationInternalId
            FROM PUBLICATIONS
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            GROUP BY Publications.publicationInternalId;"""
            all_authors= read_sql(query_2, con)
            all_authors= all_authors.rename(columns={"GROUP_CONCAT(Authors.authorID)":"all_authors_id"})
            
            publication_all_authors = publication_author_df.merge(all_authors)
            
        return publication_all_authors
           
        return authorID_df
    def getMostCitedPublication(self):
        with connect(self.dbPath) as con:
            query ="""SELECT DISTINCT Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, GROUP_CONCAT(Authors.authorID), Cited
            FROM (SELECT CitedPublications1.citedPublications, COUNT (CitedPublications1."citedPublications") as Cited
            FROM CitedPublications1
            GROUP BY CitedPublications1.citedPublications
            ORDER BY Cited DESC
            LIMIT 1)
            LEFT JOIN Publications ON Publications.doi==CitedPublications1.citedPublications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            GROUP BY Publications.publicationInternalId;"""
            mostcitedpub=read_sql(query, con)
            mostcitedpub= mostcitedpub.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id"})
           
        return mostcitedpub
    def getMostCitedVenue(self):
        with connect(self.dbPath) as con:
            query ="""SELECT DISTINCT Venues.venueInternalId, Publications.publicationVenue, Venues.venueID, Publishers.organizationID,  Cited
            FROM (SELECT CitedPublications1."citedPublications", COUNT (CitedPublications1."citedPublications") as Cited
            FROM CitedPublications1
            GROUP BY CitedPublications1."citedPublications"
            ORDER BY Cited DESC
            LIMIT 1)
            LEFT JOIN Publications ON Publications.doi==CitedPublications1.doi
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Publishers ON Publishers.publisherInternalId==Publications.publisher
            GROUP BY Publications.publicationInternalId;"""
            mostcitedvenue=read_sql(query, con)
            mostcitedvenue= mostcitedvenue.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id"})
        return mostcitedvenue


 
    def getVenuesByPublisherID(self,publisherid):
        with connect(self.dbPath) as con:
            query="""SELECT Venues.venueInternalId, Publications.publicationVenue, Venues.venueID, Publishers.organizationID
            FROM Publications
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId 
            LEFT JOIN Publishers ON Publishers.publisherInternalId==Publications.publisher
            WHERE Publications.publisher LIKE '%{0}%'
            GROUP BY Publications.publicationInternalId;""".format(publisherid)
            venbypub=read_sql(query, con)
        return venbypub
    def getPublicationInVenue(self,venueid):
        with connect(self.dbPath) as con:
            query ="""SELECT Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, CitedPublications.id_all_references, 
            GROUP_CONCAT(Authors.authorID)
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE Venues.venueID LIKE '%{0}%'
            GROUP BY Publications.doi;""".format(venueid)
            pubinvenue = read_sql(query, con)
            pubinvenue= pubinvenue.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id"})
        return pubinvenue
    def getJournalArticlesInIssue(self,issue, volume,journal_id):
        with connect(self.dbPath) as con:
            query ="""SELECT Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, CitedPublications.id_all_references, 
            GROUP_CONCAT(Authors.authorID), Publications.issue, Publications.volume
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE  Publications.issue = '{0}' AND Publications.volume = '{1}' AND Venues.venueID LIKE '%{2}%'
            GROUP BY Publications.publicationInternalId;""".format(issue,volume,journal_id)
            journalissue = read_sql(query, con)
            journalissue= journalissue.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id"})
        return journalissue
    def getJournalArticlesInVolume(self,volume,journal_id):
        with connect(self.dbPath) as con:
            query ="""SELECT Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, CitedPublications.id_all_references, 
            GROUP_CONCAT(Authors.authorID), Publications.issue, Publications.volume
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE Publications.volume = '{0}' AND Venues.venueID LIKE '%{1}%'
            GROUP BY Publications.publicationInternalId;""".format(volume,journal_id)
            journalvolume = read_sql(query, con)
            journalvolume= journalvolume.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id"})
        return journalvolume
    def getJournalArticlesInJournal(self,journal_id):
        with connect(self.dbPath) as con:
            query ="""SELECT Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, CitedPublications.id_all_references, 
            GROUP_CONCAT(Authors.authorID), Publications.issue, Publications.volume
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE Venues.venueID LIKE '%{0}%'
            GROUP BY Publications.publicationInternalId;""".format(journal_id)
            journaljournal = read_sql(query, con)
            journaljournal= journaljournal.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id"})
        return journaljournal
    def getProceedingsByEvent(self,event):
        with connect(self.dbPath) as con:
            query ="""SELECT Venues.venueInternalId, Publications.publicationVenue, Venues.venueID, Publications.publisher, Publications.event
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE Publications.event LIKE '%{0}%'
            GROUP BY Publications.publicationInternalId;""".format(event)
            procevent= read_sql(query, con)
        return procevent
    def getPublicationAuthors(self,publid):
        with connect(self.dbPath) as con:
            query ="""SELECT Authors.familyName, Authors.givenName, Authors.authorID
            FROM Authors
            LEFT JOIN Publications ON Authors.publicationInternalId == Publications.publicationInternalId
            WHERE Publications.doi == '{0}';""".format(publid)
            publidf= read_sql(query, con)
        return publidf
    def getPublicationsByAuthorName(self,authorname):
        with connect(self.dbPath) as con:
            query ="""SELECT Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, CitedPublications.id_all_references, 
            GROUP_CONCAT(Authors.authorID)
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE Authors.familyName LIKE '%{0}%' OR Authors.givenName LIKE '%{0}%' OR (Authors.familyName AND Authors.givenName COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%')
            GROUP BY Publications.publicationInternalId;""".format(authorname)
            authornamedf = read_sql(query, con)
            authornamedf= authornamedf.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id"})
    def getPublicationsByAuthorName2(self,authorname):
        import re
        with connect(self.dbPath) as con:
            query ="""SELECT Authors.familyName,
            Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, CitedPublications.id_all_references, 
            GROUP_CONCAT(Authors.authorID), Publications.issue, Publications.volume
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE 
            Authors.givenName ||''|| Authors.familyName LIKE '%{0}%' OR
            Authors.givenName ||' '|| Authors.familyName LIKE '%{0}%' OR
            Authors.familyName ||''|| Authors.givenName LIKE '%{0}%' OR
            Authors.familyName ||' '|| Authors.givenName LIKE '%{0}%'
            GROUP BY Publications.publicationInternalId;""".format(authorname)

            authornamedf = read_sql(query, con)
            authornamedf1= authornamedf.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id"})
                
        return authornamedf1
    def getPublicationsByAuthorName1(self,authorname):
        import re
        with connect(self.dbPath) as con:
            query ="""SELECT Authors.familyName,
            Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, CitedPublications.id_all_references, 
            GROUP_CONCAT(Authors.authorID), Publications.issue, Publications.volume
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE 
            Authors.givenName ||''|| Authors.familyName LIKE '%{0}%' OR
            Authors.givenName ||' '|| Authors.familyName LIKE '%{0}%' OR
            Authors.familyName ||''|| Authors.givenName LIKE '%{0}%' OR
            Authors.familyName ||' '|| Authors.givenName LIKE '%{0}%'
            GROUP BY Publications.publicationInternalId;""".format(authorname)

            authornamedf = read_sql(query, con)
            
          
            
            query_2="""SELECT  GROUP_CONCAT(Authors.authorID), Publications.publicationInternalId
            FROM PUBLICATIONS
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            GROUP BY Publications.publicationInternalId;"""
            all_authors= read_sql(query_2, con)
            all_authors= all_authors.rename(columns={"GROUP_CONCAT(Authors.authorID)":"all_authors_id"})
            
            publications_by_author_name_df=authornamedf.merge(all_authors)
        return  publications_by_author_name_df
        

    def getDistinctPublisherOfPublications(self, plist):
        import pandas as pd
        from pandas import DataFrame
        from pandas import concat
        with connect(self.dbPath) as con:
            pubId=DataFrame()
            for el in plist:
                query="""SELECT Publishers.organizationID, Publishers.name
                FROM Publishers
                LEFT JOIN Publications ON Publications.publisher==Publishers.publisherInternalId
                WHERE Publications.doi ='{0}';""".format(el)
                distinctpub_df=read_sql(query, con)
                pubId=pd.concat([pubId, distinctpub_df])
            return pubId

rel_path = "publications.db"
rel_dp = RelationalDataProcessor(rel_path)
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("/Users/martinapensalfini/Documents/data science/relational_publications.csv")
rel_dp.uploadData("/Users/martinapensalfini/Documents/data science/relational_other_data.json")
rel_qp = RelationalQueryProcessor(rel_path)
rel_qp.setDbPath(rel_path) 

# print("1) getPublicationsPublishedInYear:\n",rel_qp.getPublicationsPublishedInYear(2020))
# print("-----------------")
# print("2) getPublicationsByAuthorId:\n",rel_qp.getPublicationsByAuthorId("0000-0002-1111-1828"))
# print("2) getPublicationsByAuthorId:\n",rel_qp.getPublicationsByAuthorId2("0000-0002-1111-1828"))
# print("-----------------")
# print("3) getMostCitedPublication:\n", rel_qp.getMostCitedPublication())
# print("3) getMostCitedPublication:\n", rel_qp.getMostCitedPublication1())
# print("3) getMostCitedPublication:\n", rel_qp.getMostCitedPublication2())
# print("-----------------")
# print("4) getMostCitedVenue:\n", rel_qp.getMostCitedVenue())
# print("-----------------")
# print("5) getVenuesByPublisherId:\n", rel_qp.getVenuesByPublisherID("crossref:78"))
# print("-----------------")
# print("6) getPublicationInVenue:\n", rel_qp.getPublicationInVenue("issn:0944-1344"))
# print("-----------------")
# print("7) getJournalArticlesInIssue:\n", rel_qp.getJournalArticlesInIssue(9, 17, "issn:2164-5515"))
# print("-----------------")
# print("8) getJournalArticlesInVolume:\n", rel_qp.getJournalArticlesInVolume(17, "issn:2164-5515"))
# print("-----------------")
# print("9) getJournalArticlesInJournal:\n", rel_qp.getJournalArticlesInJournal("issn:2164-5515"))
# print("-----------------")# 
# print("10) getProceedingsByEvent:\n", rel_qp.getProceedingsByEvent("meet"))
# print("-----------------")
# print("11) getPublicationAuthors:\n", rel_qp.getPublicationAuthors("doi:10.1080/21645515.2021.1910000"))
# print("-----------------")
# print("12) getPublicationsByAuthorName:\n", rel_qp.getPublicationsByAuthorName("peroni"))
print("12) getPublicationsByAuthorName:\n", rel_qp.getPublicationsByAuthorName1("SilvioPeroni"))
# print("-----------------")
# print("13) getDistinctPublisherOfPublications:\n", rel_qp.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]))
# print("-----------------")
# print(rel_qp.getCitedOfPublication("doi:10.1162/qss_a_00023"))
# print("15) getPublicationsByAuthorName:\n", rel_qp.getPublicationsByAuthorNameProva("per"))

