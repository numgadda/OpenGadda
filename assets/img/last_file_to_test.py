import string
from statistics import mode
from pandas import concat
from sparql_dataframe import get
from pandas import DataFrame
from pandas import concat
from rdflib import Graph
from rdflib import URIRef
from rdflib import RDF
from rdflib import Literal
from pandas import merge 
from pandas import Series
from pandas import DataFrame
from sqlite3 import connect
from pandas import read_sql
from pandas import read_csv
from sqlite3 import connect
from json import load
import pandas as pd


   


class IdentifiableEntity (object): #in Python all new classes must be subclass of the generic class object
    def __init__(self, identifiers):
        self.identifiers=identifiers
    def getIds(self):
       
        return self.identifiers

class Person (IdentifiableEntity):
    def __init__(self, identifiers, givenName, familyName):
        self.givenName= givenName
        self.familyName = familyName
        #we recall the constructor of the superclass to handle the 
        #input parameters as done in the superclass
        super().__init__(identifiers)
    def getGivenName(self):
        return self.givenName
    def getFamilyName(self):
        return self.familyName
    

class Organization (IdentifiableEntity):
    def __init__(self, identifiers, name):
        self.name = name
        super().__init__(identifiers)
    def getName(self):
        return self.name
        
class Venue (IdentifiableEntity):
    def __init__(self, identifiers, title, publisher):
        self.title= title
        self.publisher=publisher
        super().__init__(identifiers)
    def getTitle(self):
        return self.title
    def getPublisher(self):
        return self.publisher

class Publication (IdentifiableEntity):
    def __init__(self, identifiers, publicationYear, title, author, publicationVenue, citedpublication):
        self.publicationYear= publicationYear
        self.title= title
        self.author =author
        self.publicationVenue = publicationVenue
        self.citedpublication = citedpublication
        super().__init__(identifiers)
    def getPublicationYear(self):
        if self.publicationYear == int:
            return self.publicationYear
        else:
            return None
    def getTitle(self):
        return self.title
    def getCitedPublication(self):
        
        return self.citedpublication
    def getPublicationVenue(self):
        return self.publicationVenue
    def getAuthors(self):
        return self.author

class JournalArticle (Publication):
    def __init__(self, identifiers, publicationYear, title, author, publicationVenue, issue, volume, citedpublication):
        self.issue = issue
        self.volume = volume
        super().__init__(identifiers, publicationYear, title, author, publicationVenue, citedpublication)
    def getIssue(self):
        return self.issue
        

    def getVolume(self):
        return self.volume

class BookChapter (Publication):
    def __init__(self, identifiers, publicationYear, title, author, publicationVenue, chapterNumber, citedpublication):
        self.chapterNumber = chapterNumber
        super().__init__(identifiers, publicationYear, title, author, publicationVenue, citedpublication)
    def getChapterNumber(self):
        return self.chapterNumber

class ProceedingsPaper (Publication):
    pass

class Journal (Venue):
    pass

class Book (Venue):
    pass

class Proceedings (Venue):
    def __init__(self, identifiers, title, publisher, event):
        self.event = event
        super().__init__(identifiers, title, publisher)
    def getEvent(self):
        return self.event

#additional classes:
class RelationalProcessor(object): 
    def __init__(self, dbPath=None):
        self.dbPath= dbPath
    def getDbPath(self):
        return self.dbPath
    def setDbPath(self, path):
        if path!='':
            self.dbPath=path
            return True
        else:
            return False

class TriplestoreProcessor(object): 
    def _init_(self,endpointUrl = None):
        self.endpointUrl=endpointUrl   
    def getEndpointUrl(self):
        return self.endpointUrl
    def setEndpointUrl(self, Url):
        if Url != '':
            self.endpointUrl = Url
            return True
        else:
            return False         


import sqlite3
import os

from sqlite3 import connect
from pandas import read_sql
from pandas import merge 
from json import load 
from pandas import read_csv
import pandas as pd
from sqlite3 import connect
from pandas import DataFrame
from pandas import Series



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
            # print(df_rel_publications["publisher"])

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
            print(df_publishers)
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
                
            

from sqlite3 import connect
from pandas import read_sql

class TriplestoreDataProcessor (TriplestoreProcessor):
    
    def __init__(self,endpointUrl=None):
        super().__init__()
    def uploadData (self, path):
        
        #creation of an empty graph
        my_graph = Graph()

        #classes of resources 
        Person= URIRef("https://schema.org/Person")
        JournalArticle= URIRef ("https://schema.org/ScholarlyArticle")
        BookChapter=URIRef ("https://schema.org/Chapter")
        Journal= URIRef("https://schema.org/Periodical")
        Book= URIRef("https://schema.org/Book")
        Organization= URIRef("https://schema.org/Organization")
        ProceedingsPaper= URIRef("https://schema.org/Article")
        Proceedings= URIRef("https://schema.org/Event")

        #attributes related to classes
        givenName = URIRef("https://schema.org/givenName")
        familyName = URIRef("https://schema.org/familyName")
        id = URIRef("https://schema.org/identifier")
        publicationYear = URIRef("https://schema.org/datePublished")
        title = URIRef("https://schema.org/name")
        issue = URIRef("https://schema.org/issueNumber")
        volume = URIRef("https://schema.org/volumeNumber")
        name = URIRef("https://schema.org/name")
        chapterNumber = URIRef("https://schema.org/Number")
        event = URIRef("https://schema.org/releasedEvent")
        n_citations = URIRef("http://purl.org/spar/cito/isCitedBy")

        #relations among classes
        publicationVenue = URIRef("https://schema.org/isPartOf")
        publisher= URIRef("https://schema.org/publisher")
        cites = URIRef ("https://schema.org/citation")
        author = URIRef("https://schema.org/author")
        creators = URIRef ("https://schema.org/creator")
        all_citations = URIRef ("https://schema.org/relatedLink")

        base_url = "https://my_little_py.github-io/res/"

        
        if path.endswith(".json"):
            from json import load 
            with open(path, "r", encoding="utf-8") as f:
                graph_data = load(f)


            #authors' dataframe

            findex=0
            for x in graph_data["authors"]:
                for y in graph_data["authors"][x]:
                    findex+=1

            df_author=pd.DataFrame(columns=["family","given","orcid"],index=range(findex))#dataframe with authors information
            ind=0
            for x in graph_data["authors"]:
                for y in graph_data["authors"][x]:
                    df_author.iloc[ind] = (y["family"],y["given"],y["orcid"])
                    ind+=1

            #dataframe with id publications and authors' orcid 
            df_pub_author = pd.DataFrame (columns=["id", "orcid"], index=range(findex)) #dataframe with doi and orcid of the authors
            ind =0
            for x in graph_data["authors"]:
                for y in graph_data["authors"][x]:
                    df_pub_author.iloc[ind] = (x,y["orcid"])
                    ind+=1

            #dataframe with publications'doi and all authors id
            all_authors = dict()
            for x in graph_data["authors"]:
                all_authors[x] = list()
                for y in graph_data["authors"][x]:
                    all_authors[x].append(y["orcid"])

            findex = 0
            for x in all_authors:
                findex += 1
            df_all_authors_id = pd.DataFrame(columns = ["doi","all_authors_id"], index= range(findex))
            ind =0
            for x in all_authors:
                df_all_authors_id.iloc[ind] = (x, all_authors[x])
                ind +=1
            print(df_all_authors_id)
            df_all_authors_id
                    
            #statements with authors as subject
            author_internal_id = {}
            for idx, row in df_author.iterrows():
                local_id ="author-" + str(row["orcid"])

                subj=URIRef (base_url + local_id)

                author_internal_id[row["orcid"]] = subj
                author_fullname=(str(row["given"])+ ""+ str(row["family"]))

                my_graph.add((subj, RDF.type, Person))
                my_graph.add((subj, givenName, Literal(row["given"])))
                my_graph.add((subj, familyName, Literal(row["family"])))
                my_graph.add((subj, name, Literal(author_fullname)))
                my_graph.add((subj, id, Literal(row["orcid"])))
            
            #dataframe for publishers 
            findex = 0
            for x in graph_data["publishers"]:
                findex+=1
            df_publishers = pd.DataFrame(columns=["id", "name"], index=range(findex))
            ind=0 
            for x in graph_data["publishers"]:
                df_publishers.iloc[ind] = (graph_data["publishers"][x]["id"], graph_data["publishers"][x]["name"])
                ind +=1
            #statements with publishers as subject
            for idx, row in df_publishers.iterrows():
                local_id = "publisher-" + str(row["id"])

                subj= URIRef (base_url + local_id)

                my_graph.add((subj, RDF.type, Organization))
                my_graph.add((subj, name, Literal(row["name"])))
                my_graph.add((subj, id, Literal(row["id"])))

            #dataframe with publications'doi and venues id
            findex = 0
            for x in graph_data["venues_id"]:
                findex += 1
            df_pub_venues = pd.DataFrame(columns = ["doi","venues_id"], index= range(findex))
            ind =0
            for x in graph_data["venues_id"]:
                df_pub_venues.iloc[ind] = (x, graph_data["venues_id"][x])
                ind +=1

            #statements with venues as subject
            venue_internal_id = dict()
            for idx, row in df_pub_venues.iterrows():

                local_id = "venue-" + str(row["doi"])
                subj = URIRef (base_url + local_id)
                venue_internal_id[str(row["venues_id"])] = subj

                my_graph.add((subj, id, Literal(row["venues_id"])))

            #references 
            findex=0
            for x in graph_data["references"]:
                for y in graph_data["references"][x]:
                    findex+=1
            df_references = pd.DataFrame(columns=["id", "id_references"], index=range(findex))
            ind = 0
            for x in graph_data["references"]:
                for y in graph_data["references"][x]:
                    df_references.iloc[ind]= (x, y)
                    ind += 1
            #dataframe with all the references 
            findex=0
            for x in graph_data["references"]:
                findex+=1
            df_all_references = pd.DataFrame(columns=["id", "id_all_references"], index=range(findex))
            ind = 0
            for x in graph_data["references"]:
                df_all_references.iloc[ind]= (x, graph_data["references"][x])
                ind += 1
            #dictionary with publications as keys and number of times a publication is cited as values
            dic_citations = {}
            for ind, x in df_references.iterrows():
                if x["id_references"] not in dic_citations:
                    dic_citations[x["id_references"]] = 1
                else:
                    dic_citations[x["id_references"]] += 1
            #publications dataframe
            findex=0
            for x in graph_data["authors"]:
                findex += 1
            df_doi = pd.DataFrame (columns= ["id"], index = range(findex))
            ind = 0
            for x in graph_data["authors"]:
                df_doi.iloc[ind] = (x)
                ind +=1
            #statements with publications as subject
            publication_internal_id= {}
            
            for idx, row in df_doi.iterrows():
                
                local_id = "publication-" + str(row["id"])
                subj = URIRef (base_url + local_id)
                    
                publication_internal_id[row["id"]] = subj

            for idx, row in df_doi.iterrows():
                local_id = "publication-" + str(row["id"])
                subj = URIRef (base_url + local_id)


                if row["id"] in dic_citations.keys():
                      my_graph.add((subj, n_citations, Literal(dic_citations[row["id"]])))
                #predicate author
                for ind, ref in df_pub_author.iterrows():
                    if ref["id"] == row["id"]:
                        my_graph.add((subj, author, author_internal_id[ref["orcid"]]))
                
                for ind, ref in df_all_authors_id.iterrows():
                    if ref["doi"] == row["id"]:
                        if ref["doi"] in all_authors:
                            my_graph.add((subj, creators, Literal(all_authors[ref["doi"]])))

                
                #predicate isPartOf
                for ind, ref in df_pub_venues.iterrows():
                    if ref["doi"] == row["id"]:
                        my_graph.add((subj, publicationVenue, venue_internal_id[str(ref["venues_id"])]))
                
                #predicate cites
                for ind, ref in df_references.iterrows():
                    if ref["id"] == row["id"]:
                        if ref["id_references"] in publication_internal_id:
                            my_graph.add((subj, cites, publication_internal_id[ref["id_references"]]))
                
                for ind, ref in df_all_references.iterrows():
                    if ref["id"] == row["id"]:
                        my_graph.add((subj, all_citations, Literal(ref["id_all_references"])))



        
        if path.endswith(".csv"):
            from pandas import read_csv
            df_graph_publications = read_csv (path,
                                  keep_default_na=False,
                                  encoding="utf-8",
                                dtype={
                               "id": "string",
                               "title": "string",
                               "type": "string",
                               "publication year": "int",
                               "issue": "string",
                               "volume": "string",
                               "chapter": "string",
                               "publication venue": "string",
                               "venue_type": "string",
                               "publisher":"string",
                               "event":"string"            
                            }) 
            
            #venues
            for idx, row in df_graph_publications.iterrows():
                local_id= "venue-" + str(row["id"])
                subj = URIRef(base_url + local_id)

                if row["venue_type"] == "journal":
                    my_graph.add((subj, RDF.type, Journal))
    
                if row["venue_type"] == "book":
                    my_graph.add((subj, RDF.type, Book))

                if row["venue_type"] == "proceedings":
                    my_graph.add((subj, RDF.type, Proceedings))
                    my_graph.add((subj, event, Literal(row["event"])))
                
                my_graph.add((subj, title, Literal(row["publication_venue"])))
                my_graph.add((subj, publisher, URIRef(base_url + "publisher-" + str(row["publisher"]))))
            
          
            
            #publications
            
            for idx, row in df_graph_publications.iterrows():
                local_id= "publication-" + str(row["id"])

                subj= URIRef(base_url + local_id)
                   
                if row["type"] == "journal-article":
                    my_graph.add((subj, RDF.type, JournalArticle))
                    my_graph.add((subj, issue, Literal(row["issue"])))
                    my_graph.add((subj, volume, Literal(row["volume"])))
                
                if row["type"] == "book-chapter":
                    my_graph.add((subj, RDF.type, BookChapter))
                    my_graph.add((subj, chapterNumber, Literal(row["chapter"])))
                
                if row["type"] == "proceedings-paper":
                    my_graph.add((subj, RDF.type, ProceedingsPaper))
                

                my_graph.add((subj, title, Literal(str(row["title"]))))
                my_graph.add((subj, id, Literal(row["id"])))
                my_graph.add((subj, publicationYear, Literal(str(row["publication_year"]))))
            
            

   
        from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

        store = SPARQLUpdateStore()

        store.open((self.endpointUrl, self.endpointUrl))

        for triple in my_graph.triples((None, None, None)):
            store.add(triple)

        store.close()

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
            Publications.publicationYear, Venues.venueID, CitedPublications.id_all_references
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
        
    def getMostCitedPublication(self):
        with connect(self.dbPath) as con:
            query ="""SELECT DISTINCT Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, GROUP_CONCAT(Authors.authorID), Cited
            FROM (SELECT CitedPublications1."citedPublications", COUNT (CitedPublications1."citedPublications") as Cited
            FROM CitedPublications1
            GROUP BY CitedPublications1."citedPublications"
            ORDER BY Cited DESC
            LIMIT 1)
            LEFT JOIN Publications ON Publications.doi=="citedPublications"
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
            LEFT JOIN Publications ON Publications.doi=="citedPublications"
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Publishers ON Publishers.publisherInternalId==Publications.publisher
            GROUP BY Publications.publicationInternalId;"""
            mostcitedvenue=read_sql(query, con)
            mostcitedvenue= mostcitedvenue.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id", "publicationVenue":"title"})
        return mostcitedvenue


 
    def getVenuesByPublisherId(self,publisherid):
        with connect(self.dbPath) as con:
            query="""SELECT Venues.venueInternalId, Publications.publicationVenue, Venues.venueID, Publishers.organizationID
            FROM Publications
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId 
            LEFT JOIN Publishers ON Publishers.publisherInternalId==Publications.publisher
            WHERE Publications.publisher LIKE '%{0}%'
            GROUP BY Publications.publicationInternalId;""".format(publisherid)
            venbypub=read_sql(query, con)
            venbypub=venbypub.rename(columns={"publicationVenue":"title"})
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
            query ="""SELECT Venues.venueInternalId, Publications.publicationVenue, Venues.venueID, Publications.publisher
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE Publications.event LIKE '%{0}%'
            GROUP BY Publications.publicationInternalId;""".format(event)
            procevent= read_sql(query, con)
            procevent=procevent.rename(columns={"publicationVenue":"title", "publisher":"organizationID"})
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
        lower_authorname = authorname.lower()
        with connect(self.dbPath) as con:
            query ="""SELECT Publications.publicationInternalId, Publications.doi, Publications.title, 
            Publications.publicationYear, Venues.venueID, CitedPublications.id_all_references, 
            GROUP_CONCAT(Authors.authorID), Publications.issue, Publications.volume
            FROM Publications
            LEFT JOIN Authors ON Authors.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN Venues ON Venues.publicationInternalId == Publications.publicationInternalId
            LEFT JOIN CitedPublications ON CitedPublications.publicationInternalId == Publications.publicationInternalId
            WHERE lower(Authors.familyName) LIKE '%{0}%' OR lower(Authors.givenName) LIKE '%{0}%' 
            GROUP BY Publications.publicationInternalId;""".format(lower_authorname)
            authornamedf = read_sql(query, con)
            authornamedf= authornamedf.rename(columns={"GROUP_CONCAT(Authors.authorID)": "all_authors_id"})
        return authornamedf
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

def remove_dotzero(s):
    return s.replace(".0", "") 

class TriplestoreQueryProcessor (TriplestoreProcessor):
    

    def _init_(self, endpointUrl):
        super()._init_(endpointUrl)
    
    #methods:
    def getPublicationsPublishedInYear (self, year):
        
        query_1= [ """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <https://schema.org/>
        SELECT ?internalID ?doi ?type ?title ?publicationYear ?chapterNumber ?issue ?volume ?venueID ?all_authors_id ?id_all_references
          WHERE {
            VALUES ?type {
                schema:ScholarlyArticle schema:Chapter schema:Article}
            ?internalID rdf:type ?type .
            ?internalID schema:identifier ?doi.
            ?internalID schema:name ?title.
            ?internalID schema:creator ?all_authors_id.
            ?internalID schema:datePublished ?publicationYear.
            ?internalID schema:datePublished""", str("'"+year+"'"), ".", 
            """OPTIONAL {?internalID schema:isPartOf ?publicationVenue.
            ?publicationVenue schema:identifier ?venueID.}
                                
           
            OPTIONAL { ?internalID schema:Number ?chapterNumber .}
            OPTIONAL {?internalID schema:issueNumber ?issue .}
            OPTIONAL {?internalID schema:volumeNumber ?volume .}
            OPTIONAL {?internalID schema:relatedLink ?id_all_references.}
            
          }
          """
            ]
        stringa_1 = (" ".join(query_1))
        df_publications_by_year = get(TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), stringa_1, True)
        df_publications_by_year["chapterNumber"] =  df_publications_by_year["chapterNumber"].astype("string")
        df_publications_by_year["issue"]=df_publications_by_year["issue"].astype("string")
        df_publications_by_year["volume"]= df_publications_by_year["volume"].astype("string")
        df_publications_by_year = df_publications_by_year. fillna("")
        df_publications_by_year["chapterNumber"] =  df_publications_by_year["chapterNumber"].apply(remove_dotzero)
        df_publications_by_year["issue"]=df_publications_by_year["issue"].apply(remove_dotzero)
        df_publications_by_year["volume"]= df_publications_by_year["volume"].apply(remove_dotzero)
        df_publications_by_year.drop_duplicates(subset="doi", keep = "first", inplace=True)
        return df_publications_by_year
   
#TriplestoreQueryProcessor.getPublicationsPublishedInYear(TriplestoreQueryProcessor, year="2020")
    
    def getPublicationsByAuthorId(self, author_id):
        query_2=["""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <https://schema.org/>
     
 SELECT DISTINCT ?internalID ?doi ?type ?title ?issue ?volume ?chapterNumber ?publicationYear ?venueID ?all_authors_id ?id_all_references
         
        WHERE {
            VALUES ?type {
                schema:ScholarlyArticle schema:Chapter schema:Article}
            ?internalID rdf:type ?type .
            ?internalID schema:identifier ?doi.
            ?internalID schema:name ?title.
            ?internalID schema:datePublished ?publicationYear.
            OPTIONAL {?internalID schema:isPartOf ?publicationVenue.
            ?publicationVenue schema:identifier ?venueID.}
            ?internalID schema:creator ?all_authors_id. 
            ?internalID schema:author ?author.
            ?author schema:identifier """, str("'"+author_id+"'"), ".",
                         
            """OPTIONAL {?internalID schema:Number ?chapterNumber .}
            OPTIONAL {?internalID schema:issueNumber ?issue .}
            OPTIONAL {?internalID schema:volumeNumber ?volume .}
            OPTIONAL {?internalID schema:relatedLink ?id_all_references.}
            }     
"""]    
        stringa_2 = (" ".join(query_2))
        df_publications_author_id = get (TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), stringa_2, True)
        df_publications_author_id["chapterNumber"] =  df_publications_author_id["chapterNumber"].astype("string")
        df_publications_author_id["issue"]=df_publications_author_id["issue"].astype("string")
        df_publications_author_id["volume"]= df_publications_author_id["volume"].astype("string")
        df_publications_author_id = df_publications_author_id. fillna("")
        df_publications_author_id["chapterNumber"] =  df_publications_author_id["chapterNumber"].apply(remove_dotzero)
        df_publications_author_id["issue"]=df_publications_author_id["issue"].apply(remove_dotzero)
        df_publications_author_id["volume"]= df_publications_author_id["volume"].apply(remove_dotzero)
        df_publications_author_id.drop_duplicates(subset="doi", keep="first", inplace=True)
        return df_publications_author_id
    
    def getMostCitedPublication (self):
        query_3="""
    PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX schema: <https://schema.org/>
    PREFIX cito: <http://purl.org/spar/cito/>
    SELECT ?Cited ?internalID ?doi ?title ?publicationYear ?venueID ?chapterNumber ?issue ?volume ?all_authors_id ?id_all_references
    WHERE{
        VALUES ?type {
        schema:ScholarlyArticle schema:Chapter schema:Article}
        ?internalID rdf:type ?type. 
        ?internalID cito:isCitedBy ?Cited.
        ?internalID schema:identifier ?doi.
        ?internalID schema:name ?title.
        ?internalID schema:datePublished ?publicationYear.
        ?internalID schema:author ?author.
        ?internalID schema:creator ?all_authors_id.
        OPTIONAL{?internalID schema:isPartOf ?publicationVenue.
        ?publicationVenue schema:identifier ?venueID.}
        OPTIONAL {?internalID schema:citation ?cites.
        ?internalID schema:relatedLink ?id_all_references.}
        OPTIONAL {?internalID schema:chapterNumber ?chapter.}
        OPTIONAL {?internalID schema:issue ?issue.}
        OPTIONAL {?internalID schema:volume ?volume.}
        
    }
    ORDER BY DESC(?Cited)
    LIMIT 1"""

        df_most_cited_publication = get(TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), query_3, True)
        
        #transforming the columns of type float64 in strings 
        df_most_cited_publication["chapterNumber"] = df_most_cited_publication["chapterNumber"].astype("string")
        df_most_cited_publication["volume"] = df_most_cited_publication["volume"].astype("string")
        #removing the NaN
        df_most_cited_publication = df_most_cited_publication.fillna("")
        #remove .0
        df_most_cited_publication["chapterNumber"] = df_most_cited_publication["chapterNumber"].apply(remove_dotzero)
        df_most_cited_publication["volume"] = df_most_cited_publication["volume"].apply(remove_dotzero)
        return df_most_cited_publication
    
    
    def getMostCitedVenue(self):
        
        query_2="""
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
         SELECT (SAMPLE(?cit_venue) AS ?internalID) (SAMPLE (?Id) AS ?venueID)(COUNT(?Id) as ?Cited) (SAMPLE(?Type) AS ?type) 
         (SAMPLE(?Title) AS ?title) (SAMPLE(?organizationID) AS ?organizationID)
        (SAMPLE(?Event) AS ?event)
           WHERE{
             VALUES ?type {
             schema:ScholarlyArticle schema:Chapter schema:Article}
             ?internalId rdf:type ?type .
             ?internalId schema:citation ?cited_publications.
             ?cited_publications schema:isPartOf ?cit_venue. 
             ?cit_venue schema:identifier ?Id.
             ?cit_venue rdf:type ?Type.
             ?cit_venue schema:name ?Title.
             ?cit_venue schema:publisher ?Organization.
             ?Organization schema:identifier ?organizationID.
             OPTIONAL {
             ?cit_venue schema:releasedEvent ?Event.
             }
           }
        GROUP BY?Id
        ORDER BY DESC(?Cited)
        LIMIT 1
        """

        df_most_cited_venue = get (TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), query_2, True)
        df_most_cited_venue["event"] = df_most_cited_venue["event"].astype("string")
        df_most_cited_venue = df_most_cited_venue.fillna("")
        df_most_cited_venue["event"] = df_most_cited_venue["event"].apply(remove_dotzero)
        return df_most_cited_venue





#TriplestoreQueryProcessor.getMostCitedVenue(TriplestoreQueryProcessor)


    def getVenuesByPublisherId (self, publisher_id):
       
                               
        query_5= ["""
                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>
                SELECT ?internalID ?venueID ?type ?title ?event ?organizationID
                WHERE{
                 VALUES ?type {
                 schema:Book schema:Periodical schema:Event}
                 ?internalID rdf:type ?type .
                 ?internalID schema:identifier ?venueID. 
                 ?internalID schema:publisher ?publisher.
                 ?publisher schema:identifier ?organizationID.
                 ?publisher schema:identifier""", str("'"+ publisher_id + "'"), ".",
                 """?internalID schema:name ?title.
                 OPTIONAL {
                 ?internalID schema:releasedEvent ?event.
                 }
               }"""]
        stringa_3 = (" ".join(query_5))
        df_venues_by_publisher_id = get(TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), stringa_3, True)
        #transforming the values in the column Event (float64) in strings
        df_venues_by_publisher_id["event"] = df_venues_by_publisher_id["event"].astype("string")
        #removing the NaN
        df_venues_by_publisher_id = df_venues_by_publisher_id.fillna("")
        #removing the .0
        df_venues_by_publisher_id["event"] = df_venues_by_publisher_id["event"].apply(remove_dotzero)
        df_venues_by_publisher_id.drop_duplicates(subset="venueID", keep="first", inplace=True)
        return df_venues_by_publisher_id




#TriplestoreQueryProcessor.getVenuesByPublisherId(TriplestoreQueryProcessor, i = "crossref:78")
    
    def getPublicationInVenue(self, venue_id):
       
        query = [ """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT ?internalID ?doi ?type ?title ?issue ?volume ?chapterNumber ?publicationYear ?venueID ?all_authors_id ?id_all_references
        WHERE{
     VALUES ?type {
     schema:ScholarlyArticle schema:Chapter schema:Article}
     ?internalID rdf:type ?type .
     ?internalID schema:isPartOf ?publicationVenue.
     ?publicationVenue schema:identifier ?venueID.
     ?internalID schema:identifier ?doi.
     ?internalID schema:name ?title.
     ?internalID schema:datePublished ?publicationYear.
     ?internalID schema:author ?author.
     ?internalID schema:creator ?all_authors_id.
     
     OPTIONAL {?internalID schema:relatedLink ?id_all_references.}
     OPTIONAL {?internalID schema:Number ?chapterNumber .}
     OPTIONAL {?internalID schema:issueNumber ?issue .}
     OPTIONAL {?internalID schema:volumeNumber ?volume .}
    FILTER REGEX (?venueID, """, str("'"+venue_id+"'"), """, "i").
   }
""" ]
        stringa= (" ".join(query))
        df_final = get(TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), stringa, True)

        #transforming the columns of type float64 in strings 
        df_final["chapterNumber"] = df_final["chapterNumber"].astype("string")
        df_final["issue"]=df_final["issue"].astype("string")
        df_final["volume"] = df_final["volume"].astype("string")
        #removing the NaN
        df_final = df_final.fillna("")
        #remove .0
        df_final["chapterNumber"] = df_final["chapterNumber"].apply(remove_dotzero)
        df_final["issue"]=df_final["issue"].apply(remove_dotzero)
        df_final["volume"] = df_final["volume"].apply(remove_dotzero)
        df_final.drop_duplicates(subset="doi", keep="first", inplace=True)
        return df_final
  
                


#get publ by  pub year
    def getJournalArticlesInIssue(self, input_issue, input_volume, input_venue_id):
        
        query_7 =["""
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT ?internalID ?title ?doi ?publicationYear ?issue ?volume ?all_authors_id ?venueID ?id_all_references
        WHERE {
        
            ?internalID rdf:type schema:ScholarlyArticle.
            ?internalID schema:name ?title.
            ?internalID schema:identifier ?doi.
            ?internalID schema:isPartOf ?publicationVenue.
            ?publicationVenue schema:identifier ?venueID.
            ?internalID schema:volumeNumber ?volume.
            ?internalID schema:issueNumber ?issue.
            ?internalID schema:volumeNumber""", str("'"+ input_volume + "'"), ".",
            """?internalID schema:issueNumber""", str("'"+ input_issue + "'"), ".",
            """?internalID schema:datePublished ?publicationYear.
            ?internalID schema:author ?author.
            ?internalID schema:creator ?all_authors_id.
            OPTIONAL {?internalID schema:relatedLink ?id_all_references.}
            FILTER REGEX (?venueID, """, str("'"+input_venue_id+"'"), """, "i").
        }"""]
        stringa=(" ".join(query_7))
        df_result = get(TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), stringa, True)
        df_result.drop_duplicates(subset="doi", keep="first", inplace=True)
        return df_result
   

    def getJournalArticlesInVolume(self, input_volume, input_venue_id):
        
        query_8 =["""
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT ?internalID ?title ?doi ?publicationYear ?issue ?volume ?all_authors_id ?venueID ?id_all_references
        WHERE {
        
            ?internalID rdf:type schema:ScholarlyArticle.
            ?internalID schema:name ?title.
            ?internalID schema:identifier ?doi.
            ?internalID schema:isPartOf ?publicationVenue.
            ?publicationVenue schema:identifier ?venueID.
            ?internalID schema:volumeNumber ?volume.
            ?internalID schema:volumeNumber""", str("'"+ input_volume + "'"), ".",
            """?internalID schema:issueNumber ?issue.
            ?internalID schema:datePublished ?publicationYear.
            ?internalID schema:author ?author.
            ?internalID schema:creator ?all_authors_id.
            OPTIONAL {?internalID schema:relatedLink ?id_all_references.}
            FILTER REGEX (?venueID, """, str("'"+input_venue_id+"'"), """, "i").
        }"""]
        stringa=(" ".join(query_8))
        df_result = get(TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), stringa, True)
        df_result.drop_duplicates(subset="doi", keep="first", inplace=True)
        return df_result
        
    

    def getJournalArticlesInJournal(self, inputvenueid):
      
        
        query_8=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
                SELECT ?internalID ?title ?doi ?all_authors_id ?publicationYear ?venueID ?issue ?volume ?id_all_references
                WHERE {
                    ?internalID rdf:type schema:ScholarlyArticle.
                    ?internalID schema:name ?title.
                    ?internalID schema:identifier ?doi.
                    ?internalID schema:datePublished ?publicationYear.
                    ?internalID schema:creator ?all_authors_id.
                    ?internalID schema:isPartOf ?publicationVenue.
                    ?publicationVenue  schema:identifier ?venueID.
                    ?internalID schema:issueNumber ?issue.
                    ?internalID schema:volumeNumber ?volume. 
                    OPTIONAL {?internalID schema:relatedLink ?id_all_references.}
                    FILTER REGEX (?venueID, """, str("'"+inputvenueid+"'"), """, "i").
            }"""]
        stringa_4=(" ".join(query_8))

        df_result= get(TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), stringa_4, True)
        df_result["issue"] = df_result["issue"].astype("string")
        df_result["volume"] = df_result["volume"].astype("string")
        #removing the NaN
        df_result= df_result.fillna("")
        #removing the .0
        df_result["issue"] = df_result["issue"].apply(remove_dotzero)
        df_result["volume"] = df_result["volume"].apply(remove_dotzero)
        df_result.drop_duplicates(subset="doi", keep = "first", inplace=True)
        return df_result



    def getProceedingsByEvent(self, input_name):
       
        query_9=["""
    PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX schema: <https://schema.org/>
    SELECT ?venueID ?organizationID ?title
    WHERE {?internalId rdf:type schema:Event.
           ?internalId schema:releasedEvent""", str("'"+ input_name + "'"), ".",
           """
           ?internalId  schema:name ?title.
           ?internalId  schema:identifier ?venueID.
           
           ?internalId schema:publisher ?publisher.
           ?publisher schema:identifier ?organizationID.}
    """]
        stringa_5=(" ".join(query_9))

        df_result= get(TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), stringa_5, True)
        df_result.drop_duplicates(subset="venueID", keep = "first", inplace=True)
        return df_result

#TriplestoreQueryProcessor.getProceedingsByEvent(TriplestoreQueryProcessor, "web" )
        


    def getPublicationAuthors(self, inputPubId):
       
        query= ["""
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT ?internalID ?authorID ?givenName ?familyName 
        WHERE{ ?publication schema:identifier""", str("'"+inputPubId+"'"), ";", """  schema:author   ?internalID.
            ?internalID schema:identifier ?authorID;
                    schema:givenName ?givenName;
                    schema:familyName ?familyName.
        }
        """]
        stringa=(" ".join(query))
        df_final = get (TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), stringa, True)
        return df_final



    def getPublicationsByAuthorName(self, inputstring):
        
        query_10=["""
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX schema: <https://schema.org/>
SELECT ?internalID ?title ?doi ?publicationYear ?venueID ?type ?issue ?volume ?chapterNumber ?all_authors_id ?id_all_references
    WHERE{ 
    ?internalID schema:author ?author;
                schema:name ?title;
                schema:datePublished ?publicationYear;
                schema:identifier ?doi;
                rdf:type ?type.
    OPTIONAL{?internalID schema:isPartOf ?publicationVenue.
    ?publicationVenue schema:identifier ?venueID.}
                            
      
    ?author  schema:name ?fullname.
    ?author  schema:identifier ?authorID.
    
    ?internalID schema:creator ?all_authors_id.         
    FILTER REGEX(?fullname,""",str("'"+inputstring+"'"), """, "i").
           
    OPTIONAL {?internalID schema:issueNumber ?issue.
             ?internalID schema:volumeNumber ?volume.
             }
    OPTIONAL {?internalID schema:Number ?chapterNumber.}
    OPTIONAL {?internalID schema:relatedLink ?id_all_references.}
    }
"""]
        stringa=(" ".join(query_10))
        df_final = get (TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), stringa, True)
        df_final["chapterNumber"] = df_final["chapterNumber"].astype("string")
        df_final["issue"]=df_final["issue"].astype("string")
        df_final["volume"] = df_final["volume"].astype("string")
        #removing the NaN
        df_final = df_final.fillna("")
        #remove .0
        df_final["chapterNumber"] = df_final["chapterNumber"].apply(remove_dotzero)
        df_final["issue"]=df_final["issue"].apply(remove_dotzero)
        df_final["volume"] = df_final["volume"].apply(remove_dotzero)
        df_final.drop_duplicates(subset="doi", keep="first", inplace=True)
        return df_final
        
    def getDistinctPublisherOfPublications(self, inputList):
        
        df=DataFrame()
        
        for inputdoi in inputList:
            query=['''
        
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?internalID ?organizationID ?name  
        WHERE {
        ?publication schema:identifier ''', str("'"+inputdoi+"'"),";",'''
                    schema:name ?title;
                    schema:isPartOf  ?publicationVenue.
        ?publicationVenue  schema:publisher  ?internalID.
        ?internalID schema:name      ?name;
                    schema:identifier ?organizationID.

        }

        
        ''']
            stringa=(" ".join(query))
            df_final = get (TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), stringa,  True)
            df_final.drop_duplicates(subset="organizationID", keep="first", inplace=True)
            df = concat([df, df_final], ignore_index=True)
        return df

class GenericQueryProcessor(object):
    def __init__(self, queryProcessor):
        self.queryProcessor=queryProcessor

    def cleanQueryProcessors(self):
        for obj in self.queryProcessor:
            self.queryProcessor.remove(obj)
            return self.queryProcessor
    def addQueryProcessor(self, input):
        self.queryProcessor.append(input) 
    
    def getPublicationsPublishedInYear(self, inputYear):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor: 
            q=processor.getPublicationsPublishedInYear(inputYear)
            result = concat([result, q],ignore_index=True)
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="doi", keep = "first", inplace=True)
                    
        for row_idx,row in result.iterrows():
            x=Publication(identifiers=row["doi"],title=row["title"],publicationYear=row["publicationYear"],publicationVenue=row["venueID"], author = row["all_authors_id"],
                         citedpublication = row["id_all_references"])  

            self.finalresultlist.append(x)

        print(self.finalresultlist)

        #for x in self.finalresultlist:
            #print(x.getIds(), x.getTitle())
        #return result
    
    def getPublicationsByAuthorId(self, authorId):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor: 
            q=processor.getPublicationsByAuthorId(authorId)
            result = concat([result, q],ignore_index=True)
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="doi", keep = "first", inplace=True)
                    
        for row_idx,row in result.iterrows():
            x=Publication(identifiers=row["doi"],title=row["title"],publicationYear=row["publicationYear"],publicationVenue=row["venueID"], author = row["all_authors_id"],
                         citedpublication = row["id_all_references"])  

            self.finalresultlist.append(x)
        
        print(self.finalresultlist)
        #for x in self.finalresultlist:
         #   print(x, x.getPublicationVenue())
        #return result
    
    def getMostCitedPublication(self):
        self.finalresultlist=[]
        result=DataFrame()
        for processor in self.queryProcessor:
            q= processor.getMostCitedPublication()
            result=concat([q, result], ignore_index=True)

        
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="doi", keep = "first", inplace=True)
                    
        result_sorted = result.sort_values(by=["Cited"], ascending=False)

        first_row = result_sorted.iloc[0]
        
        x=Publication(identifiers=first_row["doi"],title=first_row["title"],publicationYear=first_row["publicationYear"],publicationVenue=first_row["venueID"], author = first_row["all_authors_id"],
                          citedpublication = first_row["id_all_references"])  
        self.finalresultlist.append(x)
        #for x in self.finalresultlist:
            #print(x.getIds())
        print( self.finalresultlist)
        #return result_sorted

    def getMostCitedVenue(self):
        self.finalresultlist=[]
        result=DataFrame()
        for processor in self.queryProcessor:
            q= processor.getMostCitedVenue()
            result=concat([q, result], ignore_index=True)

        
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="venueID", keep = "first", inplace=True)
                    
        result_sorted = result.sort_values(by=["Cited"], ascending=False)

        first_row = result_sorted.iloc[0]
        
     
        x=Venue(identifiers=first_row["venueID"],title=first_row["title"],publisher=first_row["organizationID"])
        self.finalresultlist.append(x)
          
        #for x in self.finalresultlist:
            #print(x.getPublisher())
        print( self.finalresultlist)
        #return first_row

    def getVenuesByPublisherId(self, publisherId):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor:
            q=processor.getVenuesByPublisherId(publisherId)
            
            result = concat([result, q],ignore_index=True)
        
        
        
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="venueID", keep = "first", inplace=True)
                    
        for row_idx,row in result.iterrows():
            x=Venue(identifiers=row["venueID"],title=row["title"],publisher=row["organizationID"])
            self.finalresultlist.append(x)
        
       
        print(self.finalresultlist)
        #return result
        #for x in self.finalresultlist:
            #print(x, x.getPublicationVenue())
    
    def getPublicationInVenue(self, venueId):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor:
            q=processor.getPublicationInVenue(venueId)
            
            result = concat([result, q],ignore_index=True)
        
        
        
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="doi", keep = "first", inplace=True)
                    
        for row_idx,row in result.iterrows():
            x=Publication(identifiers=row["doi"],title=row["title"],publicationYear=row["publicationYear"],publicationVenue=row["venueID"], author = row["all_authors_id"],
                          citedpublication = row["id_all_references"])  
            self.finalresultlist.append(x)
        
        
        print(self.finalresultlist)
        
    
    def getJournalArticlesInIssue(self, inputIssue, inputVolume, inputVenueId):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor:
            q=processor.getJournalArticlesInIssue(inputIssue, inputVolume, inputVenueId)
            
            result = concat([result, q],ignore_index=True)
        
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="doi", keep = "first", inplace=True)
                    
        for row_idx,row in result.iterrows():
            x=JournalArticle(identifiers=row["doi"],title=row["title"],publicationYear=row["publicationYear"], 
                             author = row["all_authors_id"],publicationVenue=row["venueID"],
                          citedpublication = row["id_all_references"],issue = row["issue"], volume = row["volume"])  
            self.finalresultlist.append(x)
        
        #return result
        print(self.finalresultlist)
        #for x in self.finalresultlist:
             #print(x, x.getPublicationVenue())


    def getJournalArticlesInVolume(self, inputVolume, inputVenueId):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor:
            q=processor.getJournalArticlesInVolume( inputVolume, inputVenueId)
            
            result = concat([result, q],ignore_index=True)
        
        
        
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="doi", keep = "first", inplace=True)
                    
        for row_idx,row in result.iterrows():
            x=JournalArticle(identifiers=row["doi"],title=row["title"],publicationYear=row["publicationYear"],publicationVenue=row["venueID"], author = row["all_authors_id"],
                          citedpublication = row["id_all_references"],issue = row["issue"], volume = row["volume"])  
            self.finalresultlist.append(x)
        
        #return result
        print(self.finalresultlist)   
        

    def getJournalArticlesInJournal(self, inputvenueid):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor: 
            q=processor.getJournalArticlesInJournal(inputvenueid)
            result = concat([result, q],ignore_index=True)
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="doi", keep = "first", inplace=True)
                    
        for row_idx,row in result.iterrows():
            x= JournalArticle(identifiers=row["doi"],title=row["title"],publicationYear=row["publicationYear"],publicationVenue=row["venueID"], author = row["all_authors_id"],
                          citedpublication = row["id_all_references"],issue = row["issue"], volume = row["volume"])  

            self.finalresultlist.append(x)
        print(self.finalresultlist)
        
    
    def getProceedingsByEvent(self, inputEvent):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor: 
            q=processor.getProceedingsByEvent(inputEvent)
            result = concat([result, q],ignore_index=True)
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="venueID", keep = "first", inplace=True)
        for row_idx,row in result.iterrows():
            x = Proceedings(identifiers=row["venueID"],title=row["title"],publisher=row["publisherID"], event= row["event"])
            self.finalresultlist.append(x)
        print(self.finalresultlist)
       
    
    def getPublicationAuthors(self, inputPubId):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor:
            q=processor.getPublicationAuthors(inputPubId)
            
            result = concat([result, q],ignore_index=True)
        
        
        
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="authorID", keep = "first", inplace=True)
                    
        for row_idx,row in result.iterrows():
            x=Person(identifiers=row["authorID"], givenName = row["givenName"], familyName = row["familyName"])  
            self.finalresultlist.append(x)
        
        #return result
        print(self.finalresultlist)
        
    def getPublicationsByAuthorName(self, authorname):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor:
            q=processor.getPublicationsByAuthorName(authorname)
            
            result = concat([result, q],ignore_index=True)
        
        #return result
        
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="doi", keep = "first", inplace=True)
                    
        for row_idx,row in result.iterrows():
            x=Publication(identifiers=row["doi"],title=row["title"],publicationYear=row["publicationYear"],publicationVenue=row["venueID"], author = row["all_authors_id"],
                          citedpublication = row["id_all_references"])  
            self.finalresultlist.append(x)
        
        #return result
        print(self.finalresultlist)
        
    
    def getDistinctPublisherOfPublications(self, inputList):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor:
            q=processor.getDistinctPublisherOfPublications(inputList)
            
            result = concat([result, q],ignore_index=True)
        
        
        
        #removing the NaN
        result= result.fillna("")
        result.drop_duplicates(subset="organizationID", keep = "first", inplace=True)
                    
        for row_idx,row in result.iterrows():
            x=Organization(identifiers=row["organizationID"],name=row["name"])
            self.finalresultlist.append(x)
        
        #return result
        print(self.finalresultlist)
        
        
rel_path = "sqlite\publications.db"
rel_dp = RelationalDataProcessor(rel_path)
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("relational_publications.csv")
rel_dp.uploadData("relational_other_data.json")
rel_qp = RelationalQueryProcessor(rel_path)
rel_qp.setDbPath(rel_path) 
rel_qp = RelationalQueryProcessor(rel_path)
rel_qp.setDbPath(rel_path) 
grp_qp = TriplestoreQueryProcessor()
TriplestoreProcessor.setEndpointUrl (TriplestoreProcessor, "http://192.168.1.98:9999/blazegraph/sparql")
generic = GenericQueryProcessor(queryProcessor=[])
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)
generic.getPublicationsByAuthorId("0000-0001-7542-0286")