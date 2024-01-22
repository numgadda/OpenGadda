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
    def __init__(self, identifiers, publicationYear, title, author, publicationVenue,citedpublication):
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
        result=[]
        for publication in self.citedpublication:
            result.append(publication)
        result.sort()
        return result
    def getPublicationVenue(self):
        return self.publicationVenue
    def getAuthors(self):
        return self.author

class JournalArticle (Publication):
    def __init__(self, identifiers, publicationYear, title, author, publicationVenue, issue, volume):
        self.issue = issue
        self.volume = volume
        super().__init__(identifiers, publicationYear, title, author, publicationVenue)
    def getIssue(self):
        if self.issue == string:
            return self.issue
        else:
            return None

    def getVolume(self):
        if self.volume == string:
            return self.volume
        else:
            return None

class BookChapter (Publication):
    def __init__(self, identifiers, publicationYear, title, author, publicationVenue, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(identifiers, publicationYear, title, author, publicationVenue)
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
        self.dbPath=dbPath  
    def getDbPath(self):
        return self.dbPath
    def setDbPath(self, path):
        if path!='':
            self.dbPath=path
            return True
        else:
            return False

class TriplestoreProcessor(object): 
    def _init_(self, endpointUrl=None):
        self.endpointUrl=endpointUrl   
    def getEndpointUrl(self):
        return self.endpointUrl
    def setEndpointUrl(self, Url):
        if Url != '':
            self.endpointUrl = Url
            return True
        else:
            return False     

class TriplestoreDataProcessor(TriplestoreProcessor):
    
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
                
                #predicate isPartOf
                for ind, ref in df_pub_venues.iterrows():
                    if ref["doi"] == row["id"]:
                        my_graph.add((subj, publicationVenue, venue_internal_id[str(ref["venues_id"])]))
                
                #predicate cites
                for ind, ref in df_references.iterrows():
                    if ref["id"] == row["id"]:
                        if ref["id_references"] in publication_internal_id:
                            my_graph.add((subj, cites, publication_internal_id[ref["id_references"]]))



        
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

        endpoint = "http://10.250.13.167:9999/blazegraph/sparql"

        store.open((endpoint, endpoint))

        for triple in my_graph.triples((None, None, None)):
            store.add(triple)

        store.close()         


class RelationalDataProcessor (RelationalProcessor):
    def __init__(self, dbPath=None):
       super().__init__(dbPath)
    def uploadData (self, path):
        if path!='':
            if path.endswith(".csv"):
                df_rel_publications = read_csv(path, 
                                            keep_default_na=False,
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
                # Publication
                publications = df_rel_publications[["id","title","publication_year","publication_venue"]]
                publications = publications.rename(columns={"id":"doi", "publication_year":"publicationYear","publication_venue":"publicationVenue"})
                publication_internal_id = []
                for idx, row in publications.iterrows():
                    publication_internal_id.append("publication-" + str(idx))
                publications.insert(0, "internalID", Series(publication_internal_id, dtype="string"))

                
                # BookChapter
                bookchapters = df_rel_publications.query("type == 'book-chapter'")
                book_chapter = bookchapters[["id","chapter"]]
                book_chapter = book_chapter.rename(columns={"id":"doi","chapter":"chapterNumber"})
                
                # JournalArticle
                journalarticles = df_rel_publications.query("type == 'journal-article'")
                journal_article = journalarticles[["id","issue","volume"]]
                journal_article = journal_article.rename(columns={"id":"doi"})
                
                # ProceedingsPapers
                proceedings_paper = df_rel_publications.query("type=='proceedings-paper'")
                proceedings_paper = proceedings_paper[["id"]]
                proceedings_paper = proceedings_paper.rename(columns={"id":"doi"})
                
                # Venue
                venue=df_rel_publications[["id","publication_venue","publisher"]]
                venue = venue.rename(columns={"id":"doi","publication_venue":"title"})
                venue_internal_id = []
                for idx, row in venue.iterrows():
                    venue_internal_id.append("venue-" + str(idx))
                venue.insert(0, "internalID", Series(venue_internal_id, dtype="string"))

                
                # Proceedings
                proceedings=df_rel_publications.query("type == 'proceedings'")
                proceedings=df_rel_publications[["id","publication_venue","publisher","event"]]
                proceedings = proceedings.rename(columns={"id":"doi","publication_venue":"title"})
                
                # Book
                book=df_rel_publications.query("type == 'book'")
                book=df_rel_publications[["id","publication_venue","publisher"]]
                book = book.rename(columns={"id":"doi","publication_venue":"title"})
                
                # Journal
                journal=df_rel_publications.query("type == 'journal'")
                journal=df_rel_publications[["id","publication_venue","publisher"]]
                journal = journal.rename(columns={"id":"doi","publication_venue":"title"})
                
                

                with connect(self.dbPath) as con:
                    publications.to_sql("Publication", con, if_exists="replace", index=False)
                    journal_article.to_sql("JournalArticle", con, if_exists="replace", index=False)
                    book_chapter.to_sql("BookChapter", con, if_exists="replace", index=False)
                    proceedings_paper.to_sql("ProceedingsPaper", con, if_exists="replace", index=False)
                    venue.to_sql("Venue", con, if_exists="replace", index=False)
                    journal.to_sql("Journal", con, if_exists="replace", index=False)
                    book.to_sql("Book", con, if_exists="replace", index=False)
                    proceedings.to_sql("Proceedings", con, if_exists="replace", index=False)
                    con.commit()
            
                return True
            if path.endswith(".json"):
                with open(path, "r", encoding="utf-8") as f:
                    reldata = load(f)

                    # Author_id
                    findex=0
                    for x in reldata["authors"]:
                        for y in reldata["authors"][x]:
                            findex+=1
                    
                    df_author=pd.DataFrame(columns=["id","family","given","orcid"],index=range(findex))
                    ind=0
                    for x in reldata["authors"]:
                        for y in reldata["authors"][x]:
                            df_author.iloc[ind] = (x,y["family"],y["given"],y["orcid"])
                            ind+=1
                            
                    author_id =df_author[["id","orcid"]]
                    author_id = author_id.rename(columns={"id":"doi","orcid":"authorID"})
                    
                    
                    # Person
                    person=df_author[["orcid","family","given"]]
                    person = person.rename(columns={"orcid":"authorID","family":"familyName","given":"givenName"})
                    author_internal_id = []
                    for idx, row in person.iterrows():
                        author_internal_id.append("author-" + str(idx))
                    person.insert(0, "internalID", Series(author_internal_id, dtype="string"))
                    
                    # Cites
                    findex_references=0
                    for x in reldata["references"]:
                        for y in reldata["references"][x]:
                            findex_references +=1
                    cited_publications = pd.DataFrame(columns=["id", "id_references"], index=range(findex_references))
                    ind = 0
                    for x in reldata["references"]:
                        for y in reldata["references"][x]:
                            cited_publications.iloc[ind]= (x, y)
                            ind += 1
                    cited_publications = cited_publications.rename(columns={"id":"doi","id_references":"CitedPublications"})
                    
                    # Venues ID
                    findex = 0
                    for x in reldata["venues_id"]:
                        findex += 1
                    df_pub_venues = pd.DataFrame(columns = ["doi","venues_id"], index= range(findex))
                    ind =0
                    for x in reldata["venues_id"]:
                        venues_id=(" , ").join(reldata["venues_id"][x])
                        df_pub_venues.iloc[ind] = (x, venues_id)
                        ind +=1
                    df_venues_id = df_pub_venues.rename(columns={"id":"doi","venues_id":"venueID"})       
                            
                    # Publisher
                    df_publisher = pd.DataFrame(columns =["id", "name"], index=range(len(list(reldata["publishers"]))))
                    ind = 0
                    for x in reldata["publishers"]:
                        df_publisher.iloc[ind] = (reldata["publishers"][x]["id"], reldata["publishers"][x]["name"])
                        ind+=1
                    df_publisher = df_publisher.rename(columns={"id":"organizationID"})  
                    publisher_internal_id = []
                    for idx, row in df_publisher.iterrows():
                        publisher_internal_id.append("publisher-" + str(idx))
                    df_publisher.insert(0, "internalID", Series(publisher_internal_id, dtype="string"))

                        
                    # All authors id
                    all_authors = dict()
                    for x in reldata["authors"]:
                        all_authors[x] = list()
                        for y in reldata["authors"][x]:
                            all_authors[x].append(y["orcid"])

                    findex = 0
                    for x in all_authors:
                        findex += 1
                    df_all_authors_id = pd.DataFrame(columns = ["doi","all_authors_id"], index= range(findex))
                    ind =0
                    for x in all_authors:
                        authors=(" , ".join(all_authors[x]))
                        df_all_authors_id.iloc[ind] = (x,authors)
                        ind +=1 
                    
                    # All the references 
                    findex=0
                    for x in reldata["references"]:
                        findex+=1
                    df_all_references = pd.DataFrame(columns=["id", "id_all_references"], index=range(findex))
                    ind = 0
                    for x in reldata["references"]:
                        references=(" , ").join(reldata["references"][x])
                        df_all_references.iloc[ind]= (x, references)
                        ind += 1
                    with connect(self.dbPath) as con:
                        author_id.to_sql("AuthorID", con, if_exists="replace", index=False)
                        person.to_sql("Person", con, if_exists="replace", index=False)
                        cited_publications.to_sql("CitedPublication", con, if_exists="replace", index=False)
                        df_venues_id.to_sql("VenuesId", con, if_exists="replace", index=False)
                        df_publisher.to_sql("Organization", con, if_exists="replace", index=False)
                        df_all_authors_id.to_sql("AllAuthorsID", con, if_exists="replace", index=False) 
                        df_all_references.to_sql("AllReferences", con, if_exists="replace", index=False) 
                        con.commit
                return True 
            else:
                return False
        else:
            False

class RelationalQueryProcessor (RelationalProcessor):
    def __init__(self, dbpath):
        super().__init__(dbpath)
        from sqlite3 import connect
        from pandas import read_sql
    def getPublicationsPublishedInYear(self, year):
        with connect(self.dbPath) as con:
            query = """SELECT Publication.internalID, Publication.doi, Publication.title, Publication.publicationYear, VenuesId.venueID, AllAuthorsID.all_authors_id, AllReferences.id_all_references
            FROM Publication 
            LEFT JOIN AllAuthorsID ON AllAuthorsID.doi==Publication.doi
            LEFT JOIN Person ON AllAuthorsID.all_authors_id==Person.authorID
            LEFT JOIN Venue ON Venue.doi==Publication.doi
            LEFT JOIN VenuesId ON VenuesId.doi==Publication.doi
            LEFT JOIN AllReferences ON Publication.doi == AllReferences.id
            WHERE Publication.publicationYear = '{0}';""".format(year)
            publication_year_df = read_sql(query, con)
        return publication_year_df
    def getPublicationsByAuthorId(self,authorID):
        with connect(self.dbPath) as con:
            query =  """SELECT Publication.internalID, Publication.doi, Publication.title, Publication.publicationYear, VenuesId.venueID, AllAuthorsID.all_authors_id, AllReferences.id_all_references
            FROM Publication 
            LEFT JOIN AllAuthorsID ON AllAuthorsID.doi==Publication.doi
            LEFT JOIN Person ON AllAuthorsID.all_authors_id==Person.authorID
            LEFT JOIN Venue ON Venue.doi==Publication.doi
            LEFT JOIN VenuesId ON VenuesId.doi==Publication.doi
            LEFT JOIN AllReferences ON Publication.doi==AllReferences.id
            WHERE AllAuthorsID.all_authors_id = '{0}'""".format(authorID)
            authorID_df=read_sql(query, con)
        return authorID_df
    def getMostCitedPublication(self):
        with connect(self.dbPath) as con:
            query =  """SELECT Publication.internalID, AllAuthorsID.all_authors_id, Publication.title, AllAuthorsID.doi, Venue.title, Venue.publisher, Publication.publicationYear, AllReferences.id_all_references,
            COUNT 
                (AllReferences.id_all_references) as Cited
            FROM 
                AllReferences
            LEFT JOIN Publication ON Publication.doi == AllReferences.id
            LEFT JOIN AllAuthorsID ON Publication.doi == AllAuthorsID.doi
            LEFT JOIN Venue ON Venue.doi == Publication.doi
            GROUP BY 
                AllReferences.id_all_references
            ORDER BY
                Cited DESC
            LIMIT 1;"""
            mostcitedp_df=read_sql(query, con)
        return mostcitedp_df
    def getMostCitedVenue(self):
        with connect(self.dbPath) as con:
            query = """SELECT Venue.internalID, VenuesId.venueID, Venue.title, Venue.publisher, AllReferences.id_all_references,
            COUNT 
                (AllReferences.id_all_references) as Cited
            FROM 
                AllReferences
            LEFT JOIN Publication ON Publication.doi == AllReferences.id
            LEFT JOIN AllAuthorsID ON Publication.doi == AllAuthorsID.doi
            LEFT JOIN Venue ON Venue.doi == Publication.doi
            LEFT JOIN VenuesId ON Venue.doi == VenuesId.doi
            GROUP BY 
                AllReferences.id_all_references
            ORDER BY
                Cited DESC
            LIMIT 1;"""
            mostcitedv_df=read_sql(query, con)
        return mostcitedv_df
    def getVenuesByPublisherId(self,publisherid):
        with connect(self.dbPath) as con:
            query = """SELECT Venue.internalID, VenuesId.venueID, Venue.title, Venue.publisher, Organization.name
            FROM Venue
            LEFT JOIN VenuesId ON Venue.doi == VenuesId.doi
            LEFT JOIN Organization On Organization.organizationID == Venue.publisher
            WHERE Venue.publisher ='{0}';""".format(publisherid)
            venuebypub_df=read_sql(query, con)
        return venuebypub_df
    def getPublicationInVenue(self,venueid):
        with connect(self.dbPath) as con:
            query ="""SELECT Publication.internalID, Publication.doi, Publication.title, Publication.publicationYear, VenuesId.venueID, AllAuthorsID.all_authors_id, AllReferences.id_all_references
            FROM Publication
            LEFT JOIN AllAuthorsID ON AllAuthorsID.doi==Publication.doi
            LEFT JOIN Person ON AllAuthorsID.all_authors_id==Person.authorID
            LEFT JOIN Venue ON Venue.doi==Publication.doi
            LEFT JOIN AllReferences ON Publication.doi==AllReferences.id
            LEFT JOIN VenuesId ON Venue.doi == VenuesId.doi
            WHERE VenuesId.venueID='{0}';""".format(venueid)
            pubinvenue_df=read_sql(query,con)
        return pubinvenue_df
    def getJournalArticlesInIssue(self,issue, volume,journal_id):
        with connect(self.dbPath) as con:
            query="""SELECT Publication.internalID, JournalArticle.doi, JournalArticle.issue, JournalArticle.volume, Publication.title, Publication.publicationYear, VenuesId.venueID, AllAuthorsID.all_authors_id, AllReferences.id_all_references
            FROM Publication
            LEFT JOIN AllAuthorsID ON AllAuthorsID.doi==Publication.doi
            LEFT JOIN Person ON AllAuthorsID.all_authors_id==Person.authorID
            LEFT JOIN Venue ON Venue.doi==Publication.doi
            LEFT JOIN VenuesId ON Venue.doi == VenuesId.doi
            LEFT JOIN JournalArticle ON JournalArticle.doi == Publication.doi
            LEFT JOIN AllReferences ON AllReferences.id==Publication.doi
            WHERE  JournalArticle.issue = '{0}' AND JournalArticle.volume = '{1}' AND VenuesId.venueID = '{2}';""".format(
            issue, volume, journal_id)
            journalartinissue_df = read_sql(query,con)
        return journalartinissue_df
    def getJournalArticlesInVolume(self,volume, journal_id):
        with connect(self.dbPath) as con:
            query="""SELECT Publication.internalID, JournalArticle.doi, JournalArticle.issue, JournalArticle.volume, Publication.title, Publication.publicationYear, VenuesId.venueID, AllAuthorsID.all_authors_id, AllReferences.id_all_references
            FROM Publication
            LEFT JOIN AllAuthorsID ON AllAuthorsID.doi==Publication.doi
            LEFT JOIN Person ON AllAuthorsID.all_authors_id==Person.authorID
            LEFT JOIN Venue ON Venue.doi==Publication.doi
            LEFT JOIN VenuesId ON Venue.doi == VenuesId.doi
            LEFT JOIN JournalArticle ON JournalArticle.doi == Publication.doi
            LEFT JOIN AllReferences ON AllReferences.id==Publication.doi
            WHERE JournalArticle.volume = '{0}' AND VenuesId.venueID = '{1}';""".format(volume, journal_id)
            journartinvolume_df=read_sql(query,con)
        return journartinvolume_df
    def getJournalArticlesInJournal(self,journal_id):
        with connect(self.dbPath) as con:
            query="""SELECT Publication.internalID, JournalArticle.doi, JournalArticle.issue, JournalArticle.volume, Publication.title, Publication.publicationYear, VenuesId.venueID, AllAuthorsID.all_authors_id, AllReferences.id_all_references
            FROM Publication
            LEFT JOIN AllAuthorsID ON AllAuthorsID.doi==Publication.doi
            LEFT JOIN Person ON AllAuthorsID.all_authors_id==Person.authorID
            LEFT JOIN Venue ON Venue.doi==Publication.doi
            LEFT JOIN VenuesId ON Venue.doi == VenuesId.doi
            LEFT JOIN JournalArticle ON JournalArticle.doi == Publication.doi
            LEFT JOIN AllReferences ON AllReferences.id==Publication.doi
            WHERE  VenuesId.venueID = '{0}';""".format(journal_id)
            journartinjour_df=read_sql(query, con)
        return journartinjour_df
    def getProceedingsByEvent(self,event):
        with connect(self.dbPath) as con:
            query="""SELECT Venue.internalID, VenuesId.venueID, Proceedings.title, Proceedings.publisher
            FROM Proceedings
            LEFT JOIN VenuesId ON VenuesId.doi == Proceedings.doi
            LEFT JOIN Venue ON VenuesId.doi==Venue.doi
            WHERE Proceedings.event COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%' """.format(event)
            proceedingsbyevent_df=read_sql(query, con)
        return proceedingsbyevent_df
    def getPublicationAuthors(self,pub_id):
        with connect(self.dbPath) as con:
            query ="""SELECT Person.internalID, Person.authorID, Person.familyName, Person.givenName
            FROM AuthorID
            LEFT JOIN Person ON Person.authorID == AuthorID.authorID
            WHERE AuthorID.doi = '{0}';""".format(pub_id)
            pubauthors_df=read_sql(query,con)
        return pubauthors_df
    def getPublicationsByAuthorName(self,author):
        with connect(self.dbPath) as con:
            query="""SELECT Publication.internalID, Publication.doi, Publication.title, Publication.publicationYear, VenuesId.venueID, AllAuthorsID.all_authors_id, AllReferences.id_all_references
            FROM Publication
            LEFT JOIN AllAuthorsID ON AllAuthorsID.doi==Publication.doi
            LEFT JOIN Person ON AllAuthorsID.all_authors_id==Person.authorID
            LEFT JOIN Venue ON Venue.doi==Publication.doi
            LEFT JOIN VenuesId ON Venue.doi == VenuesId.doi
            LEFT JOIN JournalArticle ON JournalArticle.doi = Publication.doi
            LEFT JOIN AllReferences ON Publication.doi=AllReferences.id
            WHERE Person.familyName COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%' OR Person.givenName COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%' """.format(author) 
            pubbyauthors_df=read_sql(query,con)
        return pubbyauthors_df
    def getDistinctPublisherOfPublications(self, plist):
        with connect(self.dbPath) as con:
            pubId=DataFrame()
            for el in plist:
                query="""SELECT Organization.internalId, Organization.organizationID, Organization.name
                FROM Organization
                LEFT JOIN Venue ON Organization.organizationID == Venue.publisher
                WHERE Venue.doi = '{0}';""".format(el)
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
        SELECT ?internalID ?doi ?type ?title ?chapterNumber ?issue ?volume ?venueID ?all_authors_id ?id_all_references
          WHERE {
            VALUES ?type {
                schema:ScholarlyArticle schema:Chapter schema:Article}
            ?internalID rdf:type ?type .
            ?internalID schema:identifier ?doi.
            ?internalID schema:name ?title.
            ?internalID schema:creator ?all_authors_id.
            ?internalID schema:datePublished""", str("'"+year+"'"), ".", 
            """?internalID schema:isPartOf ?publicationVenue.
            ?publicationVenue schema:identifier ?venueID.
                                
           
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
            ?internalID schema:isPartOf ?publicationVenue.
            ?publicationVenue schema:identifier ?venueID.
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
    SELECT ?n_citations ?internalID ?doi ?title ?type ?publicationYear ?chapterNumber ?issue ?volume ?venueID ?all_authors_id ?id_all_references
    WHERE{
        VALUES ?type {
        schema:ScholarlyArticle schema:Chapter schema:Article}
        ?internalID rdf:type ?type. 
        ?internalID cito:isCitedBy ?n_citations.
        ?internalID schema:identifier ?doi.
        ?internalID schema:name ?title.
        ?internalID schema:datePublished ?publicationYear.
        ?internalID schema:author ?author.
        ?internalID schema:creator ?all_authors_id.
        ?internalID schema:isPartOf ?publicationVenue.
        OPTIONAL {?internalID schema:Number ?chapterNumber .}
        OPTIONAL {?internalID schema:issueNumber ?issue .}
        OPTIONAL {?internalID schema:volumeNumber ?volume .}
        OPTIONAL {?internalID schema:citation ?cites.
        ?internalID schema:relatedLink ?id_all_references.}
        
    }
    ORDER BY DESC(?n_citations)
    LIMIT 1"""

        df_most_cited_publication = get(TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), query_3, True)
        df_first_most_cited = df_most_cited_publication[df_most_cited_publication["internalID"] == df_most_cited_publication["internalID"][0]]
        #transforming the columns of type float64 in strings 
        df_first_most_cited["chapterNumber"] = df_first_most_cited["chapterNumber"].astype("string")
        df_first_most_cited["volume"] = df_first_most_cited["volume"].astype("string")
        #removing the NaN
        df_first_most_cited = df_first_most_cited.fillna("")
        #remove .0
        df_first_most_cited["chapterNumber"] = df_first_most_cited["chapterNumber"].apply(remove_dotzero)
        df_first_most_cited["volume"] = df_first_most_cited["volume"].apply(remove_dotzero)
        return df_first_most_cited
    
    
    def getMostCitedVenue(self):
        
        query_2="""
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
         SELECT (SAMPLE(?cit_venue) AS ?internalID) (SAMPLE (?Id) AS ?venueID)(COUNT(?Id) as ?N_citations) (SAMPLE(?Type) AS ?type) (SAMPLE(?Title) AS ?title) (SAMPLE(?organizationID) AS ?organizationID)
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
        ORDER BY DESC(?N_citations)
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
                SELECT ?internalID ?venueID ?type ?title ?event 
                WHERE{
                 VALUES ?type {
                 schema:Book schema:Periodical schema:Event}
                 ?internalID rdf:type ?type .
                 ?internalID schema:identifier ?venueID. 
                 ?internalID schema:publisher ?publisher.
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
        SELECT ?internalID ?title ?doi ?publicationYear ?all_authors_id ?venueID ?id_all_references
        WHERE {
        
            ?internalID rdf:type schema:ScholarlyArticle.
            ?internalID schema:name ?title.
            ?internalID schema:identifier ?doi.
            ?internalID schema:isPartOf ?publicationVenue.
            ?publicationVenue schema:identifier ?venueID.
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
        SELECT ?internalID ?title ?doi ?publicationYear ?issue ?all_authors_id ?venueID ?id_all_references
        WHERE {
        
            ?internalID rdf:type schema:ScholarlyArticle.
            ?internalID schema:name ?title.
            ?internalID schema:identifier ?doi.
            ?internalID schema:isPartOf ?publicationVenue.
            ?publicationVenue schema:identifier ?venueID.
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
        #transforming the values in the column Event (float64) in strings
        df_result["issue"] = df_result["issue"].astype("string")
        #removing the NaN
        df_result= df_result.fillna("")
        #removing the .0
        df_result["issue"] = df_result["issue"].apply(remove_dotzero)
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
        SELECT ?authorID ?givenName ?familyName 
        WHERE{ ?internalID schema:identifier""", str("'"+inputPubId+"'"), ";", """  schema:author   ?author.
            ?author schema:identifier ?authorID;
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
SELECT ?internalID ?title ?doi ?authorID ?publicationYear ?venueID ?type ?issue ?volume ?chapterNumber ?id_all_references
    WHERE{ 
    ?internalID schema:author ?author;
                schema:name ?title;
                schema:datePublished ?publicationYear;
                schema:isPartOf ?publicationVenue;
                schema:identifier ?doi;
                rdf:type ?type.
    ?publicationVenue schema:identifier ?venueID.
                            
      
    ?author  schema:name ?fullname.
    ?author  schema:identifier ?authorID.
    
             
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
            query=["""
        
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT ?organizationID ?name  
        WHERE {
        ?internalId schema:identifier """, str("'"+inputdoi+"'"),";","""
                    schema:publisher  ?publisher.
        ?publisher  schema:name      ?name;
                    schema:identifier ?organizationID.
        }
        
        """]

            df_final = get (TriplestoreProcessor.getEndpointUrl(TriplestoreProcessor), query,  True)
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
    def getPublicationsPublishedInYear(self, inputyear):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor: 
            q=processor.getPublicationsPublishedInYear(inputyear)
            result = concat([result, q],ignore_index=True)
            result.drop_duplicates(subset=["id"], keep="first")
            
        for idx, row in result.iterrows():
            x=Publication(identifiers=row["doi"],title=row["title"],publicationYear=row["publicationYear"],publicationVenue=row["venueID"], author = row["all_authors_id"],
                         citedpublication = row["id_all_references"])  

            self.finalresultlist.append(x)
            return result

        for element in self.finalresultlist:
            print(element)
    def getPublicationsByAuthorId(self, inputauthorid):
        
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor: 
            q=processor.getPublicationsByAuthorId(inputauthorid)
            result = concat([result, q],ignore_index=True)
            result.drop_duplicates(keep="first", inplace=True)
        for row_idx,row in result.iterrows():
            x=Publication(identifiers=row["doi"],title=row["title"],publicationYear=row["publicationYear"],publicationVenue=row["venueID"], author = row["all_authors_id"],
                        citedpublication = row["id_all_references"])  

            self.finalresultlist.append(x)
            return result
        for element in self.finalresultlist:
                print(element)

    def getMostCitedPublication(self):
        result = DataFrame()
        for processor in self.queryProcessor: 
            q=processor.getMostCitedPublication()
            result = concat([result, q],ignore_index=True)
        return result



    def getMostCitedVenue(self):
        result = DataFrame()
        for processor in self.queryProcessor: 
            q=processor.getMostCitedPublication(self)
            result = concat([result, q],ignore_index=True)
        for row_idx,row in result.iterrows():
            x=Venue(identifiers=row["venueID"],title=row["title"], publisher=["publisher"])  


            return result


    def getVenuesByPublisherId(self, inputpub):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor: 
            q=processor.getVenuesByPublisherId(inputpub)
            result = concat([result, q],ignore_index=True)
        for row_idx,row in result.iterrows():
            x=Venue(identifiers=row["venueID"],title=row["title"], publisher=["publisher"])  


            self.finalresultlist.append(x)
            return result
        for element in self.finalresultlist:
            print(element)


    
    def getPublicationInVenue(self, inputvenue):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor: 
            q=processor.getPublicationInVenue(inputvenue)
            result = concat([result, q],ignore_index=True)
        for row_idx,row in result.iterrows():
            x=Publication(identifiers=row["doi"],title=row["title"],publicationYear=row["publicationYear"],publicationVenue=inputvenue, author = row["all_authors_id"],
                         citedpublication = row["id_all_references"])  

            self.finalresultlist.append(x)
            return result
        for element in self.finalresultlist:
            print(element)

    def getJournalArticlesInJournal(self, inputvenueid):
        result = DataFrame()
        self.finalresultlist=[]
        for processor in self.queryProcessor: 
            q=processor.getJournalArticlesInJournal(inputvenueid)
            result = concat([result, q],ignore_index=True)
            
        for row_idx,row in result.iterrows():
            x=Publication(identifiers=row["doi"],title=row["title"],publicationYear=row["publicationYear"],publicationVenue=row["venueID"], author = row["all_authors_id"],
                         citedpublication = row["id_all_references"])  

            self.finalresultlist.append(x)
            return result


# first create the relational
# database using the related source data
rel_path = "relationalDataba.db"
rel_dp = RelationalDataProcessor(rel_path)
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("/Users/martinapensalfini/Documents/data science/relational_publications.csv")
rel_dp.uploadData("/Users/martinapensalfini/Documents/data science/relational_other_data.json")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
# grp_dp.uploadData("graph_other_data.json")
# In the next passage, create the query processors for both
# the databases, using the related classes
rel_qp = RelationalQueryProcessor(rel_path)
rel_qp.setDbPath(rel_path)
grp_qp = TriplestoreQueryProcessor()
TriplestoreProcessor.setEndpointUrl (TriplestoreProcessor, "http://192.168.1.10:9999/blazegraph/sparql")
grp_endpoint ="http://192.168.1.98:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
grp_dp.uploadData("/Users/martinapensalfini/Documents/data science/graph_publications.csv")
grp_dp.uploadData("/Users/martinapensalfini/Documents/data science/graph_other_data.json")

# Finally, create a generic query processor for asking
# about data
generic = GenericQueryProcessor(queryProcessor=[])
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)


