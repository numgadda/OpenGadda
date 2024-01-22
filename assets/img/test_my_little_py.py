


from my_little_py import RelationalDataProcessor, RelationalQueryProcessor
from my_little_py import TriplestoreDataProcessor, TriplestoreQueryProcessor
from my_little_py import GenericQueryProcessor

# first create the relational
# database using the related source data
rel_path = "publications.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("c:\\Users\\lored\\OneDrive\\Documenti\\GitHub\\2021-2022\\docs\\project\\data\\relational_publications.csv")
rel_dp.uploadData("c:\\Users\\lored\\OneDrive\\Documenti\\GitHub\\2021-2022\\docs\\project\\data\\relational_other_data.json")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint =" http://10.250.13.167:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
#grp_dp.uploadData("c:\\Users\\lored\\OneDrive\\Documenti\\GitHub\\2021-2022\\docs\\project\\data\\graph_publications.csv")
#grp_dp.uploadData("c:\\Users\\lored\\OneDrive\\Documenti\\GitHub\\2021-2022\\docs\\project\\data\\graph_other_data.json")

# In the next passage, create the query processors for both
# the databases, using the related classes
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path) 
grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl( grp_endpoint)


# Finally, create a generic query processor for asking
# about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)


#now you can execute all the methods of the GenericQueryProcessor (use print)
#print(generic.getPublicationInVenue("issn:1588-2861"))
#for x in  generic.getPublicationsPublishedInYear("2014"):
    #print(x.getIds())
    
print( generic.getDistinctPublisherOfPublications(["doi:10.1007/978-3-030-54956-5_1",
        "doi:10.1371/journal.pone.0236863",
        "doi:10.1145/3407194",
        "doi:10.3390/app10144893",
        "doi:10.1145/3309547",
        "doi:10.1007/978-3-030-58285-2_27",
        "doi:10.1007/s11280-020-00842-7"] ))
    
#print(generic.getPublicationsPublishedInYear("2016"))

#print(generic.getDistinctPublisherOfPublications(['doi:10.1007/s11192-018-2705-y', 'doi:10.1080/2157930x.2018.1439293']))
