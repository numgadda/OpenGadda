from methods import RelationalProcessor
from methods import TriplestoreProcessor
from methods import RelationalDataProcessor, RelationalQueryProcessor
from methods import TriplestoreDataProcessor, TriplestoreQueryProcessor
from methods import GenericQueryProcessor
# first create the relational
# database using the related source data
rel_path = "relationalDataba.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("relational_publications.csv")
rel_dp.uploadData("relational_other_data.json")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint ="http://192.168.1.98:9999/blazegraph/sparql"
#grp_dp = TriplestoreDataProcessor()
#grp_dp.setEndpointUrl(grp_endpoint)
#grp_dp.uploadData("graph_publications.csv")
#grp_dp.uploadData("graph_other_data.json")
# In the next passage, create the query processors for both
# the databases, using the related classes
rel_qp = RelationalQueryProcessor(rel_path)
rel_qp.setDbPath(rel_path)
grp_qp = TriplestoreQueryProcessor()
TriplestoreProcessor.setEndpointUrl (TriplestoreProcessor, "http://192.168.1.98:9999/blazegraph/sparql")

# Finally, create a generic query processor for asking
# about data
generic = GenericQueryProcessor(queryProcessor=[])
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)


#generic.getPublicationsByAuthorId("0000-0001-5505-3327")
#generic.getJournalArticlesInJournal( "issn:1588-2861")
generic.getJournalArticlesInIssue("2","115","issn:0138-9130")
generic.getDistinctPublisherOfPublications(['doi:10.1007/s11192-018-2705-y', 'doi:10.1080/2157930x.2018.1439293'])