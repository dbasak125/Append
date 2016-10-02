# Append
Process raw Twitter json documents produced by TwitterStreamAPI and TweetFetch projects in my repository


The Append class takes as input a raw twitter json document collection (like: json-1.json), and processes it to get rid of the unnecessary fluff and prep it as a fresh json array element - ready to be fed into Apache Lucene/Solr for indexing and data analysis.

*Please refer to TwitterStreamAPI and TweetFetch project repositories to see how the input json files are created.
