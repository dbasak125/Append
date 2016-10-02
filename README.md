# Append
Process raw Twitter json documents produced by TwitterStreamAPI and TweetFetch projects in my repository


The Append class (main()) takes as input a raw twitter json document collection (like: json-1.json), and processes it to get rid of the unnecessary fluff and prep it as a clean json array element - ready to be fed into Apache Lucene/Solr for indexing and data analysis.

class KeyMatch is used to initialize various 'topics' and their keywords - which is then used to classify every single tweet as one of the five topics.

Regular Expressions are used to fish out emojis and url contents from the tweet_text.
Emojis and special charaters are represented by unicode surrogate pairs, and the regex includes all such unicode pairs to match emojis in a tweet text field (ref: http://instagram-engineering.tumblr.com/post/118304328152/emojineering-part-2-implementing-hashtag-emoji).

text_xx (where xx is the ISO-639-1 language code) is used to store the language specific tweet text. This is used as the indexed field in Apache Solr.

Example of input file: json-1.json
Example of output file: condense-3-1.json

example execution parameters (detailed args[] definitions are given in source code) -
bash:
$ bin/Append condense-3-1.json start json-1.json

*Please refer to TwitterStreamAPI and TweetFetch project repositories to see how the input json files are created.
