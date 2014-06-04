Phrase Finding基本实现以及分布式实现
===========================

This is a colleciton of Java files using the Hadoop framework to build a 
distributed phrase finding and simple analysis framework. It operates by
examining a unigram corpus and comparing it to a bigram corpus, taking into
account what dates constitute the background. It is split into three main jobs:
the first constructs foreground and background counts for each term, the second
joins the unigram and bigram counts together, and the third computes the
"phraseness" and "informativeness" statistics that are used to identify the
most meaningful phrases.


To run the job:

    hadoop jar PhraseFinding.jar -D unigram=/path/to/unigram/file -D bigram=/path/to/bigram/file -D output=/output/dir [-D reducers=10]
    