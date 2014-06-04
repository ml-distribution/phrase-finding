HW4b: Phrase Finding on Hadoop
===========================

This is a colleciton of Java files using the Hadoop framework to build a 
distributed phrase finding and simple analysis framework. It operates by
examining a unigram corpus and comparing it to a bigram corpus, taking into
account what dates constitute the background. It is split into three main jobs:
the first constructs foreground and background counts for each term, the second
joins the unigram and bigram counts together, and the third computes the
"phraseness" and "informativeness" statistics that are used to identify the
most meaningful phrases.

To create the .jar archive:

    javac *.java
    jar cfm PhraseFinding.jar Manifest.txt *.class

*By specifying some `Manifest.txt` file, you can point Java--and hence, Hadoop--to
the class with the `main` method. Just include the following line (followed by
a newline) in the text file:*

    Main-Class: Package.ClassName

To run the job:

    hadoop jar PhraseFinding.jar -D unigram=/path/to/unigram/file -D bigram=/path/to/bigram/file -D output=/output/dir [-D reducers=10]
    