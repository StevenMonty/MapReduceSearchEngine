MapReduceFinalProj

TODO email prof by November 23rd that I did proj individually and if I met all reqs
TODO Site all code sample sources here:

### Grading Criteria Met

- [x] Dockerized Search Engine GUI
- [x] Docker to Cluster Communication
- [x] MapReduce Inverted Index Implementation
- [x] Search Term and Top-N Search Implementation

### Build Steps

##### SearchEngineGUI Container
Requirements:
    Docker: I have Docker Desktop for Mac Version 2.3.0.1(46911)
    Docker for Mac and Requires Socat and Xquarts to render a GUI application from within a Container
    Apache Maven 3.6.3
TODO MAKE SURE MY II JAR IS UPDATED IN MY BUCKET
TODO make sure these paths are all correct from the command line
TODO change dockerfile back to jre not jdk and test functionality
1. `cd frontent`
2. `mvn install`
3. `docker build --rm -t stevenmonty/gui .`
4. `docker run -it --rm stevenmonty/gui:latest `

TODO create pom files for these jar packing
##### InvertedIndex Jar

##### Top-N Jar


### Sources Referenced
[Most Common English Words](https://www.espressoenglish.net/the-100-most-common-words-in-english/) used to construct StopWord list
[Secondary Sorting Algorithm](https://www.oreilly.com/library/view/data-algorithms/9781491906170/ch01.html)
http://www-scf.usc.edu/~shin630/Youngmin/files/HadoopInvertedIndexV5.pdf