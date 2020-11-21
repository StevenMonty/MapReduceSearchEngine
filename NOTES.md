* For search and topN, you can construct the inverted indices once and reuse them if user does topN first then search term
    * Seperate map tasks? I think?
    
## TODOs
*   !!!!!!!!!! SET REPO TO PUBLIC !!!!!!!!!!!!
* Add prod.env file with the proper credentials that he would need to test my app
* Add JavaDocs to all of my classes/methods
* Configure the entire project as a maven proj with modules for frontend, backend.TopN, and backend.InvertedIndex to
    create all 3 artifacts in one build step
* Need to be able to read credentials from outside the jar file
* Make stop words an external file shared by all classes
    * GUI: if user queries a stop word, show alert
    

* Make sure I can create the GUI jar from the command line.     
* Manually create cluster and connect app to cluster on init
* --target to use specific java version

WORKFLOW:
1. Move selected files into new input directory
2. Construct InvertedIndices from the new input dir
3. Move results into new output dir
3. Show Menu
4. 


    
Office Hour Questions:
    * Do you run our app on the cluster? Do we provide our json auth file and or leave our cluster running
        * Add to readme which env vars need to be changed for his cluster to work
    * Can I use maven to build my GUI jar? Can I assume you have maven or include directions to install?
        * submit GUI jar file, but specify that you need maven version Im using to build
        * And jars for backend modules
    * Docker to local cluster communication in proj req?
        * That was for Pitt cluster, not local docker cluster submit IT ticket
    * Do we use stop words in both TopN and Inverted Index?
        * Up to me, usually used for both
    * How to we execute SearchTerm? is it from inverted index jar or separate jar?
        * Search term is diff jar, but we can just load the results of inverted index and do the searchterm locally inside
            the GUI
    * Top N is supposed to run on the inverted index results, not the source files
        * Can construct inverted index in descending order and TopN just reads the top N lines
        * use secondary sorting algorithm

    * How many reducer tasks should we specify?



    * Ideally use secondary sorting with Combiner and if using Combiner we need a Partitioner
    * How do we generate the DocID?
        * Doesnt matter. 
