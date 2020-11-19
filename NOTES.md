* Each text file is its own document, can put them all into one dir for simplicity
* For search and topN, you can construct the inverted indices once and reuse them if user does topN first then search term
    * Seperate map tasks? I think?
    
* Manually create cluster and connect app to cluster on init
* --target to use specific java version
* TODO filter punctuation using a STOP WORD list
    
Class Questions:
    * How many reducer tasks should we specify?
    * Ideally use secondary sorting with Combiner and if using Combiner we need a Partitioner
    * How do we generate the DocID?
        * Doesnt matter. 
