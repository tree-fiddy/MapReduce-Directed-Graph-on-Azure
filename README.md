# MapReduce Directed Graph on Azure

## Dependencies
This program runs on Cloudera's CDH 5.18 distribution.  It also requires a few packages (which I haven't looked into... I just received them and went along with it).
## Objective
Using a TSV with 2 columns (Source and Target), calculating out-degree and in-degree of nodes.

## Instructions
Fork or clone, and `cd ` into Q4.  Then, run the following commands:
>  mvn package  
>  bash run.sh

That's it.

## Details
Calculates the count of out-degree - in-degree differences.  
For example, if the degree difference is -1, and this happens 10 times, then -1 will be associated with the count of 10.
