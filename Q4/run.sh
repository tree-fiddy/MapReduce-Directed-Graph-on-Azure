hadoop jar ./target/q4-1.0.jar edu.gatech.cse6242.Q4 /user/cse6242/smaller.tsv /user/cse6242/q4output1
hadoop fs -getmerge /user/cse6242/q4output1/ q4output1.tsv
hadoop fs -rm -r /user/cse6242/q4output1
