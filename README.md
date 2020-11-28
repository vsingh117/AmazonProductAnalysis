# AmazonProductAnalysis

The java code is to be executed on hadoop cluster. It help to answer following question.

What is the distribution of product prices overall?  Count empty price fields separately. 
Produce 10 evenly split buckets.  Please parse out any bad values (HTML).  For price ranges, use an average.

Following is the output :-

Range	Number of records
0.0 to 6.0	886977 </br> 
6.0 to 8.0	573006 </br>
8.0 to 10.0	657146 </br>
10.0 to 14.0	947843 </br>
14.0 to 18.0	879275 </br>
18.0 to 24.0	759797 </br>
24.0 to 32.0	619713 </br>
32 to 52.0	689419 </br>
52.0 to 88.0	446613 </br>
88.0 to 1000.0	491774
