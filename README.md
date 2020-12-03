# AmazonProductAnalysis

The java code is to be executed on hadoop cluster. It help to answer following question.

Q-1) What is the distribution of product prices overall?  Count empty price fields separately. 
Produce 10 evenly split buckets.  Please parse out any bad values (HTML).  For price ranges, use an average.

Following is the output :-

Range	    Number of records
0.0 to 6.0	    886977 </br> 
6.0 to 8.0	    573006 </br>
8.0 to 10.0	    657146 </br>
10.0 to 14.0	947843 </br>
14.0 to 18.0	879275 </br>
18.0 to 24.0	759797 </br>
24.0 to 32.0	619713 </br>
32 to 52.0	    689419 </br>
52.0 to 88.0	446613 </br>
88.0 to 1000.0	491774 </br>

Q-2) What is the distribution of review lengths (in number of words)?  Produce 10 evenly split buckets and make a histogram.  Please parse out any HTML and "stop words".

Ans :- Folowing is the output :- 

Review length range	Record count
0 to 19              16165643 </br>
20 to 40             15657846 </br>
41 to 68             15673446 </br>
69 to 105            15860108 </br>
106 to 137           15654404 </br>
138 to 185           15623077 </br>
186 to 261           15396308 </br>
262 to 400           15381992 </br>
401 to 766           15400058 </br>
767 to 35094         12991954 </br>



## Imp Note
Since the sample code is getting executed on jdoodle , ensure adding folowing library from maven :-
org.netbeans.external:com-google-gson:RELEASE113
