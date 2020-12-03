# AmazonProductAnalysis

The java code is to be executed on hadoop cluster. It help to answer following question.

Q-1) What is the distribution of product prices overall?  Count empty price fields separately. 
Produce 10 evenly split buckets.  Please parse out any bad values (HTML).  For price ranges, use an average.

Following is the output :-

|Range	       | Number of records  |
|-------------|-----------------|
|0.0 to 6.0	   | 886977  |  
|6.0 to 8.0	   | 573006  | 
|8.0 to 10.0	|    657146  | 
|10.0 to 14.0	|947843  | 
|14.0 to 18.0	|879275  | 
|18.0 to 24.0	|759797  | 
|24.0 to 32.0	|619713  | 
|32 to 52.0	   | 689419  | 
|52.0 to 88.0	|446613  | 
|88.0 to 1000.0|	491774  | 

Q-2) What is the distribution of review lengths (in number of words)?  Produce 10 evenly split buckets and make a histogram.  Please parse out any HTML and "stop words".

Ans :- Following is the output :- 

|Review length range|	Record count  |
|-------------------|-----------------|
|0 to 19            |  16165643  | 
|20 to 40           |  15657846  | 
|41 to 68           |  15673446  | 
|69 to 105          |  15860108  | 
|106 to 137         |  15654404  | 
|138 to 185         |  15623077  | 
|186 to 261         |  15396308  | 
|262 to 400         |  15381992  | 
|401 to 766         |  15400058  | 
|767 to 35094       |  12991954  | 



## Imp Note
Since the sample code is getting executed on jdoodle , ensure adding folowing library from maven :-
org.netbeans.external:com-google-gson:RELEASE113
