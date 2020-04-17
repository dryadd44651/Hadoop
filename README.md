<h1>MapReduce program in Hadoop</h1>

<h2>Mutual/Common friend list of two friends</h2>

<p>The key idea is that if two people are friend then they have a lot of mutual/common friends. This program will find the common/mutual friend list for them.

For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy

As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this case you may exclude them from your output). 

Input:
Input files 

1. soc-LiveJournal1Adj.txt
The input contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>
</p>
