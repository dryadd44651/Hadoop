<h1>MapReduce program in Hadoop</h1>

<p>
The source code path /src/p1
/src/example has other sample
commonFriend.java is library for other program
</p>

<h2>1.Mutual/Common friend list of two friends</h2>

<p>The key idea is that if two people are friend then they have a lot of mutual/common friends. This program will find the common/mutual friend list for them.

commonFriend1.java

For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy

As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this case you may exclude them from your output). 

<b>Output</b>
<User_A>, <User_B><TAB><Mutual/Common Friend List>
</p>



<h2>2.Common Friend Number</h2>
<p>
using dataset from Q1.

Find friend pairs whose common friend number are within the top-10 in all the pairs.

topTen.java

output them in decreasing order.
Output Format:
<User_A>, <User_B><TAB><Mutual/Common Friend Number>
</p>

<h2>3.Friend's Birth day</h2>
use in-memory join 
<p>
Given any two Users (they are friend) as input, output the list of the names and the date of birth (mm/dd/yyyy) of their mutual friends.

friendBirth.java

Note: use the userdata.txt to get the extra user information.
Output format:
UserA id, UserB id, list of [names: date of birth (mm/dd/yyyy)] of their mutual Friends.

Sample Output:
1234     4312       [John:12/05/1985, Jane : 10/04/1983, Ted: 08/06/1982]

</p>

<h2>4.Maximum age of the direct friends</h2>
<p>
Use reduce-side join and job chaining
Step 1: Calculate the maximum age of the direct friends of each user.
Step 2: Sort the users based on the calculated maximum age in descending order as described in step 1.
Step 3. Output the top 10 users from step 2 with their address and the calculated maximum age.

topAge.java

Sample output.
  
User A, 1000 Anderson blvd, Dallas, TX, 60.
where User A is the id of a particular user and 60 represents the maximum age of direct friends for this particular user.

</p>

<h3>Input files </h3>

<p>1. soc-LiveJournal1Adj.txt
The input contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>

2. userdata.txt 
The userdata.txt contains dummy data which consist of 
column1 : userid
column2 : firstname
column3 : lastname
column4 : address
column5: city
column6 :state
column7 : zipcode
column8 :country
column9 :username
column10 : date of birth.

Here, <User> is a unique integer ID corresponding to a unique user and <Friends> is a comma-separated list of unique IDs corresponding to the friends of the user with the unique ID <User>. Note that the friendships are mutual (i.e., edges are undirected): if A is friend with B then B is also friend with A. The data provided is consistent with that rule as there is an explicit entry for each side of each edge. So when you make the pair, always consider (A, B) or (B, A) for user A and B but not both.
</p>
  
<p>some output data</p>
<img src="https://raw.githubusercontent.com/dryadd44651/Hadoop/master/friendBirth.JPG" alt="friendBirth" style="width:200px;">
<img src="https://raw.githubusercontent.com/dryadd44651/Hadoop/master/topAgeUser.JPG" style="width:100vw;">

