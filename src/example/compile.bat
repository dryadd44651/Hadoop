taskkill /IM "java.exe" /F
set fileName=WordCount
set cp=D:\hadoop\hadoop-2.7.1\etc\hadoop;D:\hadoop\hadoop-2.7.1\share\hadoop\common\lib\*;D:\hadoop\hadoop-2.7.1\share\hadoop\common\*;D:\hadoop\hadoop-2.7.1\share\hadoop\hdfs;D:\hadoop\hadoop-2.7.1\share\hadoop\hdfs\lib\*;D:\hadoop\hadoop-2.7.1\share\hadoop\hdfs\*;D:\hadoop\hadoop-2.7.1\share\hadoop\yarn\lib\*;D:\hadoop\hadoop-2.7.1\share\hadoop\yarn\*;D:\hadoop\hadoop-2.7.1\share\hadoop\mapreduce\lib\*;D:\hadoop\hadoop-2.7.1\share\hadoop\mapreduce\*


javac %fileName%.java -cp %cp%
jar cfm %fileName%.jar %fileName%.MF *.class
del *.class
timeout 5
exit
