# hadoop-mapreduce-demo-masterdev

SSH vào server 172.17.80.27, đăng nhập vào user "hadoop" 
```
su hadoop
```
(password: 1)
## Bài 1 - Word Count
* Run mapreduce program
```
yarn jar ~/manhnk9/hadoop-demo/word-count/target/word-count-1.0-SNAPSHOT.jar WordCount manhnk9/text manhnk9/output/word_count_output
```
* Cat output
```
hdfs dfs -cat manhnk9/output/word_count_output/* | less
```
* Get file from HDFS to local
```
hdfs dfs -get manhnk9/output/word_count_output/part-r-00000 ~/manhnk9/output/word_count_result.txt
```

## Bài 2 - Count Distinct
* Run mapreduce program
```
yarn jar ~/manhnk9/hadoop-demo/word-count/target/word-count-1.0-SNAPSHOT.jar WordCount manhnk9/text manhnk9/output/word_count_output
```
* Cat output
```
hdfs dfs -cat manhnk9/output/count_distinct_output/result/* | less
```
* Get file from HDFS to local
```
hdfs dfs -get manhnk9/output/count_distinct_output/result/part-r-00000 ~/manhnk9/output/count_distinct_result.txt
```

## Bài 3 - Join Example
* Run mapreduce program
```
yarn jar ~/manhnk9/hadoop-demo/join/target/join-example-1.0-SNAPSHOT.jar Driver manhnk9/bai3/people.csv manhnk9/bai3/salary.csv manhnk9/output/join-example
```
* Cat output
```
hdfs dfs -cat manhnk9/output/join-example/* | less
```
* Get file from HDFS to local
```
hdfs dfs -get manhnk9/output/join-example/part-r-00000 ~/manhnk9/output/join.csv
```
