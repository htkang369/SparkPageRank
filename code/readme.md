############

In this project, I implemented spark code, GraphX code to extract top pages in wikipedia. And found top 100 universities.

### Dataset

freebase-wex-2009-01-12-articles.tsv

You can get the data by accessing aws

### Platform

Google Cloud Engine and AWS

### Code

## Description

In each file, you can find .scala file with .sbt file. The former one is original code and the latter build script.

# spark

This code could get top pages using pure spark by plugging PageRank algorithm. 

# graphx

This code could get top pages using graphx by plugging PageRank algorithm. 

# universities

This code could get top universities using pure spark by plugging PageRank algorithm. 

## Using Code

create file path like /spark/src/main/scala
Then, put .scala into /spark/src/main/scala file and put relavent .sbt file into /spark. After that, exectude command /sbt package to compile the scala code into jar code. If successful, you can submit your code on spark.