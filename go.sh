rm *.class
rm *.jar
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class

hdfs dfs -rm -r output
yarn jar wc.jar WordCount input output
hdfs dfs -cat output/*
