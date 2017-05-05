#O exemplo a seguir foi executado em uma VM Cloudera, carregando arquivos .txt de livros extraidos de paginas html no Hadoop Distributed File System (HDFS). Usando linguagem python no Apache Spark.
	
##Comandos para carregar arquivos no hdfs 

#hdfs dfs -copyFromLocal /home/cloudera/input 

#hdfs dfs -ls /user/cloudera/input	
	
#executanto o shel interativo do pyspark 

#pyspark

# 1 - Contar todas as cocorrências  de palavras (removendo as preposições e coisas assim)

text_file = sc.textFile("hdfs://home/cloudera/input/index.txt")

texto = re.sub(r"<.+?>", "", text_file)

counts = texto.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("hdfs://home/cloudera/input/result.txt")

# 2 - Contar palavras por livro 

text_file = sc.textFile("hdfs://home/cloudera/input/index.txt")

counts = texto.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("hdfs://home/cloudera/input/result.txt")
