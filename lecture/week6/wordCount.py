import time
import re

# Create an RDD in PySpark from large text file
start = time.time()
WORD_RE = re.compile(r"\b[\w']+\b")

input_text_file = str(sys.argv[1]) if len(sys.argv) > 1 else "/nfs/turbo/arcts-data-hadoop-stage/data/Gutenberg.txt"
rdd = sc.textFile(input_text_file)

#Get each word and count them
word_counts = rdd.flatMap(lambda line: WORD_RE.findall(line)) \
                        .map(lambda word: (word, 1)) \
                        .reduceByKey(lambda a, b: a + b)

#Sort by frequency
word_counts_sorted = word_counts.sortBy(lambda x: x[1], ascending = False)

#Write to file
word_counts_sorted.saveAsTextFile("/scratch/siads618f22_class_root/siads618f22_class/cbudak/gutenberg_count")
word_counts_sorted.map(lambda t : t[0] + '\t' + str(t[1])).saveAsTextFile('/scratch/siads618f22_class_root/siads618f22_class/cbudak/gutenberg_count2')


end = time.time()

print("Top 10 most frequent words were found with Spark in {} seconds.".format(round(end - start, 2)))
