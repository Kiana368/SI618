
import mrjob
import nltk
import re
from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.stem.porter import *

pattern = r'\b[A-Za-z]{4,}\b'
stemmer = PorterStemmer()

class MRMostUsedWordStems(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol

    def mapper_get_word_stems(self, _, line):
        try:
            if line != '' and line is not None:
                origin_word_list = re.findall(pattern, line)
                for origin_word in origin_word_list:
                    stem_word = stemmer.stem(origin_word)
                    yield stem_word, 1
        except:
            pass
       
    def combiner_count_word_stems(self, word_stem, counts):
        # sums up the count for each stem
        yield word_stem, sum(counts)

    def reducer_count_word_stems(self, word_stem, counts):
        # gets the tuple of (stem_length, (word_stem, counts))
        yield  len(word_stem), (word_stem, sum(counts))
        
    def reducer_find_max_word_stem(self, length, word_stem_count_pairs):
        # gets most frequent word stem for each word stem length
        max_word_stem_count_pairs = max(word_stem_count_pairs, key = lambda pair: list(pair)[1])
        yield  str(length) + '\t' + list(max_word_stem_count_pairs)[0], str(list(max_word_stem_count_pairs)[1])
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_word_stems,
                   combiner = self.combiner_count_word_stems,
                   reducer = self.reducer_count_word_stems),
            MRStep(reducer = self.reducer_find_max_word_stem)
        ]
        

if __name__ == "__main__":
    MRMostUsedWordStems.run()
