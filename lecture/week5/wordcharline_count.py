from mrjob.job import MRJob

class MRWordCharLineCount(MRJob):
    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "line", 1
    
    def reducer(self, key,values):
        yield key, sum(values)
        
if __name__ == '__main__':
    a = MRWordCharLineCount()
