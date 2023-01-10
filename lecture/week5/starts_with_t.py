
import mrjob
from mrjob.job import MRJob

class StartsWithT(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol
    
    def mapper (self, _, line):
        if(line.startswith("T")):
            yield (None, line)
            
if __name__ == '__main__':
    StartsWithT.run()
