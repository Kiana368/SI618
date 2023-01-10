import mrjob
from mrjob.job import MRJob

class UpvotesAverageCalculator(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol

    def mapper(self, _, line):
        try:
            # # +++your code here+++
            date = line.split('\t')[0] # date
            description = line.split('\t')[1] # string containing all category tags
            upvotes = float(line.split('\t')[2]) # upvotes
            count = 1
            if description != '':
                # # +++your code here+++
                tags = description.replace("'","").split(", ")  # get a list of all the tags from the description
                # hint: the mapper maps each category under a date with the upvotes and the product count
                for tag in tags:
                    yield (date, tag), (upvotes, count)
        except:
            pass
       
    def combiner(self, key, values):
        # # +++your code here+++
        total_upvotes = 0
        total_product_counts = 0
        for pair in values:
            total_upvotes += list(pair)[0]
            total_product_counts += list(pair)[1]
        
        yield key, (total_upvotes, total_product_counts)
        # hint: the combiner combines to get the total upvotes and the total product counts

    def reducer(self, key, values):
        # # +++your code here+++
        total_upvotes = 0
        total_product_counts = 0
        for pair in values:
            total_upvotes += list(pair)[0]
            total_product_counts += list(pair)[1]
        avg_upvotes = round(total_upvotes/total_product_counts, 2)
        
        yield key[0] + "\t" + key[1], str(avg_upvotes)
        # the reducer combines and reduces the total upvotes and total product counts to the average number of upvotes per product


if __name__ == '__main__':
    UpvotesAverageCalculator.run()
    
