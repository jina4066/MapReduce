from mrjob.job import MRJob
from mrjob.step import MRStep

class PopMovie(MRJob):
    def steps(self):
        return [
                MRStep(mapper=self.map_rating_count,
                    reducer=self.reduce_rating_count),
                MRStep(reducer=self.reduce_sort)
                ]

    def map_rating_count(self,_,line):
        userid, movieid, rating, timestamp = line.split(',')
        if userid != 'userId':
            yield (movieid,(float(rating),1))

    def reduce_rating_count(self, movie_id, ratings):
        rating_sum = 0
        rating_count = 0
        for rating, count in ratings:
            rating_sum += rating
            rating_count += count
        rating_avg = rating_sum / rating_count
        yield (movie_id, rating_avg)

    def reduce_sort(self, movie_id, average_ratings):
        for average_rating in average_ratings:
            yield(movie_id,str(average_rating).zfill(6))

if __name__=='__main__':
    PopMovie.run()
