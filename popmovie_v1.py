from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class PopMovie(MRJob):
    def configure_args(self):
        super(PopMovie, self).configure_args()
        self.add_file_arg('--movies', help='Path to movies.csv')

    def steps(self):
        return [
                MRStep(mapper_init=self.load_movie_data,
                    mapper=self.map_rating_count,
                    reducer=self.reduce_rating_count),
                MRStep(reducer=self.reduce_sort)
                ]

    def load_movie_data(self):
        self.movie_dict = {}
        with open(self.options.movies, 'r', encoding='utf-8') as csvfile:
            movie_data = csv.reader(csvfile)
            next(movie_data)
            for row in movie_data:
                movie_id = row[0]
                movie_title = row[1]
                self.movie_dict[movie_id] = movie_title

    def map_rating_count(self,_,line):
        row = next(csv.reader([line]))
        userid, movieid, rating, timestamp = row
        if userid != 'userId':
            movie_title = self.movie_dict.get(movieid, 'Unknown')
            yield (movie_title,(float(rating),1))

    def reduce_rating_count(self, movie_title, ratings):
        rating_sum = 0
        rating_count = 0
        for rating, count in ratings:
            rating_sum += rating
            rating_count += count

        rating_avg = rating_sum / rating_count
        yield (movie_title, rating_avg)

    def reduce_sort(self, movie_title, average_ratings):
        for average_rating in average_ratings:
            yield(movie_title, str(average_rating).zfill(6))

    def mapper_init(self):
        self.load_movie_data()

if __name__=='__main__':
    PopMovie.run()
