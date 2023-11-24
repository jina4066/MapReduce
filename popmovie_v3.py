from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import re

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
            match = re.search('\((\d{4})\)', movie_title)
            if match:
                year = match.group(1)
                yield (year, (float(rating), 1))

    def reduce_rating_count(self, year, ratings):
        rating_sum = 0
        rating_count = 0
        for rating, count in ratings:
            rating_sum += rating
            rating_count += count
        yield None, (rating_sum/rating_count, year)

    def reduce_sort(self,_, year_data):
        sorted_list = sorted(list(year_data), reverse=True)
        for item in sorted_list:
            yield item[1], item[0]

    def mapper_init(self):
        self.load_movie_data()

if __name__=='__main__':
    PopMovie.run()
