from pyspark import SparkContext

def sumFloat(a, b):
	try:
		return float(a) + float(b)
	except:
		return float(a)

def divFloat(k):
	try:
		return (k[0], (float(k[1][0]) / float(k[1][1])))
	except:
		return k

if __name__ == "__main__":

	sc = SparkContext(appName="IMDB_Ratings")

	fMovies = sc.textFile("hdfs://localhost:9000/tmp/labdata/sparkdata/movies.csv")
	fRates = sc.textFile("hdfs://localhost:9000/tmp/labdata/sparkdata/ratings.csv")

	rddMoviesIDs = fMovies.map(lambda lines: lines.split(";")).map(lambda vals: (vals[0], vals[1]))

	rddRatesIDs = fRates.map(lambda lines: lines.split(",")).map(lambda vals: (vals[1], vals[2])).reduceByKey(sumFloat)

	rddRatesCounts = fRates.map(lambda lines: lines.split(",")).map(lambda vals: (vals[1], 1)).reduceByKey(lambda a, b: a + b)

	rddRateAvarage = rddRatesIDs.join(rddRatesCounts).map(divFloat)

	rddAvarageMovies = rddMoviesIDs.join(rddRateAvarage).map(lambda k: (k[1][0], k[1][1]))

	rddAvarageMovies.cache()

	rddAvarageMovies.sortBy(lambda x: x[1]).saveAsTextFile("/home/ubuntu/new/")

	sc.stop()