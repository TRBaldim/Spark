from pyspark import SparkContext

import sys
import ast
reload(sys)
sys.setdefaultencoding("UTF-8")

def rola(a, b):
	try:
		return float(a) + float(b)
	except:
		try:
			float(str(a.unicode('utf8'))) + float(str(b.unicode('utf8')))
		except:
			return ast.literal_eval(a) + ast.literal_eval(b)

if __name__ == "__main__":

	sc = SparkContext(appName="ClimateChanges")

fCities = sc.textFile("hdfs://localhost:9000//tmp/gTemp/GlobalLandTemperaturesByCity.csv")

rddSplitedCities = fCities.map(lambda line: line.split(",")).map(lambda vals: (vals[0], vals[1])).reduceByKey(lambda a, b: a + float(b))
rddYears = rddSplitedCities.map(lambda line: (line[0].split("-")[0], line[3])).reduceByKey(lambda a, b: a)
rddCountYears = rddSplitedCities.map(lambda line: (line[0].split("-")[0], 1)).reduceByKey(lambda a, b: a + b)
rddSumTemp = rddSplitedCities.map(lambda line: (line[0].split("-")[0], float(str(line[1])[:4]))).reduceByKey(lambda a, b: float(a) + float(b))
rddAvgTempByYear = rddSumTemp.join(rddCountYears).map(lambda k: (k[0], k[1][0] / k[1][1]))
rddFullJoin = rddAvgTempByYear.join(rddYears).map(lambda n: (n[0], n[1][0], n[1][1]))
for i in rddFullJoin.collect()[:1000]:
	print i

	rddGroupByCity = rddSplitedCities.map(lambda val: (val[3], 1)).reduceByKey(lambda a, b: a + b)
	rddGroupByTemp = rddSplitedCities.map(lambda val: (val[3], val[1])).reduceByKey(lambda a, b: a + b)
	rddJoinedAvg = 

