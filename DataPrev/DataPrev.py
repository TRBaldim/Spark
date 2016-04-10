import time
import datetime


def CheckIn (base, new):
	try:
		if new in base:
			return base
		else:
			return base + ", " + new
	except:
		return base

def ConvertTime (x):
	try:
		return (str(x[7]).strip(), int(datetime.date.today().year - int(str(x[1]).strip().split("-")[0])))
	except:
		return ("Inconcistente", 1)

def TimeCount (x):
	try:
		return (int(str(x[1]).strip().split("-")[0]), 1)
	except:
		return ("Inconcistente", 1)

def ConvertStamp (k):
	try:
		return (k[0], float((k[1][0] / k[1][1])))
	except:
		return (k[0], "ERROR")


def calcEtnias (a, b):
	try:
		if type(a) is int:
			b[0][b[1]] = b[0][b[1]] + 1
			return b
		else:
			a[0][b[1]] = a[0][b[1]] + 1
			return a
	except:
		return "PAU"



fEnd = sc.textFile("/u01/data/ENDERECOS")
fPF = sc.textFile("/u01/data/PESSOAS_FISICAS")


#Retorna o Número de CEP agrupados
fNum = fEnd.map(lambda line: line.split("|")).map(lambda x: (x[3], 1)).reduceByKey(lambda a, b: a + b)
fNum.sortBy(lambda x: x[1]).saveAsTextFile("/u01/data/EndResults/")

#Count Num Distinct CEP
fNum.count()

#Group By Distrito
fNum = fEnd.map(lambda line: line.split("|")).map(lambda x: (str(x[3]).strip()[:4], 1)).reduceByKey(lambda a, b: a + b)
fNum.sortBy(lambda x: x[1]).saveAsTextFile("/u01/data/DistResults/")


#Retorna o Número de CEP agrupados
fNum = fEnd.map(lambda line: line.split("|")).map(lambda x: (x[13], str(x[3]))).reduceByKey(CheckIn)
fNum.sortBy(lambda x: x[1]).saveAsTextFile("/u01/data/GroupResults/")


#Retorna o Número de CEP agrupados
fNum = fEnd.map(lambda line: line.split("|")).map(lambda x: (x[9], 1)).reduceByKey(lambda a, b: a + b)
fNum.sortBy(lambda x: x[1]).saveAsTextFile("/u01/data/Bairro/")


fPF = sc.textFile("/home/ubuntu/Baldim/PESSOAS_FISICAS")

fMunicipioCount = fPF.map(lambda line: line.split("|")).map(lambda x: (str(x[7]).strip(), 1)).reduceByKey(lambda a, b: a + b)
fDateMuni = fPF.map(lambda line: line.split("|")).map(ConvertTime).reduceByKey(lambda a, b: a + b)
fJoined = fDateMuni.join(fMunicipioCount).map(ConvertStamp)
fJoined.sortBy(lambda x: x[1]).saveAsTextFile("/home/ubuntu/Baldim/Idade/")

#Count por ano de nascimentos
fCountYear = fPF.map(lambda line: line.split("|")).map(TimeCount).reduceByKey(lambda a, b: a + b)
fCountYear.sortBy(lambda x: x[1]).saveAsTextFile("/home/ubuntu/Baldim/Idade/")

listEtinias = fPF.map(lambda line: line.split("|")).map(lambda x: (str(x[3]).strip(), 1)).reduceByKey(lambda a, b: a + b).collect()
dictEtinias = dict(listEtinias)
mapEtiniaMuni = fPF.map(lambda line: line.split("|")).map(lambda x: (str(x[7]).strip(), (dictEtinias, str(x[3]).strip()))).reduceByKey(calcEtnias)
mapEtiniaMuni.sortBy(lambda x: x[1]).saveAsTextFile("/home/ubuntu/Baldim/Cor/")


listMuni = fPF.map(lambda line: line.split("|")).map(lambda x: (str(x[7]).strip(), (str(x[3]).strip, 1)).reduceByKey(lambda a, b: a + 1 if b[0] == '' else a).collect()[:10]


 id_pessoa_fisica | dt_nascimento |  dt_obito  | cs_etnia | cs_estado_civil | cs_sexo | cs_grau_instrucao | id_muni_ibge