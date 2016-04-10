def getNewstEnd(x):
	baseDate = '1800-01-01'
	incon = 0
	idMuni = 0
	CEPId = 0
	for i in x[1]:
		CEP = i[3]
		while(len(CEP) < 8): 
			CEP = CEP + '0'
		if i[1] == '':
			incon = 1
			idMuni = i[2]
			CEPId = CEP
		elif i[1] > baseDate:
			baseDate = i[1]
			idMuni = i[2]
			CEPId = CEP
		else:
			incon = 1
	return (x[0], (idMuni, CEPId, CEPId[:4], incon if idMuni != '' else 1))	



fEnd = sc.textFile("/home/ubuntu/Baldim/TESTE_ENDERECOS")


#('1111621225', (('131370896', '2011-07-18', '330490', '24461840'),))
rddEndByPF = fEnd.map(lambda line: line.split('|')).map(lambda x: (str(x[1]).strip(), ((str(x[0]).strip(), str(x[4]).strip(), str(x[2]).strip(), str(x[3]).strip()),))).reduceByKey(lambda a, b: a + b)

#(ID_PF, (ID_MUNICIPIO, CEP, CEP[:4], 0))
rddPFbyRecentEnd = rddEndByPF.map(getNewstEnd).filter(lambda x: x[1][3] != 1)


rddPFbyRecentEnd.saveAsTextFile("/home/ubuntu/Baldim/Enderecos/")

def dicionario(x):
	return	{
		'id_pf': x[0],
		'values':	{
			'id_municipio': x[1][0],
			'CEP': x[1][1],
			'Dist': x[1][2],
			'Incons': x[1][3]
		}
	}