#Version 0.0.0

def getEvents (a, b):
	try:
		return a.append(b[0])
	except:
		return a

def filterByEvent (line):
	try:
		if str(line[2]).strip() != '':
			return (str(line[1]).strip(), str(line[2]).strip() + ";" +str(line[0]).strip())
		elif str(line[3]).strip() != '':
			return (str(line[1]).strip(), str(line[3]).strip() + ";" +str(line[0]).strip())
		else:
			return (str(line[1]).strip(), "Inc." + ";" + str(line[0]).strip())
	except:
		return (str(line[1]).strip(), "ERRO")

def filterDates(x):
	splitedInfo = x[1].split("|")
	incVal = 0
	dateBase = '1800-01-01'
	newestId = 'Inc.'
	for i in splitedInfo:
		if 'Inc.' in i:
			incVal = 1
			newestId = i.split(";")[1]
			dateBase = 0
		else:
			DateInfo = i.split(";")
			if DateInfo[0] > dateBase:
				newestId = DateInfo[1]
				dateBase = DateInfo[0]
				incVal = 0
			else:
				newestId = 'Inc.'
				incVal = 1
				dateBase = 0
	return (newestId, (x[0], dateBase, incVal))


def filterDivorceDates(x):
	dateBase = '2999-01-01'
	result = ''
	for i in x[1][1]:
		if i == '' and result == '':
			result = 'NC'
		else:
			if i < dateBase and i != '' and i != 'NC':
				result = i
				dateBase = result
	return (x[0], result)


def getNotInc (x):
	if x[1][0] == 'Inc.':
		return 'NONE'
	else:
		return (x[1][0], 1)

def finalFiltering (x):
	#ID_PF, Data_Alteracao, Data_Divorcio, Inconcistencia
	return (x[1][1][0], (x[1][1][1], x[1][0], x[1][1][2]))

def getNewstEnd(x):
	lEnderecos = x[1].split('|')
	baseDate = '1800-01-01'
	incon = 0
	for i in lEnderecos:
		endElement = i.split(';')
		while(len(endElement[3]) < 8): 
			endElement[3] = endElement[3] + '0'
		if endElement == '':
			incon = 1
			idEnd = endElement[0]
			idMuni = endElement[2]
			CEP = endElement[3]
		elif endElement[1] > baseDate:
			baseDate = endElement[1]
			idMuni = endElement[2]
			CEP = endElement[3]
		else:
			incon = 1
			idMuni = 'NaN'
			CEP = 'NaN'
	return (x[0], (idMuni, CEP, CEP[:4], incon))

def finalFormat(x):
	try:
		return (x[0], (x[1][0], x[1][1][0], x[1][1][1]))
	except:
		return None

def finalAggFormat(x):
	try:
		retorno = (x[0], (x[1][0], x[1][1][0], x[1][1][1], x[1][1][2]))
		if retorno[1][0] == None:
			return None
		elif retorno[1][2] == None:
			return None
		elif retorno[1][2][1] == '00000000':
			return None
		return retorno
	except:
		return None

def calculateRelacoes(x):
	relations = x[1]
	active = False
	inc = 0
	contribDays = 0
	baseDate = '2999-01-01'
	remTotal = 0
	rem13Total = 0
	remFGTSTotal = 0
	for i in relations:
		if i[1] == '':
			active = True
		elif int(i[1]) < 0:
			inc = 1
		else:
			contribDays = contribDays + int(i[1])
		baseDate = i[0] if i[0] < baseDate and i[0] != '' else baseDate
		try:
			remTotal = remTotal + float(i[2])
		except:
			remTotal = remTotal + 0
		try:
			rem13Total = rem13Total + float(i[3])
		except:
			rem13Total = rem13Total + 0
		try:
			remFGTSTotal = remFGTSTotal + int(float(i[4]))
		except:
			remFGTSTotal = remFGTSTotal + 0
	return (x[0], (contribDays, baseDate if active else 0, '%.2f'%remTotal, '%.2f'%rem13Total, '%.2f'%remFGTSTotal, inc))

def filtering(x):
	try:
		return x[1][2][1]
	except:
		return 0

fEnd = sc.textFile("/u01/data/base/ENDERECOS")
fPF = sc.textFile("/u01/data/base/PESSOAS_FISICAS")
fCert = sc.textFile("/u01/data/base/CERTIDOES_CIVIS")
fRes = sc.textFile("/u01/data/base/RESUMO_RELACOES_TRABALHISTAS") 



'''
CERTIDOES_CIVIS_PART
'''

#Retorno da Linha: [[id_certidao_civil, id_pessoa_fisica, dt_evento, dt_emissao, dt_separacao, dt_divorcio]]
rddCertidoes = fCert.map(lambda line: line.split("|"))

#Retorno [('1027040844', 'Inc.'),  ('1665214649', '1976-12-08;155259490')] -> (PF_ID, 'data mais recente entre dt_evento e dt_emissão; id_certidao_civil') 
rddDates = rddCertidoes.map(filterByEvent).reduceByKey(lambda a, b: a + ("|" + b))

#Retorno [('113927770', ('1229477534', '2006-07-15', 0))] -> (CERT_ID, (PF_ID, Data_Mais_Recente, Check_Inconsistencia (0-OK, 1-Inc)))
rddInfo = rddDates.map(filterDates)

#Retorno [('13047457', ('', ''))] -> (CERT_ID, (dt_separacao, dt_divorcio))
rddCertIds = rddCertidoes.map(lambda x: (str(x[0]).strip(), (str(x[4]).strip(), str(x[5]).strip())))

#Retorno [('14733170', 'NC')] -> (CERT_ID, Data Mais Antiga separacao e divorcio (Caso não conste data retorna NC))
rddDivorcio = rddInfo.map(lambda x: (x[0], 1)).join(rddCertIds).map(filterDivorceDates)

#Retorno [('1116310950', 0, 'NC', 1)] -> (PF_ID, dt_atualização mais nova, data de divorcio ou separacao mais antiga, check_inconsistencia)
fullJoined = rddDivorcio.join(rddInfo).map(finalFiltering)


'''
ENDERECOS PART
'''

rddEnderecos = fEnd.map(lambda line: line.split('|'))

#('2090398103', '310267648;2015-05-29|310267648;2015-05-29')
rddEndByPF = rddEnderecos.map(lambda x: (str(x[1]).strip(), (str(x[0]).strip() + ";" + str(x[4]).strip() + ";" + str(x[2]).strip() + ";" + str(x[3]).strip()))).reduceByKey(lambda a, b: a + ("|" + b))

#(ID_PF, (ID_MUNICIPIO, CEP, CEP[:4], 0))
rddPFbyRecentEnd = rddEndByPF.map(getNewstEnd)

partialResult = rddPFbyRecentEnd.fullOuterJoin(fullJoined)

'''
PESSOAS FISICAS
'''

rddPF = fPF.map(lambda line: line.split('|')).map(lambda x: (str(x[0]).strip(), (str(x[1]).strip(), str(x[2]).strip(), str(x[3]).strip(), str(x[4]).strip(), str(x[5]).strip(), str(x[6]).strip(), str(x[7]).strip(), str(x[8]).strip())))
rddResult = rddPF.fullOuterJoin(partialResult).map(finalFormat).filter(lambda line: line != None)


'''
RELACOES TRABALHISTAS
'''

rddRelacoes = fRes.map(lambda line: line.split("|")).map(lambda x: (str(x[1]).strip(), ((str(x[3]).strip(), str(x[4]).strip(), str(x[5]).strip(), str(x[6]).strip(), str(x[7]).strip()),)))
rddAgg = rddRelacoes.reduceByKey(lambda a, b: a + b).map(calculateRelacoes)

#('1238510985', ((58, 0, '6762958.48', '772990.68', '0.00', 0), (None, None, ('1999-06-14', 'NC', 0))))

finalResult = rddAgg.fullOuterJoin(rddResult).map(finalAggFormat).filter(lambda line: line != None)

#id_pessoa_fisica|id_pessoa_fisica_dv|dt_nascimento|dt_obito|cs_etnia|cs_estado_civil|cs_grau_instrucao|cs_nacionalidade|cs_sexo 


#(id_pessoa_fisica,((id_pessoa_fisica_dv,dt_nascimento,dt_obito,cs_etnia,cs_estado_civil,cs_grau_instrucao,cs_nacionalidade,cs_sexo),(dt_atualização mais nova, data de divorcio ou separacao mais antiga, check_inconsistencia),(ID_MUNICIPIO, CEP, distrito, id_inconcistencia)))
finalResult.sortBy(filtering).saveAsTextFile("/u01/data/Resultados/")


(id_pessoa_fisica: '1283983077', 
	arr_objetos: 
	(	
		vida_trabalhista: 
			(15, 0, '128.51', '0.00', '0.00', 0), 
		dados_PF:
			('0', '2067-06-13', '', '', '', '7', '10', '3'), 
		dados_endereco:
			('354850', '11020150', '1102', 0), 
		dados_relacao_civil
			(0, 'NC', 1)
	)
)
('1281289112', ((486, 0, '3133.34', '260.00', '0.00', 0), ('4', '1980-08-21', '', '', '', '', '10', '3'), ('Jesus', '11020130', '1102', 0), ('1996-04-29', 'NC', 0)))

a = {
	'ID_PF':x[0],
	'OBJETOS':{
		'VIDA_TRABALHISTA':{
			'DIAS_CONTR':x[1][0][0],
			'DATA_FIRST_TRAB':x[1][0][1],
		}
	}
}

('1214281582', ((647, 0, '463519.16', '8963.99', '0.00', 0), ('2', '2067-06-28', '', '', '4', '9', '10', '3'), ('354850', '11020080', '1102', 0), ('1991-09-06', '2005-12-21', 0)))

