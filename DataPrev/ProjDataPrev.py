
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

fEnd = sc.textFile("/u01/data/ENDERECOS")
fPF = sc.textFile("/u01/data/PESSOAS_FISICAS")
fCert = sc.textFile("/u01/data/CERTIDOES_CIVIS")


#Formato da tabela de Certidão Civil Exportada e Filtrada
#id_certidao_civil | id_pessoa_fisica | dt_evento  | dt_emissao | dt_separacao | dt_divorcio

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

fullJoined.sortBy(lambda x: x[1][1]).saveAsTextFile("/u01/data/Teste/")




id_endereco | id_pessoa_fisica | id_muni_ibge |  nu_cep  | dt_alteracao


#id_certidao_civil | id_pessoa_fisica | dt_evento  | dt_emissao | dt_separacao | dt_divorcio
