 id_seq_relacao_trabalhista|id_pessoa_fisica|id_estab|dt_ini_relacao_trabalhista|nu_dias_contribuicao|vl_remuneracao_total|vl_remuneracao13_total|vl_remuneracao_fgts_total

('1249948386', (('1999-06-08', '304', '3522.09', '317.70', ''), ('1997-04-15', '', '12174.04', '508.89', '')))

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

fRes = sc.textFile("/u01/data/RESUMO_RELACOES_TRABALHISTAS") 
rddRelacoes = fRes.map(lambda line: line.split("|")).map(lambda x: (str(x[1]).strip(), ((str(x[3]).strip(), str(x[4]).strip(), str(x[5]).strip(), str(x[6]).strip(), str(x[7]).strip()),)))
rddAgg = rddRelacoes.reduceByKey(lambda a, b: a + b).map(calculateRelacoes)

rddAgg.saveAsTextFile("/u01/data/Resultados/")

objeto = {
	'id_pf': x[0],
	'values': {
		'contrib_days': x[1][0]
		
	} 
}

