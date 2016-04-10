class RDDmaster(object):
	def __init__(self, inputPath=None, outputPath=None):
		self.data = None
		self.inputPath = inputPath
		self.outputPath = outputPath
		self.sc = sc
	def dict(self, x):
		raise NotImplementedError("Subclass should implement the dict Method")
	def execute(self):
		raise NotImplementedError("Subclass should implement the execute Method")
	def save(self):
		raise NotImplementedError("Subclass should implement the save Method")
	def getRDDschema(self):
		raise NotImplementedError("Subclass should implement the getRDDschema Method")
	def setInputPath(self, inputPath):
		self.inputPath = inputPath
	def setOutputPath(self, outputPath):
		self.outputPath = outputPath

class Enderecos(RDDmaster):
	@staticmethod
	def __getNewstEnd(x):
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
	def dict(self, x):
		return	{
			'id_pf': x[0],
			'values':{
				'id_municipio': x[1][0],
				'CEP': x[1][1],
				'Dist': x[1][2],
				'Incons': x[1][3]
			}
		}
	def execute(self):
		try:
			if self.inputPath == None:
				raise NameError('No Input Path was Defined. Use setInputPath to define new one')
		except:
			print 'Failed to proceed with execution!'
			raise
		self.data = self.sc.textFile(self.inputPath) \
						.map(lambda line: line.split('|')) \
		   			 	.map(lambda x: (str(x[1]).strip(), ((str(x[0]).strip(), str(x[4]).strip(), str(x[2]).strip(), str(x[3]).strip()),))) \
		   			 	.reduceByKey(lambda a, b: a + b) \
		   			 	.map(self.__getNewstEnd)
		return True
	def save(self):
		try:
			if self.outputPath == None:
				raise NameError('No Input Path was Defined. Use setInputPath to define new one')
		except:
			print 'Falied to proceed with Save Method'
			raise
		self.data.saveAsTextFile(self.outputPath)
	def getRDDschema(self):
		return	{
			'id_pf': 'ID Pessoa Fisica',
			'value':	{
				'id_municipio': 'ID Municipio',
				'CEP': 'CEP Vinculado ao endereco mais recente',
				'Dist': 'Distrito baseado nos 4 primeiros digitos do CEP',
				'Incons': '1 - Caso dado seja inconsistente, 0 - caso dado esteja correto'
				}
			}

rddObj = Enderecos("/home/ubuntu/Baldim/TESTE_ENDERECOS", "/home/ubuntu/Baldim/Enderecos/")
rddObj.execute()
rddObj.saveOutput()
rddObj.getObjSchema()

for i in rddObj.data.collect()[:10]:
	print rddObj.dict(i)



import types
 
def create_function(name, args):
    def y(): pass
    y_code = types.CodeType(args, \
                y.func_code.co_nlocals, \
                y.func_code.co_stacksize, \
                y.func_code.co_flags, \
                y.func_code.co_code, \
                y.func_code.co_consts, \
                y.func_code.co_names, \
                y.func_code.co_varnames, \
                y.func_code.co_filename, \
                name, \
                y.func_code.co_firstlineno, \
                y.func_code.co_lnotab)
    return types.FunctionType(y_code, y.func_globals, name)
 
myfunc = create_function('myfunc', 3)
 
print repr(myfunc)
print myfunc.func_name
print myfunc.func_code.co_argcount
 
myfunc(1,2,3,4)
