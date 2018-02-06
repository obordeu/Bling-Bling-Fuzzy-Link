
# -*- coding: utf-8 -*-

from datetime import datetime
import unicodedata




def getData(path,base,sc):
	data=sc.textFile(path , minPartitions=16)
	header=data.take(1)[0].split(',')
	data=data.filter(lambda x: not x.startswith(header[0]))
	data=data.map(lambda x: processFile(x,header)).filter(lambda x: x['id']!=None).map(lambda x: (base,(base,x)))

	return data


def processFile(iterator, header):
	try: iterator=unicodedata.normalize('NFKD', unicode(iterator)).encode('ASCII', 'ignore')
	except:
		iterator=iterator.decode('latin1').encode('ASCII', 'ignore')
	iterator=iterator.split(',')
	try:
		diccionario={str(header[x]): iterator[x] for x in range(0,len(header))}
	except:
		diccionario={str(header[x]): None for x in range(0,len(header))}
	for var in diccionario:
		try: diccionario[var]=int(diccionario[var])
		except:
			diccionario[var]=diccionario[var]
	return diccionario


def sepVars(variables, dictionary):
	variables=variables.strip().split(" ")
	strVars=[]
	numVars=[]
	for v in variables:
		if type(dictionary[v])==type("") or type(dictionary[v])==type(u""):
			strVars.append(v)
		else:
			numVars.append(v)
	return (strVars,numVars)



