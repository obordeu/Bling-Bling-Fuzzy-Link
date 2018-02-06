
# -*- coding: utf-8 -*-

from datetime import datetime
import unicodedata


def getData(path,base):
	data=open(path , 'r').read().split('\n')
	global header
	header=data[0].strip().split(',')
	data=data[1:] #filter header from data

	data=list(map(processFile,data))
	data=list(filter(lambda x: x['id']!=None,data))
	data=list(map(lambda x: (base,x),data))

	return data


def processFile(iterator):
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
