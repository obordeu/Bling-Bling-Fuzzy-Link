#!/usr/bin/env python
# AUTHOR: Nano Barahona, Olivia Bordeu
# DATE: 24-Dec-16
# LAST UPDATE: 17-May-17
# ACTION: Matches students of two different years 
# COMMENTS: Submit the code using spark-submit 01_main.py

from collections import defaultdict
from datetime import datetime
import time
import csv
from optparse import OptionParser

from namesGroup import strGroup
from otherFunctions import checkOmit, calculateErrors, generateSteps
from getData import getData,  sepVars

def options():
	parser=OptionParser()
	parser.add_option("-f","--file1",dest="filename1", 
					help="include the path to the first file", metavar="FILE")
	parser.add_option("-g","--file2",dest="filename2", 
					help="include the path to the second file", metavar="FILE")
	parser.add_option("-v", type="string", dest="variables")
	parser.add_option("-s", type="string", dest="sharpVars")
	(options, args) = parser.parse_args()
	return (options.filename1 ,options.filename2,options.variables,options.sharpVars)


def createRDDS(file1, file2):
	# import data
	data1 = getData(file1,1)
	data2 = getData(file2,2)
	return data1, data2


def algorithm(base1, base2,strVars,numVars,sharpVars):
	#base1.persist()
	#base2.persist()
	matches = []
	global names
	names = strVars

	# steps: ([sharp vars],[fuzzy vars],[fuzzy tol],[check vars],[check tol],[secondary vars],[age range],[changeOrder of names])
	# inside each iteration we:
	#	1. we create keys using sharp, fuzzy, secondary vars and age
	#	2. we match using keys and check that check vars attain some minimum standards
	#	3. we match using sets for keys and check that check vars attain some minimum standards
	#	4. we vary age from 1 to age range where:
	# 		- 1) 1 year, 2) 5 years, 3) 10 years, 4) 20 years
	#		- we move 1/2 bandwidth up and down each time
	steps=generateSteps(strVars,numVars,sharpVars)

	# steps=[(['nombre1','apellido1','nombre2','apellido2'],[],1,[],1,['provincia','mujer'],1),
	# 	(['nombre1','apellido1'],['nombre2','apellido2'],1,[],1,['provincia','mujer'],1),
	# 	(['nombre1', 'apellido1'],['apellido2'],1,['nombre2'],1,['provincia','mujer'],2),
	# 	(['nombre1'],['apellido1', 'apellido2'],1,['nombre2'],1,['provincia','mujer'],2),
	# 	(['apellido1'],['nombre1', 'nombre2'],1,['apellido2'],1,['provincia', 'mujer'],2)]		

	fuzzyTol = 1
	print "String Group"
	t1=time.time()
	base1, base2= getStrGroup(base1, base2, fuzzyTol)
	print "tiempo : ", (time.time()-t1)/60
	# start loops
	for step in steps:
		sharpVars = step[0]
		fuzzyVars = step[1]
		fuzzyTol = step[2]
		checkVars = step[3]
		checkTol = step[4]
		secondVars = step[5]
		# run iterations
		matches, base1, base2 = Matches(base1,base2, matches, sharpVars,fuzzyVars,checkVars,secondVars, checkTol)

	return matches, base1, base2


def getStrGroup(base1,base2, tol):
	print "Building new clusters of strings..."
	for var in names:
		cluster = strGroup(base1, base2, var,tol)
		base1=list(map(lambda v:(v[1][var],v),base1))
		base2=list(map(lambda v:(v[1][var],v),base2))
		#join cluster to each base
		base1=[(x[1],cluster[x[0]]) for x in base1]
		base2=[(x[1],cluster[x[0]]) for x in base2]
		#re-order elements
		base1=list(map(lambda v:  tuple( [v[0][i] for i in range(0,len(v[0]))]+[v[1]] ),base1 ))
		base2=list(map(lambda v:  tuple( [v[0][i] for i in range(0,len(v[0]))]+[v[1]] ),base2 ))
	base1=list(map(lambda v: ( v[0],v[1],{names[x]:v[x+2] for x in range(0,len(names))}),base1 ) )
	base2=list(map(lambda v: ( v[0],v[1],{names[x]:v[x+2] for x in range(0,len(names))}),base2 ) )
	return base1, base2

def Matches(base1, base2, matches, sharpVars,fuzzyVars,checkVars,secondVars,checkTol):
	print "Iteration: sharp vars :", sharpVars, "; fuzzy vars :", fuzzyVars, "secondary vars :", secondVars , "; checked vars :", checkVars
	# match with keys in order			
	matches, base1, base2 = matchLoop(base1, base2, matches, sharpVars,fuzzyVars,checkVars,secondVars,checkTol,'ordered')
	# match using sets
	#matches, base1, base2 = matchLoop(base1, base2, matches, sharpVars,fuzzyVars,checkVars,secondVars, crosswalk,checkTol,iters,'sets')
	return matches, base1, base2
		

def matchLoop(base1, base2, matches, sharpVars,fuzzyVars,checkVars,secondVars,checkTol,style):
	# reorganizes the key and values using different styles
	if style=='ordered':
		base1 = list(map(lambda v: ((tuple([v[1][i] for i in sharpVars]+[v[2][j] for j in fuzzyVars]+[v[1][n] for n in secondVars]),v)),base1))
		base2 = list(map(lambda v: ((tuple([v[1][i] for i in sharpVars]+[v[2][j] for j in fuzzyVars]+[v[1][n] for n in secondVars]),v)),base2))
	elif style=='sets':
		base1 = list(map(lambda v: ((tuple(sorted([v[1][i] for i in sharpVars])+[v[j] for j in range(2,len(v))]+[v[1][n] for n in secondVars]),v)),base1))
		base2 = list(map(lambda v: ((tuple(sorted([v[1][i] for i in sharpVars])+[v[j] for j in range(2,len(v))]+[v[1][n] for n in secondVars]),v)),base2))
	else:
		print "ERROR: Not valid style (either oredered or sets)"
		
	matches, base1, base2 = matchBases(base1, base2,matches,checkVars,checkTol)
	#change back to no key
	base1=list(map(lambda (k,v): v,base1))
	base2=list(map(lambda (k,v): v,base2))
	return matches, base1, base2

def keyfy(iterator):
	key=""
	for x in iterator[0]:
		key=key+str(x)
	return (key,iterator[1])

def reduceByKey(data):
	data=list(map(keyfy,data))
	output=defaultdict(list)
	for key,value in data:
		output[key].append(value)
	return output.items()


def matchBases(base1, base2, matches, checkVars, checkTol):
	#keep only the ones that are unique in each dataset
	base1b = reduceByKey(base1)
	base2b = reduceByKey(base2)

	#merge them
	base2b={x[0]:x[1] for x in base2b}
	uniquematches=[(x[0],(x[1],base2b.get(x[0]))) for x in base1b]
	uniquematches=list(map(lambda (k,v): (k,(v[0][0],v[1][0])), filter(lambda (k,v):v[1]!=None,uniquematches)))
	print len(uniquematches)

	for var in checkVars:
		uniquematches = checkOmit(uniquematches, var, checkTol,names)

	m1=[k[1][0][1]['id'] for k in uniquematches ]
	m2=[k[1][1][1]['id'] for k in uniquematches ]
	residBase1=[x for x in base1 if x[1][1]['id'] not in m1]
	residBase2=[x for x in base2 if x[1][1]['id'] not in m2]

	uniquematches=list(map(lambda (k,v): (v[0][1]['id'],v),uniquematches)) # changes key back to id
	matches=matches+uniquematches # append matched to original database

	print "Base 1: %s" % len(residBase1)
	print "Base 2: %s" % len(residBase2)
	print "Matched: %s" % len(matches)
	print ""
	return matches, residBase1, residBase2


def main():

	print str(datetime.now())
	ti=time.time()
	# This is the main function of the program
	(file1,file2,variables,sharpVars)=options()

	RDD1, RDD2 = createRDDS(file1,file2) # runs code and creates database
	#separate variables in string and numeric
	(strVars,numVars)=sepVars(variables,RDD1[0][1])

	print "Total size of databases:"
	print "Base 1: %s" % (len(RDD1))
	print "Base 2: %s" % (len(RDD2))
	print ""
	matched, base1_notmatched, base2_notmatched = algorithm(RDD1,RDD2,strVars,numVars,sharpVars)
	
	print "SHOWING RESULTS"
	print "Total size of databases:"
	print "Base 1: %s" % (len(RDD1))
	print "Base 2: %s" % (len(RDD2))
	print "Matched: %s" % (len(matched))


	with open('match.csv', 'wb') as myfile:
		wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
		header=list()
		h1=list()
		h2=list()
		for x in matched[0][1][0][1]:
			x=str(x).strip()
			header.append(x)
			h1.append(x)
		for x in matched[0][1][1][1]:
			x=str(x).strip()
			header.append(x)
			h2.append(x)
				
		wr.writerow(header)
		for student in matched:
			row=[]
			for x in h1:
				row.append(student[1][0][1][x])
			for x in h2:
				row.append(student[1][1][1][x])
			wr.writerow(row)
	print "FINISHED"
	print "The run time was ", (time.time()-ti)/60, " minutes."


if __name__ == '__main__':
    main()


