#!/usr/bin/env python
# AUTHOR: Nano Barahona, Olivia Burdeu
# DATE: 24-Dec-16
# LAST UPDATE: 28-Dec-16
# ACTION: Matches students of two different years 
# COMMENTS: Submit the code using spark-submit 01_main.py
# TO DO:
# create option to add number of partitions
# create option to generate clusters only if necesary
# why do they need to provide in id? 

import pyspark


from operator import add
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
	parser.add_option("-p", type="int", dest="partitions")
	parser.add_option("-s", type="string", dest="sharpVars")
	(options, args) = parser.parse_args()
	if options.partitions==None:
		options.partitions=16
	return (options.filename1 ,options.filename2,options.variables, options.partitions,options.sharpVars)


def createRDDS(sc, file1, file2):
	# import data
	data1 = getData(file1,1,sc)
	data2 = getData(file2,2,sc)
	return data1, data2


def algorithm(base1, base2,strVars,numVars,sc,ti,partitions,sharpVars):
	#base1.persist()
	#base2.persist()
	matches = sc.emptyRDD()
	names = strVars

	# steps: ([sharp vars],[fuzzy vars],[fuzzy tol],[check vars],[check tol],[secondary vars],[age range])
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
	crosswalk = getStrGroup(base1, base2, names,sc, fuzzyTol)
	print "Time : ", (time.time()-t1)/60
	# start loops
	for step in steps:
		print "Time passed: ", (time.time()-ti)/60
		if fuzzyTol != step[2]:
			crosswalk = getStrGroup(base1, base2, names,sc, fuzzyTol)
		sharpVars = step[0]
		fuzzyVars = step[1]
		fuzzyTol = step[2]
		checkVars = step[3]
		checkTol = step[4]
		secondVars = step[5]
		# run iterations
		base1.repartition(partitions)
		base2.repartition(partitions)
		matches, base1, base2 = Matches(base1,base2, matches, sharpVars,fuzzyVars,checkVars,secondVars, crosswalk, checkTol, names)
	return matches, base1, base2


def getStrGroup(base1,base2,names,sc, tol):
	print "Building new clusters"
	crosswalk={name: 0 for name in names}
	for var in names:
		cluster = strGroup(base1, base2, var,sc,tol)
		crosswalk[var]=cluster
	return crosswalk

def Matches(base1, base2, matches, sharpVars,fuzzyVars,checkVars,secondVars, crosswalk,checkTol, names):
	print "Iteration: sharp vars :", sharpVars, "; fuzzy vars :", fuzzyVars, "secondary vars :", secondVars , "; checked vars :", checkVars
	# match with keys in order	
	matches, base1, base2 = matchLoop(base1, base2, matches, sharpVars,fuzzyVars,checkVars,secondVars, crosswalk,checkTol,'ordered', names)
	# match using sets
	#matches, base1, base2 = matchLoop(base1, base2, matches, sharpVars,fuzzyVars,checkVars,secondVars, crosswalk,checkTol,iters,'sets')
	return matches, base1, base2
		

def matchLoop(base1, base2, matches, sharpVars,fuzzyVars,checkVars,secondVars, crosswalk,checkTol,style, names):
	# we start by joining the clusters
	for var in fuzzyVars:		
		cluster=crosswalk[var].repartition(1)
		base1 = base1.map(lambda (k,v): (v[1][var],v)).join(cluster).map(lambda (k,v): ( k , tuple( [v[0][i] for i in range(0,len(v[0]))]+[v[1]] ) ) )
		base2 = base2.map(lambda (k,v): (v[1][var],v)).join(cluster).map(lambda (k,v): ( k , tuple( [v[0][i] for i in range(0,len(v[0]))]+[v[1]] ) ) ) 
	# reorganizes the key and values using different styles
	if style=='ordered':
		base1 = base1.map(lambda (k,v): ((tuple([v[1][i] for i in sharpVars]+[v[j] for j in range(2,len(v))]+[v[1][n] for n in secondVars]),(v[0], v[1]))))
		base2 = base2.map(lambda (k,v): ((tuple([v[1][i] for i in sharpVars]+[v[j] for j in range(2,len(v))]+[v[1][n] for n in secondVars]),(v[0], v[1]))))
	elif style=='sets':
		base1 = base1.map(lambda (k,v): ((tuple(sorted([v[1][i] for i in sharpVars])+[v[j] for j in range(2,len(v))]+[v[1][n] for n in secondVars])),(v[0], v[1])))
		base2 = base2.map(lambda (k,v): ((tuple(sorted([v[1][i] for i in sharpVars])+[v[j] for j in range(2,len(v))]+[v[1][n] for n in secondVars])),(v[0], v[1])))
	else:
		print "ERROR: Not valid style (either oredered or sets)"
	matches, base1, base2 = matchBases(base1, base2,matches,checkVars,checkTol, names)
	return matches, base1, base2


def matchBases(base1, base2, matches, checkVars, checkTol, names):
	base1b = base1.reduceByKey(add).filter(lambda (k,v): len(v)==2) 
	base2=base2.repartition(1)
	uniquematches = base1b.join(base2,16)
	base1b.unpersist()
	uniquematches = uniquematches.reduceByKey(add)
	uniquematches = uniquematches.filter(lambda (k,v): len(v)==2 and v[0][0]==1 and v[1][0]==2)
	for var in checkVars:
		uniquematches = checkOmit(uniquematches, var, checkTol, names)
	uniquematches.repartition(1)
	residBase1 = base1.leftOuterJoin(uniquematches,16).filter(lambda (k,v): v[1]==None).map(lambda (k,v): (k,v[0]))
	residBase2 = base2.leftOuterJoin(uniquematches,16).filter(lambda (k,v): v[1]==None).map(lambda (k,v): (k,v[0]))
	uniquematches=uniquematches.map(lambda (k,v): (v[0][1]['id'],v)) # changes key back to id
	matches=matches.union(uniquematches) # append matched to original database
	uniquematches.unpersist()
	matches = matches.repartition(1)
	print "Base 1: %s" % (residBase1.count())
	print "Base 2: %s" % (residBase2.count())
	print "Matched: %s" % (matches.count())

	#print "Errors:"
	#print "Error Type I: %s" % (nomatch.count())
	#print "Error Type II: %s" % (wrongmatch.count())
	print ""
	return matches, residBase1, residBase2


def main():
	print str(datetime.now())
	ti=time.time()
	# This is the main function of the program
	with pyspark.SparkContext() as sc: # create spark context
		sc.setLogLevel("ERROR")
		(file1,file2,variables,partitions,sharpVars)=options()
		print partitions
		RDD1, RDD2 = createRDDS(sc, file1, file2) # runs code and creates database
		#separate variables in string and numeric
		(strVars,numVars)=sepVars(variables,RDD1.take(1)[0][1][1])
		print "Total size of databases:"
		print "Base 1: %s" % (RDD1.count())
		print "Base 2: %s" % (RDD2.count())
		print ""
		matched, base1_notmatched, base2_notmatched = algorithm(RDD1,RDD2,strVars,numVars,sc,ti,partitions,sharpVars)
		#nomatch, wrongmatch = calculateErrors(matched, base1_notmatched, base2_notmatched)
		print "SHOWING RESULTS"
		print "Total size of databases:"
		print "Base 1: %s" % (RDD1.count())
		print "Base 2: %s" % (RDD2.count())
		#print "Errors:"
		#print "Error Type I: %s" % (nomatch.count())
		#print "Error Type II: %s" % (wrongmatch.count())
		matched=matched.collect()
		
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


