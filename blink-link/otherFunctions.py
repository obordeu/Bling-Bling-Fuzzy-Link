# AUTHOR: Nano Barahona, Olivia Burdeu
# DATE: 24-Dec-16
# LAST UPDATE: 17-May-17
# ACTION: 
# COMMENTS: 

import Levenshtein
import itertools

def checkOmit(matches, var, tol,names):
	global tolerancia
	tolerancia=tol
	global nombres
	nombres=names

	matches = list(map(lambda (k,v): (k,(v,var)),matches))
	matches=list(filter(filterOmit,matches))

	matches=list(map(lambda (k,v): (k,v[0]),matches))
	return matches


def generateSteps(strVars,numVars,sharpVars):
	if sharpVars!=None:
                sharpVars=sharpVars.strip().split(" ")
        else:
                sharpVars=[]
        numVars=[x for x in numVars if x not in sharpVars]
	strVars=[x for x in strVars if x not in sharpVars]
	steps=[]
	num=[]
	if len(numVars)>0:
		for s in xrange(len(numVars)+1):
			for comb in itertools.combinations(numVars, s):
				num.append(comb)
		for s in xrange(len(strVars)+1):
			for comb in itertools.combinations(strVars, s):
				for n in num:
                                        secondVars=list(n)+sharpVars
					fuzzy=list(set(strVars)-set(comb))
					steps.append((list(comb),fuzzy,1,[],1,secondVars))
					for x in range(0,len(fuzzy)):
						steps.append((list(comb),list(set(fuzzy)-set([fuzzy[x]])),1,[fuzzy[x]],2,secondVars))
	else:
		for s in xrange(len(strVars)+1):
			for comb in itertools.combinations(strVars, s):
				fuzzy=list(set(strVars)-set(comb))
				steps.append((list(comb),fuzzy,1,[],1,sharpVars))
				for x in range(0,len(fuzzy)):
					steps.append((list(comb),list(set(fuzzy)-set([fuzzy[x]])),1,[fuzzy[x]],2,sharpVars))
	steps.sort(key=lambda x: len(x[5]), reverse=True)
	steps.sort(key=lambda x: len(x[0]), reverse=True)
	for s in steps:
		print s
	return steps



def filterOmit(student):
	var = student[1][1]
	name1 = student[1][0][0][1][var]
	name2 = student[1][0][1][1][var]
	otherNames1 = ' '.join([student[1][0][0][1][nms] for nms in nombres])
	otherNames2 = ' '.join([student[1][0][1][1][nms] for nms in nombres])
	if Levenshtein.distance(str(name1),str(name2))<=tolerancia:
		return True
	elif name1 == '' or name2 == '':
		return True
	elif name1 in otherNames2 or name2 in otherNames1:
		return True
	else:
		return False






def calculateErrors(matches, base1, base2):
	wrongmatch = matches.filter(lambda (k,v): v[0][1]['id']!=v[1][1]['id'])
	base1 = base1.map(lambda (k,v): (v[1]['id'],v) )
	base2 = base2.map(lambda (k,v): (v[1]['id'],v) )
	nomatch = base1.join(base2)
	return nomatch, wrongmatch



