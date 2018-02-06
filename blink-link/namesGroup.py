
# AUTHOR: Nano Barahona, Olivia Bordeu
# DATE: 24-Dec-16
# LAST UPDATE: 17-May-17
# ACTION: 
# COMMENTS: 

import Levenshtein
from operator import add
import time

def levenshteinCluster(student):
	student1=student[0]
	students2=student[1]
	distances=[]
	cluster=[]
	for stu in students2:
		dist=Levenshtein.distance(student1,stu)
		distances.append((stu,dist))
	distances=sorted(distances, key=lambda palabra: palabra[1])
	x=0
	while distances[x][1]<=tolerancia:
		cluster.append(distances[x][0])
		x=x+1
		if x==len(distances): break
	cluster = set(cluster)
	return (student1,cluster)



def genCluster(base):
	base={nombres[0]:nombres[1] for nombres in base}
	cluster = reduceCluster(base)
	i=1
	clusterID=[]
	for cl in cluster:
		clusterID.append((i,list(cl)))
		i=i+1

	clusterID=[(x[0],y) for x in clusterID for y in x[1]] #flatMapValues

	clusterID = list(map(lambda x:(x[1],x[0]),clusterID))
	return clusterID


def reduceCluster(diccionario):
	cluster=[]
	def connected_components(neighbors):
	    seen = set()
	    def component(node):
	        nodes = set([node])
	        while nodes:
	            node = nodes.pop()
	            seen.add(node)
	            nodes |= neighbors[node] - seen 
	            yield node
	    for node in neighbors:
	        if node not in seen:
	            yield component(node)

	for component in connected_components(diccionario):
		c = set(component)
		cluster.append(c)
	return cluster


def strGroup(base1, base2, iter,tol):
	global tolerancia
	tolerancia=tol
	print iter
	base = base1+base2
	base = list(set(map(lambda x: x[1][iter],base)))

	#filtrar vacios y enteros
	base=list(filter(lambda x: type(x)!='int' and len(x)>0,base))
	base=list(map(lambda x: (x[0],x),base))
	#gen list of all names starting with a letter
	values = set(map(lambda x:x[0], base))
	lista=[(x,[y[1] for y in base if y[0]==x]) for x in values]
	base=[(x[1],[y[1] for y in lista if y[0]==x[0]][0]) for x in base]

	base = list(map(levenshteinCluster,base))
	cluster = genCluster(base)
	cluster.append(("",0)) # add case for empty string, all have to have cluster
	cl={c[0]:c[1] for c in cluster}
	return cl
	


