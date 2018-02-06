
# AUTHOR: Nano Barahona, Olivia Bordeu
# DATE: 24-Dec-16
# LAST UPDATE: 24-Dec-16
# ACTION: 
# COMMENTS: 


import Levenshtein
from operator import add
import time

def levenshteinCluster(student,tol):
	student1=student[0]
	students2=student[1]
	distances=[]
	cluster=[]
	for stu in students2:
		dist=Levenshtein.distance(student1,stu)
		distances.append((stu,dist))
	distances=sorted(distances, key=lambda palabra: palabra[1])
	x=0
	while distances[x][1]<=tol:
		cluster.append(distances[x][0])
		x=x+1
		if x==len(distances): break
	cluster = set(cluster)
	return (student1,cluster)



def genCluster(base):
	base=base.map(lambda (k,v): (k[0],(k,v)))
	base = base.groupByKey().mapValues(list)
	base = base.map(lambda (k,v): (k,{palabra[0]: set(edge for edge in palabra[1]) for palabra in v}))
	cluster = base.map(reduceCluster)
	cluster = cluster.flatMapValues(lambda x: x)
	cluster = cluster.map(lambda (k,v):(v)).zipWithUniqueId()
	cluster = cluster.map(lambda (k,v):(v,tuple(k))).flatMapValues(lambda x: x)
	cluster = cluster.map(lambda (k,v):(v,k))
	return cluster


def reduceCluster(diccionario):
	cluster=[]
	diccionario=diccionario[1]
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
	return (1,cluster)


def strGroup(base1, base2, iter,sc,tol):
	base = base1.union(base2)
	base = base.map(lambda (k,v): (v[1][iter],1)).reduceByKey(add)
	base=base.filter(lambda (k,v): k!="")
	lista=base.sortByKey()
	lista = base.map(lambda (k,v): (k[0],k)).groupByKey().mapValues(list)
	base = base.map(lambda (k,v): (k[0],k)).join(lista,16)
	base = base.map(lambda (k,v): (v[0], v[1]))
	base = base.map(lambda x: levenshteinCluster(x,tol))
	cluster = genCluster(base)
	aux0=sc.parallelize([("","")])
	cluster=cluster.union(aux0)
	return cluster
	


