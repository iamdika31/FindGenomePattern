# findGenomePattern

This repo is about how to get count of genome pattern from data_genome.txt.

Find & count Genome Pattern:
1. GCA
2. AGT
3. TTAGG

requirements:
- if the pattern GCAGT it will count GCA and AGT 
- if the pattern TTAGGCAGT it will count TTAGG,GCA,AGT

The requirements also applies to similar patterns

In this code will used JavaRDD approach and mapReduce Function
