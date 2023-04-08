# !/usr/bin/Python

import sys
import os

# Set variable for file
filename = sys.argv[1]

# Set counter to iterate over each line
count = 1

# Create lists to add values to eavh list
start_position = []
end_position = []
strand = []
gene = []
# Read file
with open(filename, 'r') as f:
    for row in f.readlines()[2:]:
        line = row.split('\t')

        Gene = line[0]
        Strand = line[6]
        start_pos = line[3]
        end_pos = line[4]
        # print start_pos

        start_position.append(start_pos)
        end_position.append(end_pos)
        strand.append(Strand)
        gene.append(Gene)

# Identify Operons
operon_list = []
while count < len(gene):
    prev_gene = gene[count - 1]
    curr_gene = gene[count]
    prev_start_pos = start_position[count - 1]
    curr_start_pos = start_position[count]
    prev_end_pos = end_position[count - 1]
    curr_end_pos = end_position[count]
    prev_strand = strand[count - 1]
    curr_strand = strand[count]
    if curr_strand == prev_strand:
        diff = abs(int(float(prev_end_pos) - float(curr_start_pos)))
        if diff < 50:
            operon_list.append(prev_gene)
            operon_list.append(curr_gene)
        # operon_list.append(prev_start_pos)
        else:
            if len(set(operon_list)) > 1:
                for op in list(set(operon_list)):
                    sys.stdout.write("\t" + op)
                print
            operon_list = []
    else:
        if len(set(operon_list)) > 1:
            for op in list(set(operon_list)):
                sys.stdout.write("\t" + op)
            print
        operon_list = []
    count += 1
