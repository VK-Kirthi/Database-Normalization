# This is main file to read the csv files, and call other files, and for outputs
import pandas as pd
import csv
import normalizations
import input_parser
from output_generator import output_1NF, output_all_nfs

# Reading the input csv file and the dependencies text file
input_file = pd.read_csv('exampleInputTable.csv')
print('INPUT RELATION')
print(input_file)
print('\n')

with open('dependencies.txt', 'r') as file:
    lines = [line.strip() for line in file]

dependencies = {}
for line in lines:
    determinant, dependent = line.split(" -> ")
    # Splitting the determinant by comma to make it a list
    determinant = determinant.split(", ")
    dependencies[tuple(determinant)] = dependent.split(", ")
print('DEPENDENCIES')
print(dependencies)
print('\n')

# Input from the user
max_normalization = input(
    'Choice of the highest normal form to reach (1: 1NF, 2: 2NF, 3: 3NF, B: BCNF, 4: 4NF, 5: 5NF): ')

# Convert to integer if the input is a number, else keep it as 'B'
if max_normalization != 'B':
    max_normalization = int(max_normalization)

# Find the highest normal form of the input relation
find_high_nf = int(
    input('Find the highest normal form of the input table? (1: Yes, 2: No): '))
high_nf = 'Not normalized yet to any normal form'

# Initialize primary key and normalization flags
primary_key = input("Enter Primary Key values separated by comma: ").split(', ')
print('\n')
primary_key = tuple(primary_key)

# Initialize flags for all normal forms to False by default
one_flag = two_flag = three_flag = bcnf_flag = four_flag = five_flag = False

mvd_dependencies = {}
# Multi-valued dependencies only apply to 4NF and above
if not max_normalization == 'B' and max_normalization >= 4:
    with open('mvd_dependencies.txt', 'r') as file:
        mvd_lines = [line.strip() for line in file]

    print(mvd_lines)

    for mvd in mvd_lines:
        determinant, dependent = mvd.split(" ->> ")
        determinant = determinant.split(
            ", ") if ", " in determinant else [determinant]
        determinant_str = str(determinant)
        if determinant_str in mvd_dependencies:
            mvd_dependencies[determinant_str].append(dependent)
        else:
            mvd_dependencies[determinant_str] = [dependent]

    print('MULTI-VALUED DEPENDENCIES')
    print(mvd_dependencies)
    print('\n')

# Parse the input file to handle any preprocessing
input_file = input_parser.input_parser(input_file)

# Perform normalization based on the max_normalization input
if max_normalization == 'B' or max_normalization >= 1:
    one_nf_table, one_flag = normalizations.first_normalization_form(
        input_file, primary_key)

    if one_flag:
        high_nf = 'Highest Normal Form of input table is: 1NF'

    if max_normalization == 1:
        if one_flag:
            print('Already Normalized to 1NF\n')

        print('OUTPUT QUERIES AFTER 1NF:\n')
        output_1NF(primary_key, one_nf_table)

if max_normalization == 'B' or max_normalization >= 2:
    two_nf_tables, two_flag = normalizations.second_normalization_form(
        one_nf_table, primary_key, dependencies)
    
    if one_flag and two_flag:
        high_nf = 'Highest Normal Form of input table is: 2NF'

    if max_normalization == 2:
        if two_flag and one_flag:
            print('Already Normalized to 2NF\n')

        print('OUTPUT QUERIES AFTER 2NF:\n')
        output_all_nfs(two_nf_tables)

if max_normalization == 'B' or max_normalization >= 3:
    three_nf_tables, three_flag = normalizations.third_normalization_form(
        two_nf_tables, primary_key, dependencies)

    if one_flag and two_flag and three_flag:
        high_nf = 'Highest Normal Form of input table is: 3NF'

    if max_normalization == 3:
        if three_flag and two_flag and one_flag:
            print('Already Normalized to 3NF\n')

        print('OUTPUT QUERIES AFTER 3NF:\n')
        output_all_nfs(three_nf_tables)

if max_normalization == 'B' or max_normalization >= 4:
    bc_nf_tables, bcnf_flag = normalizations.bc_normalization_form(
        three_nf_tables, primary_key, dependencies)

    if one_flag and two_flag and three_flag and bcnf_flag:
        high_nf = 'Highest Normal Form of input table is: BCNF'

    if max_normalization == 'B':
        if bcnf_flag and three_flag and two_flag and one_flag:
            print('Already Normalized to BCNF\n')

        print('OUTPUT QUERIES AFTER BCNF:\n')
        output_all_nfs(bc_nf_tables)

if not max_normalization == 'B' and max_normalization >= 4:
    four_nf_tables, four_flag = normalizations.fourth_normalization_form(
        bc_nf_tables, mvd_dependencies)

    if one_flag and two_flag and three_flag and bcnf_flag and four_flag:
        high_nf = 'Highest Normal Form of input table is: 4NF'

    if max_normalization == 4:
        if four_flag and bcnf_flag and three_flag and two_flag and one_flag:
            print('Already Normalized to 4NF\n')

        print('OUTPUT QUERIES AFTER 4NF:\n')
        output_all_nfs(four_nf_tables)

if not max_normalization == 'B' and max_normalization >= 5:
    five_nf_tables, five_flag = normalizations.fivth_normalization_form(
        four_nf_tables, primary_key, dependencies)

    if one_flag and two_flag and three_flag and bcnf_flag and four_flag and five_flag:
        high_nf = 'Highest Normal Form of input table is: 5NF'

    if max_normalization == 5:
        if five_flag and four_flag and bcnf_flag and three_flag and two_flag and one_flag:
            print('Already Normalized to 5NF\n')

        print('OUTPUT QUERIES AFTER 5NF:\n')
        output_all_nfs(five_nf_tables)

# Output the highest normal form detected
if find_high_nf == 1:
    print('\nNormalization Flags:')
    print(f"1NF: {one_flag}, 2NF: {two_flag}, 3NF: {three_flag}, BCNF: {bcnf_flag}, 4NF: {four_flag}, 5NF: {five_flag}\n")

    print('Highest Normal Form:')
    print(high_nf)
    print('\n')

