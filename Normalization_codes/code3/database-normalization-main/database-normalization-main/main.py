# This is main file to read the csv files, and call other files, and for outputs
import pandas as pd
import csv
import normalizations
import input_parser
from output_generator import output_1NF, output_all_nfs
import os

# Reading the input CSV file and the dependencies text file
input_file = pd.read_csv('exampleInputTable.csv')
print('INPUT RELATION')
print(input_file)
print('\n')

with open('dependencies.txt', 'r') as file:
    lines = [line.strip() for line in file]

dependencies = {}
for line in lines:
    determinant, dependent = line.split(" -> ")
    determinant = determinant.split(", ")
    dependencies[tuple(determinant)] = dependent.split(", ")
print('DEPENDENCIES')
print(dependencies)
print('\n')

# Input from the user
max_normalization = input(
    'Choice of the highest normal form to reach (1: 1NF, 2: 2NF, 3: 3NF, B: BCNF, 4: 4NF, 5: 5NF, 6: DKNF): ')

# Convert to integer if the input is a number, else keep it as 'B'
if max_normalization != 'B':
    max_normalization = int(max_normalization)

# Find the highest normal form of the input relation
find_high_nf = int(
    input('Find the highest normal form of the input table? (1: Yes, 2: No): '))
high_nf = 'Not normalized yet to any normal form'

# Enter Primary Key
primary_key = input(
    "Enter Primary Key values separated by comma: ").split(', ')
print('\n')

primary_key = tuple(primary_key)

# Multi-valued dependencies (only apply to 4NF and above)
mvd_dependencies = {}
if not max_normalization == 'B' and max_normalization >= 4:
    with open('mvd_dependencies.txt', 'r') as file:
        mvd_lines = [line.strip() for line in file]

    for mvd in mvd_lines:
        determinant, dependent = mvd.split(" ->> ")
        determinant = determinant.split(", ") if ", " in determinant else [determinant]
        determinant_str = str(determinant)
        if determinant_str in mvd_dependencies:
            mvd_dependencies[determinant_str].append(dependent)
        else:
            mvd_dependencies[determinant_str] = [dependent]

    print('MULTI-VALUED DEPENDENCIES')
    print(mvd_dependencies)
    print('\n')

# Parse the input file
input_file = input_parser.input_parser(input_file)

# Perform normalization based on max_normalization input
if max_normalization == 'B' or max_normalization >= 1:
    one_nf_table, one_flag = normalizations.first_normalization_form(
        input_file, primary_key)

    if one_flag:
        high_nf = 'Highest Normal Form of input table is: 1NF'

    if max_normalization == 1:
        if one_flag:
            print('Already Normalized to 1NF')
            print('\n')

        print('OUTPUT QUERIES AFTER 1NF:')
        print('\n')
        output_1NF(primary_key, one_nf_table)

if max_normalization == 'B' or max_normalization >= 2:
    two_nf_tables, two_flag = normalizations.second_normalization_form(
        one_nf_table, primary_key, dependencies)
    
    if one_flag and two_flag:
        high_nf = 'Highest Normal Form of input table is: 2NF'

    if max_normalization == 2:
        if two_flag and one_flag:
            print('Already Normalized to 2NF')
            print('\n')

        print('OUTPUT QUERIES AFTER 2NF:')
        print('\n')
        output_all_nfs(two_nf_tables)

if max_normalization == 'B' or max_normalization >= 3:
    three_nf_tables, three_flag = normalizations.third_normalization_form(
        two_nf_tables, primary_key, dependencies)

    if one_flag and two_flag and three_flag:
        high_nf = 'Highest Normal Form of input table is: 3NF'

    if max_normalization == 3:
        if three_flag and two_flag and one_flag:
            print('Already Normalized to 3NF')
            print('\n')

        print('OUTPUT QUERIES AFTER 3NF:')
        print('\n')
        output_all_nfs(three_nf_tables)

if max_normalization == 'B' or max_normalization >= 4:
    bc_nf_tables, bcnf_flag = normalizations.bc_normalization_form(
        three_nf_tables, primary_key, dependencies)

    if one_flag and two_flag and three_flag and bcnf_flag:
        high_nf = 'Highest Normal Form of input table is: BCNF'

    if max_normalization == 'B':
        if bcnf_flag and three_flag and two_flag and one_flag:
            print('Already Normalized to BCNF')
            print('\n')

        print('OUTPUT QUERIES AFTER BCNF:')
        print('\n')
        output_all_nfs(bc_nf_tables)

if not max_normalization == 'B' and max_normalization >= 4:
    four_nf_tables, four_flag = normalizations.fourth_normalization_form(
        bc_nf_tables, mvd_dependencies)

    if one_flag and two_flag and three_flag and bcnf_flag and four_flag:
        high_nf = 'Highest Normal Form of input table is: 4NF'

    if max_normalization == 4:
        if four_flag and bcnf_flag and three_flag and two_flag and one_flag:
            print('Already Normalized to 4NF')
            print('\n')

        print('OUTPUT QUERIES AFTER 4NF:')
        print('\n')
        output_all_nfs(four_nf_tables)

if not max_normalization == 'B' and max_normalization >= 5:
    five_nf_tables, five_flag = normalizations.fifth_normalization_form(
        four_nf_tables, primary_key, dependencies)

    if one_flag and two_flag and three_flag and bcnf_flag and four_flag and five_flag:
        high_nf = 'Highest Normal Form of input table is: 5NF'

    if max_normalization == 5:
        if five_flag and four_flag and bcnf_flag and three_flag and two_flag and one_flag:
            print('Already Normalized to 5NF')
            print('\n')

        print('OUTPUT QUERIES AFTER 5NF:')
        print('\n')
        output_all_nfs(five_nf_tables)

# Load domain constraints, if any
domain_constraints = {}
if os.path.exists('domain_constraints.txt') and os.stat('domain_constraints.txt').st_size > 0:
    with open('domain_constraints.txt', 'r') as file:
        lines = [line.strip() for line in file if line.strip()]
        for line in lines:
            attribute, constraint = line.split(" : ")
            domain_constraints[attribute.strip()] = constraint.strip()
else:
    print("No domain constraints provided or file is empty; proceeding without domain checks.")




# Ensure five_nf_tables is a DataFrame before proceeding
# Check and restructure five_nf_tables for DataFrame conversion
if not isinstance(five_nf_tables, pd.DataFrame):
    print("five_nf_tables structure for debugging:", five_nf_tables)  # Examine the structure of five_nf_tables

    # Attempt restructuring based on common structures
    if isinstance(five_nf_tables, dict):
        # Check for mixed values
        if all(isinstance(v, list) for v in five_nf_tables.values()):
            # All values are lists, directly convert to DataFrame
            five_nf_tables = pd.DataFrame(five_nf_tables)
            print("Converted five_nf_tables to DataFrame (from dictionary of lists).")
        elif all(isinstance(v, (str, int, float)) for v in five_nf_tables.values()):
            # All values are scalars, create a single-row DataFrame
            five_nf_tables = pd.DataFrame([five_nf_tables])
            print("Converted five_nf_tables to single-row DataFrame (from dictionary of scalars).")
        else:
            # Mixed types in dictionary values; attempt conversion to a list of dictionaries
            try:
                # Example conversion to ensure lists for consistency
                five_nf_tables = [{k: v} if not isinstance(v, list) else {k: item for item in v} for k, v in five_nf_tables.items()]
                five_nf_tables = pd.DataFrame(five_nf_tables)
                print("Converted mixed-format dictionary in five_nf_tables to DataFrame.")
            except ValueError as e:
                print("Error restructuring five_nf_tables:", e)
                print("Consider manually verifying and restructuring five_nf_tables.")
    elif isinstance(five_nf_tables, list) and all(isinstance(row, dict) for row in five_nf_tables):
        # List of dictionaries, convert directly
        five_nf_tables = pd.DataFrame(five_nf_tables)
        print("Converted five_nf_tables (list of dictionaries) to DataFrame.")
    else:
        print("Error: five_nf_tables is in an unsupported format for DataFrame conversion.")
        print("Expected formats: dictionary of lists (columns) or list of dictionaries (rows).")
else:
    print("five_nf_tables is already a DataFrame.")


# Default dknf_tables to an empty list if DKNF normalization does not execute
dknf_tables = None

# Ensure five_nf_tables is a DataFrame before proceeding with DKNF normalization
if isinstance(five_nf_tables, pd.DataFrame):  
    # Proceed with DKNF normalization if max_normalization is set to 6
    if max_normalization == 6:
        dknf_tables, dknf_flag = normalizations.dknf_normalization_form(
            five_nf_tables, primary_key, dependencies, domain_constraints
        )

        if dknf_flag:
            high_nf = 'Highest Normal Form of input table is: DKNF'
            print(high_nf)
            print('Already Normalized to DKNF')
            print('\n')

        print('OUTPUT QUERIES AFTER DKNF:')
        print('\n')

# Convert dknf_tables to a dictionary if it’s a list and not None
if dknf_tables is not None:
    if isinstance(dknf_tables, list):
        # Assign unique names to each table if it’s in list form
        dknf_tables = {f"Table_{i+1}": table for i, table in enumerate(dknf_tables)}
        print("Converted dknf_tables from list to dictionary format for output.")
    
    # Pass the correctly formatted dictionary to output_all_nfs
    output_all_nfs(dknf_tables)
else:
    print("Error: DKNF normalization did not generate any tables. Please verify the normalization process.")



# Output the highest normal form detected
if find_high_nf == 1:
    print('\n')
    print(high_nf)
    print('\n')






