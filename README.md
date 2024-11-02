Database Normalization

# Objective
To develop a program that takes a database (relations) and functional dependencies as input, normalizes the relations based on the provided functional dependencies,produces SQL queries to generate the normalized database tables, and optionally determines the highest normal form of the input table.

# Inputs
1) The input relation should be given in exampleInputTable.csv file.
2) The function dependencies must be mentioned in dependencies.txt file.
3) The multi-valued dependencies should be mentioned in mvd_dependencies.txt file.
4) Primary key is user input, and it needs to be mentioned comma separated.
5) Give the choice of the highest normal form you want to normalize.
6) choose yes if you want to find the highest normal form.
7) For 5NF, each of the relations needs candidate keys which should be user input.

# Outputs
1) The input relation will pass through each of the normalization forms until it reaches the highest normal form (user input).
2) In every step, it will check if the relation follows the normalization form and outputs the normalized tables.
3) At the required highest normal form, the program shall quit and output the queries for the normalized tables.
4) In the end, based on the user input, the highest normal form of the input table is displayed.
5) The output queries of the normalized tables are stored in the output.txt file.

# Components
1) main.py : This file is the main file to be executed.
2) input_parser.py : This file is used to parse the inputs from the csv file, txt files, and user inputs.
3) normalizations.py : This file contains the logic and code of normalizations from 1NF to 5NF.
4) output_generator.py : This file is used to generate the required SQL query by taking in the normalized tables as input.
