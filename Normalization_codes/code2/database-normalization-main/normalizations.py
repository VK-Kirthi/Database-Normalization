import pandas as pd
from itertools import combinations
import re
# This file is used for all the normalizations


def is_list_or_set(item):
    return isinstance(item, (list, set))


def is_superkey(relation, determinant):
    grouped = relation.groupby(
        list(determinant)).size().reset_index(name='count')
    return not any(grouped['count'] > 1)


def powerset(s):
    x = len(s)
    for i in range(1 << x):
        yield [s[j] for j in range(x) if (i & (1 << j)) > 0]


def closure(attributes, fds):
    closure_set = set(attributes)
    while True:
        closure_before = closure_set.copy()
        for det, deps in fds.items():
            if set(det).issubset(closure_set):
                closure_set.update(deps)
        if closure_before == closure_set:
            break
    return closure_set


def bcnf_decomposition(relation, dependencies):
    decomposed_tables = []

    for det, dep in dependencies.items():
        closure_set = closure(set(det), dependencies)
        if not closure_set.issuperset(relation.columns):
            cols = list(det) + dep
            if set(cols).issubset(relation.columns) and not set(cols) == set(relation.columns):
                new_table = relation[list(det) + dep].drop_duplicates()
                decomposed_tables.append(new_table)
                relation = relation.drop(columns=dep)

    if not decomposed_tables:
        return [relation]
    else:
        return [relation] + decomposed_tables


def is_1nf(relation):
    if relation.empty:
        return False

    for column in relation.columns:
        unique_types = relation[column].apply(type).nunique()
        if unique_types > 1:
            return False
        if relation[column].apply(lambda x: isinstance(x, (list, dict, set))).any():
            return False

    return True


def is_2nf(primary_key, dependencies, relation):
    non_prime_attributes = [
        col for col in relation.columns if col not in primary_key]

    for determinant, dependents in dependencies.items():
        if set(determinant).issubset(primary_key) and set(determinant) != set(primary_key):
            if any(attr in non_prime_attributes for attr in dependents):
                return False

    return True


def is_3nf(relations, dependencies):
    primary_keys = [key for key in dependencies]
    non_key_attributes = [item for sublist in dependencies.values()
                          for item in sublist]
    for relation_name, relation in relations.items():
        for det, dep in dependencies.items():
            if set(det).issubset(set(relation.columns)) and not set(det).issubset(primary_keys) and set(dep).issubset(non_key_attributes):
                return False
    return True


def is_bcnf(relations, primary_key, dependencies):
    for relation_name, relation in relations.items():
        all_attributes = set(relation.columns)
        for det, deps in dependencies.items():
            for dep in deps:
                if dep not in det:
                    if all_attributes - closure(det, dependencies):
                        return False

    return True


def is_4nf(relations, mvd_dependencies):
    for relation_name, relation in relations.items():
        for determinant, dependents in mvd_dependencies.items():
            for dependent in dependents:
                if isinstance(determinant, tuple):
                    determinant_cols = list(determinant)
                else:
                    determinant_cols = [determinant]

                if all(col in relation.columns for col in determinant_cols + [dependent]):
                    grouped = relation.groupby(determinant_cols)[
                        dependent].apply(set).reset_index()
                    if len(grouped) < len(relation):
                        print(
                            f"Multi-valued dependency violation: {determinant} ->-> {dependent}")
                        return False

    return True


def is_5nf(relations):
    candidate_keys_dict = {}
    for relation_name, relation in relations.items():
        print(relation)
        user_input = input(
            "Enter the candidate keys for above relation (e.g., (A, B), (C, D)): ")
        print('\n')
        tuples = re.findall(r'\((.*?)\)', user_input)
        candidate_keys = [tuple(map(str.strip, t.split(','))) for t in tuples]
        candidate_keys_dict[relation_name] = candidate_keys

    print(f'Candidate Keys for tables:')
    print(candidate_keys_dict)
    print('\n')

    for relation_name, relation in relations.items():
        candidate_keys = candidate_keys_dict[relation_name]

        data_tuples = [tuple(row) for row in relation.to_numpy()]

        def project(data, attributes):
            return {tuple(row[attr] for attr in attributes) for row in data}

        def is_superkey(attributes):
            for key in candidate_keys:
                if set(key).issubset(attributes):
                    return True
            return False, candidate_keys_dict

        for i in range(1, len(relation.columns)):
            for attrs in combinations(relation.columns, i):
                if is_superkey(attrs):
                    continue

                projected_data = project(data_tuples, attrs)
                complement_attrs = set(relation.columns) - set(attrs)
                complement_data = project(data_tuples, complement_attrs)

                joined_data = {(row1 + row2)
                               for row1 in projected_data for row2 in complement_data}
                if set(data_tuples) != joined_data:
                    print("Failed 5NF check for attributes:", attrs)
                    return False, candidate_keys_dict

    return True, candidate_keys_dict


def first_normalization_form(relation, primary_key):
    relations = {}
    one_flag = is_1nf(relation)

    if one_flag:
        relations[primary_key] = relation
        return relations, one_flag
    else:
        for col in relation.columns:
            if relation[col].apply(is_list_or_set).any():
                relation = relation.explode(col)

        print('RELATION AFTER 1NF')
        print(relation)
        print('\n')
        relations[primary_key] = relation
        return relations, one_flag


def second_normalization_form(relation, primary_key, dependencies):
    # Ensure relation is a DataFrame before passing to is_2nf
    if isinstance(relation, dict):
        relation_df = relation[primary_key]
    else:
        relation_df = relation
    
    two_flag = is_2nf(primary_key, dependencies, relation_df)
    relations = {}
    rm_cols = []

    if two_flag:
        relations[primary_key] = relation_df
        return relations, two_flag  # If already 2NF, return early
    else:
        print('RELATIONS AFTER 2NF')
        print('\n')

        non_prime_attributes = [
            col for col in relation_df.columns if col not in primary_key]
        
        # Decompose based on partial dependencies
        for det, dep in dependencies.items():
            if set(det).issubset(primary_key) and set(det) != set(primary_key):
                if any(attr in dep for attr in non_prime_attributes):
                    new_relation = relation_df[list(det) + dep].drop_duplicates()
                    relations[tuple(det)] = new_relation

                    for attr in dep:
                        if attr not in det and attr not in rm_cols:
                            rm_cols.append(attr)

        # Remove decomposed columns from original relation
        relation_df.drop(columns=rm_cols, inplace=True)
        relations[primary_key] = relation_df

        # Print the new relations after 2NF
        for relation_key in relations:
            print(relations[relation_key])
            print('\n')

        two_flag = True  # Since normalization was done, set flag to True
        return relations, two_flag



def third_normalization_form(relations, primary_key, dependencies):
    three_relations = {}
    three_flag = is_3nf(relations, dependencies)  # Check if already 3NF

    if three_flag:
        return relations, three_flag  # If already 3NF, return early
    else:
        print('RELATIONS AFTER 3NF')
        print('\n')

        # Normalize each relation based on transitive dependencies
        for relation_name, relation in relations.items():
            for det, dep in dependencies.items():
                if set(det).issubset(set(relation.columns)) and not set(dep).issubset(det):
                    new_cols = list(set(det).union(dep))

                    if set(new_cols).issubset(set(relation.columns)) and not set(new_cols) == set(relation.columns):
                        # Create new tables based on the decomposition
                        table1_cols = list(det) + dep
                        table2_cols = list(set(relation.columns) - set(dep))

                        new_table1 = relation[table1_cols].drop_duplicates().reset_index(drop=True)
                        new_table2 = relation[table2_cols].drop_duplicates().reset_index(drop=True)

                        three_relations[tuple(det)] = new_table1
                        three_relations[relation_name] = new_table2
                        break  # Stop processing this relation if a dependency was found
            else:
                three_relations[relation_name] = relation  # Keep relation if no decomposition is needed

        # Print the new relations after 3NF
        for relation_key in three_relations:
            print(three_relations[relation_key])
            print('\n')

        three_flag = True  # Set flag to True since normalization was done
        return three_relations, three_flag

def bc_normalization_form(relations, primary_key, dependencies):
    bcnf_relations = {}
    bcnf_flag = is_bcnf(relations, primary_key, dependencies)

    # If already in BCNF, return the relations and the flag
    if bcnf_flag:
        return relations, bcnf_flag

    print('RELATIONS AFTER BCNF')
    print('\n')

    # Iterate over each relation to check for BCNF violations
    for relation_name, relation in relations.items():
        original_relation = relation.copy()  # Keep a copy for comparison
        changes_made = True  # Flag to track if any changes are made
        
        while changes_made:
            changes_made = False  # Reset for the current loop
            
            # Create a set to track columns already dropped
            dropped_columns = set()
            
            for det, dep in dependencies.items():
                closure_set = closure(set(det), dependencies)

                # Check if the dependency violates BCNF
                if not closure_set.issuperset(relation.columns):
                    cols = list(det) + dep
                    
                    # Create new table with the dependency and the determinant
                    if set(cols).issubset(relation.columns) and not set(cols) == set(relation.columns):
                        new_table = relation[list(det) + dep].drop_duplicates()
                        bcnf_relations[tuple(det)] = new_table

                        # Drop the dependent columns from the original relation
                        relation = relation.drop(columns=dep)
                        dropped_columns.update(dep)  # Track dropped columns
                        changes_made = True  # Mark that a change was made

            # If no changes were made, break the loop
            if not changes_made:
                break

        # Save the remaining attributes in the BCNF relations
        if relation.columns.size > 0:  # Only save if there are columns left
            bcnf_relations[relation_name] = relation

    # Print the relations in BCNF
    for rel in bcnf_relations:
        print(bcnf_relations[rel])
        print('\n')

    # Return the BCNF relations and a flag indicating that changes were made
    return bcnf_relations, not bcnf_flag  # Return True if changes were made

         


def fourth_normalization_form(relations, mvd_dependencies):
    four_relations = {}
    four_flag = is_4nf(relations, mvd_dependencies)

    if four_flag:
        return relations, four_flag
    else:
        print('RELATIONS AFTER 4NF')
        for relation_name, relation in relations.items():
            for determinant, dependents in mvd_dependencies.items():
                for dependent in dependents:
                    if isinstance(determinant, tuple):
                        determinant_cols = list(determinant)
                    else:
                        determinant_cols = [determinant]

                    if all(col in relation.columns for col in determinant_cols + [dependent]):
                        grouped = relation.groupby(determinant_cols)[
                            dependent].apply(set).reset_index()
                        if len(grouped) < len(relation):
                            table_1 = relation[determinant_cols +
                                               [dependent]].drop_duplicates()
                            four_relations[tuple(determinant_cols)] = table_1
                            table_2 = relation[determinant_cols + [col for col in relation.columns if col not in [
                                dependent] + determinant_cols]].drop_duplicates()
                            four_relations[relation_name] = table_2

                            break
                else:
                    continue
                break
            else:
                four_relations[relation_name] = relation

    if len(four_relations) == len(relations):
        return four_relations
    else:
        return fourth_normalization_form(four_relations, mvd_dependencies)


def decompose_5nf(relation_name, dataframe, candidate_keys):
    def project(df, attributes):
        return df[list(attributes)].drop_duplicates().reset_index(drop=True)

    def is_lossless(df, df1, df2):
        common_columns = set(df1.columns) & set(df2.columns)
        if not common_columns:
            return False
        joined_df = pd.merge(df1, df2, how='inner', on=list(common_columns))
        return df.equals(joined_df)

    decomposed_tables = [dataframe]

    for key in candidate_keys:
        new_tables = []
        for table in decomposed_tables:
            if set(key).issubset(set(table.columns)):
                table1 = project(table, key)
                remaining_columns = set(table.columns) - set(key)
                table2 = project(table, remaining_columns | set(key))

                if is_lossless(table, table1, table2):
                    new_tables.extend([table1, table2])
                else:
                    new_tables.append(table)
            else:
                new_tables.append(table)
        decomposed_tables = new_tables

    return decomposed_tables


def fivth_normalization_form(relations, primary_key, dependencies):
    five_relations = {}
    five_flag, candidate_keys_dict = is_5nf(relations)

    if five_flag:
        return relations, five_flag
    else:
        print('RELATIONS AFTER 5NF')
        for relation_name, relation in relations:
            candidate_keys = candidate_keys_dict[relation_name]
            decomposed_relations = decompose_5nf(
                relation_name, relation, candidate_keys)
            five_relations.append(decomposed_relations)

    return five_relations, five_flag
