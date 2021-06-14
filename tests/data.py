from table import Table as Table

table_list = [
    Table(fields_initializer='test', num_rows=1, col_names=['c1', 'c2'], memory=False),
    Table(fields_initializer='test_1', num_rows=1, col_names=['c1', 'c2'], memory=True),
    Table(fields_initializer='test_2', num_rows=2, col_names=['c1'], memory=False),
    Table(fields_initializer=1, col_names=['ic1','ic2'], memory=False),
    Table(col_names=['ic1','ic2'], memory=False)
]

expected_table_data_output = [
    {'fld_init_type': str, 'num_rows': 1, 'col_names': ['c1', 'c2']},
    {'fld_init_type': str, 'num_rows': 1, 'col_names': ['c1', 'c2']},
    {'fld_init_type': str, 'num_rows': 2, 'col_names': ['c1']},
    {'fld_init_type': int, 'num_rows': 0, 'col_names': ['ic1','ic2']},
    {'fld_init_type': str, 'num_rows': 0, 'col_names': ['ic1','ic2']}
]


# invalid_table_list = [
#     Table(memory=False)  # INVALID OBJECT cannot be created... --> Cause Fixture to Fail !
# ]

