import pytest

from table import Table as Table
from tests import data


@pytest.fixture()
def input_table(request):
    return request.param


@pytest.fixture()
def expected_output_table(request):
    return request.param


@pytest.mark.parametrize(
    "input_table, expected_output_table", zip(data.table_list, data.expected_table_data_output)
)
def test_table_init(input_table, expected_output_table):
    tbl = input_table
    assert isinstance(tbl, Table)
    assert isinstance(tbl.fields_initializer, expected_output_table['fld_init_type'])
    assert tbl.num_rows == expected_output_table['num_rows']
    assert tbl.col_names == expected_output_table['col_names']


def test_table_init_exception():
    error_parttern = r"Must have at least 1 column_name.*"
    with pytest.raises(ValueError, match=error_parttern):
        Table(memory=False)
