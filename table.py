"""
This module contains class Table
"""
import re
import random
import sqlite3
from sqlite3 import Error
from typing import List, Union, Optional, Any


class Row:
    def __init__(self, parent: 'Table',  # https://www.python.org/dev/peps/pep-0484/#forward-references
                 values: List[Any]) -> None:
        self._parent = parent
        self.values = values
        self.col_names = parent.col_names
        self._col_names_len = len(parent.col_names)
        self._values_len = len(values)
        if self._col_names_len != self._values_len:
            raise ValueError(f"Difference in length between values and column's names. \n"
                             f"Values: {self._values_len}, Columns_name_len: {self._col_names_len}")

    @property
    def parent(self):
        return self._parent

    def __len__(self):
        return self._values_len

    def __iter__(self):
        yield from self.values

    def __str__(self):  # some thoughts about return SUPER LONG ROWS of size 100000
        return f'<Row {dict(zip(self.col_names, self.values))}>'

    def __repr__(self):
        return str(self)

    def __getattr__(self, item):  # called it Dynamic Look-up !
        r = re.compile('^_\\d+$')
        if r.match(item) is not None:
            return super().__getattribute__('values')[int(item.replace("_", ""))]
        else:
            if item in super().__getattribute__('col_names'):
                return super().__getattribute__('values')[super().__getattribute__('col_names').index(item)]
            else:
                raise AttributeError(f"Col/Index `{item}` is not existed or incorrectly spelt.")

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            return self.values[item]
        if isinstance(item, str):
            return self.values[self.col_names.index(item)]


class Col:
    def __init__(self, parent: 'Table', col_name: Union[str, int]) -> None:
        self._parent = parent
        self.col_name = col_name
        # self.value_per_column = [row[col_name] for row in parent]
        # b/c its in init ... generator compre/list compre../lazily...
        # col obj created, this list also created...
        # cant use parent[col_name] since this will cause RecursionError,
        # Reason:
        # parent[col_name] is slicing and Col class gets called in __getitem__ of parent class,
        # but parent also get called here ! --> infinite loop, calling each other back n forth  !

    @property
    def parent(self):
        return self._parent

    def __len__(self):
        return len(self.value_per_column)

    def __str__(self):
        return f'<Col {self.col_name} {self.value_per_column}>'

    def __repr__(self):
        return str(self)

    def __iter__(self):
        # for value in self.value_per_column:
        #     yield value
        for row in self.parent:
            yield row[self.col_name]

    def __getitem__(self, item):
        # TODO: CHANGE THIS
        if isinstance(item, (int, slice)):
            return self.value_per_column[item]

    def __getattr__(self, item):
        # TODO: CHANGE THIS
        r = re.compile('^_\\d+$')
        if r.match(item) is not None:
            return super().__getattribute__('value_per_column')[int(item.replace("_", ""))]
        else:
            raise AttributeError(f"Index `{item}` is not existed or incorrectly spelt.")


class Table:
    _tmp_table_list: List[str] = []

    @classmethod
    def _random_table_name(cls) -> str:
        """Generate table name and store to list.

        :param table_list List[str]: store the table list from all instances
        :return: table_name
        :rtype: str
        """
        table_name = f"tmp_table_{random.randint(1_000_000, 10_000_000 - 1)}"
        while table_name in cls._tmp_table_list:
            table_name = f"tmp_table_{random.randint(1_000_000, 10_000_000 - 1)}"
        return table_name

    @staticmethod
    def _sanitize_inputs(input_value: Union[str, int]) -> Union[str, int]:
        """Remove undesired escape characters from inputs to avoid sql-injection.

        :param input str: the input to sanitize
        :return: the sanitized input
        :rtype: str
        """
        return re.sub(r"""[`'";]""", "", input_value) if isinstance(input_value, str) else input_value

    def _fetch_data_from_db_to_row(self):
        # self._row_list = []
        sql = f"""
            SELECT {",".join(cl for cl in self.col_names)} FROM {self._tmp_table_name}
        """

        for row in self._execute_sql(sql):
            self._row_list.append(Row(parent=self, values=row))

    def _execute_sql(self, sql):
        c = None
        try:
            c = self._conn.cursor()
            c.execute(sql)
            self._conn.commit()
            return c.fetchall()
        except Error as e:
            raise e
        finally:
            if c:
                c.close()

    def _create_empty_table(self):
        num_cols = len(self.col_names)
        cols_initializer = \
            ",".join(f'`{self.col_names[i]}` VARCHAR(255) DEFAULT "{self.fields_initializer}"'
                     for i in range(num_cols))

        sql = f"""
            CREATE TABLE `{self._tmp_table_name}` (
                {cols_initializer}
            )
        """
        self._execute_sql(sql)

        if self.num_rows > 0:
            values_initializer = ",".join(f'"{self.fields_initializer}"' for _ in range(num_cols))
            sql = f"""
                INSERT INTO `{self._tmp_table_name}` VALUES({values_initializer})
            """

            for _ in range(self.num_rows):
                self._execute_sql(sql)

        Table._tmp_table_list.append(self._tmp_table_name)
        self._fetch_data_from_db_to_row()

    def __init__(self,
                 fields_initializer: Union[str, int] = None,
                 num_rows: int = 0,
                 col_names: List[Optional[str]] = None,
                 memory: bool = True) -> None:

        self._is_initialized = False
        if isinstance(fields_initializer, str):
            self.fields_initializer = self._sanitize_inputs(fields_initializer)
        elif isinstance(fields_initializer, int):
            self.fields_initializer = fields_initializer
        else:
            self.fields_initializer = "NULL"

        if col_names:
            self.col_names = [self._sanitize_inputs(col) if col else f"no_name_{idx}"
                              for idx, col in enumerate(col_names)]
        else:
            raise ValueError('Must have at least 1 column_name given to initialize table placeholders.')

        self._tmp_table_name = self._sanitize_inputs(self._random_table_name())
        self.num_rows = num_rows
        self._row_list: List[Row] = []

        if memory:
            self._conn = sqlite3.connect(":memory:")  # should it be faster retrieval using memory ?
        else:
            self._conn = sqlite3.connect("table_data/tableDB.db")

        self._create_empty_table()
        self._is_initialized = True

    def __del__(self):
        if not self._is_initialized:
            return  # EARLY RETURN ...
        try:
            sql = f"""
                DROP TABLE `{self._tmp_table_name}`
            """
            self._execute_sql(sql)
            Table._tmp_table_list.remove(self._tmp_table_name)
            print(f"Table `{self._tmp_table_name}` got deleted.")
        except NotImplementedError:
            print(f"something wrong.")
        finally:
            self._conn.close()

    def __iter__(self):
        return self.rows()

    def __str__(self):
        return f"< Row:\n{self._row_list} >"

    def __repr__(self):
        return str(self)

    def __len__(self):
        return self.num_rows

    def __getitem__(self,
                    item: Union[int, str, slice, tuple]
                    ) -> Union[List[Col], List[Row], Col, Row]:
        if isinstance(item, int):
            return self._row_list[item]

        if isinstance(item, str):
            return Col(parent=self, col_name=item)

        if isinstance(item, slice):
            try:
                start, stop, step = item.indices(len(self))
                return [self._row_list[i] for i in range(start, stop, step)]
            except TypeError:
                pass
            start_idx = 0 if not item.start else self.col_names.index(item.start)
            stop_idx = len(self.col_names) if not item.stop else self.col_names.index(item.stop)
            step = 1 if not item.step else item.step
            return [Col(parent=self, col_name=self.col_names[i]) for i in range(start_idx, stop_idx, step)]

        if isinstance(item, tuple):
            # tuple of str, tuple of int
            values_to_return: Union[List[Row], List[Col]] = []
            # if empty tuple return empty array
            # else
            for i in item:
                if isinstance(i, int):
                    # list comprehension here
                    values_to_return.append(self._row_list[i])
                if isinstance(i, str):
                    # list comprehension here
                    values_to_return.append(Col(parent=self, col_name=i))
            return values_to_return

    def __getattr__(self, item):
        r = re.compile('^_\\d+$')
        if r.match(item) is not None:
            return super().__getattribute__('_row_list')[int(item.replace("_", ""))]
        else:
            if item in super().__getattribute__('col_names'):
                return Col(parent=self, col_name=item)
        raise AttributeError(f"Row/Column `{item}` is not existed or incorrectly spelt.")

    def rows(self):
        for r in self._row_list:
            yield r

    def cols(self):
        for n in self.col_names:
            yield Col(parent=self, col_name=n)

    def add_row(self, row: List[Any] = None, default=None):
        num_cols = len(self.col_names)
        if row:
            if default:
                raise ValueError("Either row or default is set, cannot set both.")
            if len(row) != num_cols:
                raise ValueError(f"Number of values must match the number of fields,"
                                 f" which is {num_cols}")
            cell_values = ",".join(f"'{self._sanitize_inputs(v)}'" for v in row)
        else:
            if default:
                cell_values = ",".join(f"'{self._sanitize_inputs(default)}'" for _ in range(num_cols))
            else:
                cell_values = ",".join("NULL" for _ in range(num_cols))
        sql = f"""
            INSERT INTO `{self._tmp_table_name}` VALUES({cell_values})
            """
        self._execute_sql(sql)
        self.num_rows += 1
        self._fetch_data_from_db_to_row()

    def add_col(self, col: List[Any] = None, col_name: str = None, default: Union[int, str] = None):
        if col:
            if default:
                raise ValueError("Either col or default is set, cannot set both.")
            if len(col) != self.num_rows:
                raise ValueError(f"Number of col values must match the number of rows,"
                                 f" which is {self.num_rows}.")
            cell_values = col
        else:
            cell_values = []
            if default:
                cell_value = f"'{default}'"
            else:
                cell_value = "NULL"
            for _ in range(self.num_rows):
                cell_values.append(cell_value)
        if col_name:
            c_name = col_name
        else:
            num_cols = len(self.col_names)
            c_name = f"col_name_{num_cols+1}"

        sql = f"""ALTER TABLE `{self._tmp_table_name}` ADD COLUMN {self._sanitize_inputs(c_name)} VARCHAR(255)"""
        self._execute_sql(sql)
        self.col_names.append(c_name)

        for i in range(self.num_rows):
            sql = f"""
                UPDATE `{self._tmp_table_name}`
                SET {self._sanitize_inputs(c_name)} = '{self._sanitize_inputs(cell_values[i])}'
                WHERE rowid = {i+1}
            """
            self._execute_sql(sql)
        self._fetch_data_from_db_to_row()
