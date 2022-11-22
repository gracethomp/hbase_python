import happybase

host = 'localhost'
table_name1 = 'test_table1'
table_name2 = 'test_table2'
row_name = 'row-key'

connection = happybase.Connection(host=host)


def create_table(table_name):
    connection.create_table(
        table_name,
        {'cf1': dict(max_versions=10),
         'cf2': dict(max_versions=1, block_cache_enabled=False)
         }
    )
    print(table_name + ' was created\n')


def put_value(table, name1, value1, table_name):
    table.put('row-key', {name1: value1})
    print('value was added to the table ' + table_name + '\n')


def read_data(table, table_name):
    print(table_name + '. see the values: ')
    for key1, data1 in table.scan():
        print(key1, data1)
    print('\n')


def update_data(table, row1, column, new_value):
    print('data was updated')
    table.put(row1, {column: new_value})


def delete_data(table, row1, column):
    print('data was deleted')
    table.delete(row1, columns=[column])


def delete_table(table_name):
    connection.delete_table(table_name, disable=True)
    print('table ' + table_name + ' was deleted')


create_table(table_name1)
create_table(table_name2)

table1 = connection.table(table_name1)
table2 = connection.table(table_name2)

row = table1.row(row_name)
put_value(table1, b'cf2:title', b'value1', table_name1)
put_value(table2, b'cf1:title', b'value11', table_name2)
put_value(table1, b'cf1:not_title', b'value2', table_name1)
put_value(table2, b'cf2:not_title', b'value22', table_name2)

read_data(table1, table_name1)
read_data(table2, table_name2)

update_data(table1, row_name, 'cf2:title', 's')
read_data(table1, table_name1)

delete_data(table2, row_name, 'cf2:not_title')
read_data(table2, table_name2)

delete_table(table_name1)
delete_table(table_name2)
