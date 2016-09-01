"""
A few tests to make sure everything is in order.
usage: python pymldb/test.py <your_mldb_host>
"""
import sys
import pandas
import pymldb

host = sys.argv[1]

mldb = pymldb.Connection('http://' + host)

# create a dataset
mldb.put('/v1/datasets/patate', {'type': 'tabular'})
mldb.post('/v1/datasets/patate/rows', {
    'rowName': 'row0',
    'columns': [['x', 1, 0], ['y', 2, 0]]
})
mldb.post('/v1/datasets/patate/rows', {
    'rowName': 'row1',
    'columns': [['x', 2, 0], ['y', 3, 0]]
})
mldb.post('/v1/datasets/patate/commit')

# test mldb.query
# default format (dataframe)
df = pandas.DataFrame.from_records(
    [['row0', 1,2], ['row1', 2,3]],
    columns=['_rowName', 'x', 'y'],
    index='_rowName')
assert mldb.query('select * from patate order by x').equals(df)

# some other format
assert mldb.query('select * from patate order by x',
           format='soa',
           rowNames=False) == {'x': [1,2], 'y': [2,3]}

print('success!')
