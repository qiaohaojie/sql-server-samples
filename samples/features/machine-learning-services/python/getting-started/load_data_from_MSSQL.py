'''
    This is the code example to load data from MSSQL Server
'''

import pandas as pd
import revoscalepy as revoscale

conn_str = 'Driver=SQL Server;Server=.;Database=tpcxbb_1gb;Trusted_Connection=True;'
input_query = '''
    select top 1 
        1 as id,
        'A' as Name
    from sys.tables
    '''

# Define the columns we wish to import.
column_info = {
    "id": {"type": "integer"},
    "Name": {"type": "character"}
}

data_source = revoscale.RxSqlServerData(
    sql_query=input_query,
    column_info=column_info,
    connection_string=conn_str
)

# import data source and convert to pandas dataframe.
imported_data = pd.DataFrame(revoscale.rx_import(data_source))
print("Data frame:")
print(imported_data.head(n=10))