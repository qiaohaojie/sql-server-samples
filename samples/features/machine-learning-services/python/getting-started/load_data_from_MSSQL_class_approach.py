'''
    This is the code example to load data from MSSQL Server with class apprach
'''

import pandas as pd
import revoscalepy as revoscale


class DataSource():

    connectionstring = 'Driver=SQL Server;Server=.;Database=tpcxbb_1gb;Trusted_Connection=True;'

    # def __int__(self, connectionstring):
    #     '''
    #
    #     :param connectionstring: connection string to the SQL Server
    #     :return:
    #     '''
    #     self.connectionstring = connectionstring

    def loaddata(self, connectionstring=connectionstring):
        dataSource = revoscale.RxSqlServerData(
            sql_query = "select top 1 1 as id, 'A' as Name from sys.tables",
            # verbose = True,
            # reportProgress = True,
            connection_string = connectionstring
        )

        self.__computeContext = revoscale.RxInSqlServer(
            connection_string=connectionstring,
            auto_cleanup=True
        )

        data = revoscale.rx_import(dataSource)

        return data

    def getComputeContext(self):
        if self.__computeContext is None:
            raise RuntimeError("Data must be loaded before requesting computecontext!")

        return self.__computeContext




conn_str = 'Driver=SQL Server;Server=.;Database=tpcxbb_1gb;Trusted_Connection=True;'
ds = DataSource()
print(ds.connectionstring)
data = ds.loaddata()
# cc = ds.getComputeContext()

# print(type(data ))
# print(cc)
print(data.head())



