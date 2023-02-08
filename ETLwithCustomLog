"""
In this script, we keep track of the number of rows processed and log the count along with each row. 
Additionally, we log a message at the end of the ETL process indicating the total number of rows processed successfully. 
This information can be used for monitoring the progress and performance of the ETL process.
"""

import logging
import csv
import pyodbc

# set up custom logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('etl_process.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def etl_process():
    try:
        # establish a connection to Azure SQL database
        connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                    'SERVER=<server_name>;'
                                    'DATABASE=<database_name>;'
                                    'UID=<username>;'
                                    'PWD=<password>')
        cursor = connection.cursor()

        # read data from CSV file
        row_count = 0
        with open('data.csv', 'r') as file:
            reader = csv.reader(file)
            header = next(reader)
            for row in reader:
                # insert data into Azure SQL database
                cursor.execute('INSERT INTO data_table ({}) VALUES ({})'.format(
                    ', '.join(header),
                    ', '.join(['?' for i in range(len(header))])
                ), row)
                connection.commit()
                row_count += 1
                logger.info('Inserted row {}: {}'.format(row_count, row))

        # close the database connection
        connection.close()
        logger.info('ETL process completed successfully with {} rows processed.'.format(row_count))
    except Exception as e:
        logger.error('An error occurred while processing: {}'.format(e))

if __name__ == '__main__':
    etl_process()
