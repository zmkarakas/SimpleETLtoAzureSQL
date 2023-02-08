import csv
import pyodbc
import logging

# Set up logging to log any errors or warnings that occur during the ETL process
logging.basicConfig(filename='etl.log', level=logging.WARNING, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def transform_data(data):
    """Clean and transform the data"""
    transformed_data = []
    for row in data:
        # Remove leading and trailing whitespaces
        row = [item.strip() for item in row]

        # Convert date columns to the correct format
        date_columns = [0, 2] # index of date columns
        for index in date_columns:
            try:
                row[index] = datetime.strptime(row[index], '%m/%d/%Y').strftime('%Y-%m-%d')
            except ValueError:
                row[index] = None

        # Replace missing values with default values
        default_values = {1: 0, 3: 'Unknown'} # index and default value of columns with missing values
        for index, value in default_values.items():
            if row[index] == '':
                row[index] = value

        # Validate the data to ensure it meets certain constraints
        validation_errors = []
        if row[1] < 0:
            validation_errors.append(f"Value in column 2 must be greater than or equal to 0, but got {row[1]}")
        if row[3] not in ['Male', 'Female', 'Unknown']:
            validation_errors.append(f"Value in column 4 must be one of 'Male', 'Female', or 'Unknown', but got {row[3]}")
        if validation_errors:
            logging.warning("Validation errors occurred while processing row: " + str(row) + "\n" + "\n".join(validation_errors))
            continue # skip this row and move on to the next one

        transformed_data.append(row)
    return transformed_data

# Open the CSV file and read the data 
with open('data.csv', 'r') as file:
    reader = csv.reader(file)
    header = next(reader) # skip the header row
    data = [row for row in reader]

transformed_data = transform_data(data)

# Connect to the Azure SQL database (can use ODBC for other databases)
connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=<server_name>;DATABASE=<database_name>;UID=<username>;PWD=<password>')
cursor = connection.cursor()

# Insert the data into the database
for row in transformed_data:
    cursor.execute("INSERT INTO table_name (column1, column2, column3, column4) VALUES (?, ?, ?, ?)", row[0], row[1], row[2], row[3])

# Commit the changes and close the connection
connection.commit()
connection.close()
