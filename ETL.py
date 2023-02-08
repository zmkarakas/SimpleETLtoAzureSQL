import csv
import pyodbc

# Open the CSV file and read the data
with open('data.csv', 'r') as file:
    reader = csv.reader(file)
    header = next(reader) # skip the header row
    data = [row for row in reader]

# Clean and transform the data
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

    transformed_data.append(row)

# Connect to the Azure SQL database
connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=<server_name>;DATABASE=<database_name>;UID=<username>;PWD=<password>')
cursor = connection.cursor()

# Insert the data into the database
for row in transformed_data:
    cursor.execute("INSERT INTO table_name (column1, column2, column3, column4) VALUES (?, ?, ?, ?)", row[0], row[1], row[2], row[3])

# Commit the changes and close the connection
connection.commit()
connection.close()
