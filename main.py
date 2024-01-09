# ______________ Libraries
import psycopg2
from psycopg2 import sql
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# ______________ Connect to database
def connect_to_postgres(host, database, user, password, port):
    try:
        # Establish a connection to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        return connection
    except Exception as e:
        print("Error: Unable to connect to the database. {}".format(e))
        return None

# ______________ Get table column names
def get_column_names(connection, table_name):
    cursor = connection.cursor()

    # Fetch column names
    query = sql.SQL("""SELECT tx.*
                    FROM {} tx
                    LIMIT 0;""".format(table_name))
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]

    cursor.close()
    return column_names

# ______________ Get row from DataBase
def retrieve_row(col_name, table_name, sensor_id, connection):
    try:
        cursor = connection.cursor()

        # Query to retrieve a row based on a condition
        query = sql.SQL("""SELECT tx.{} FROM {} tx WHERE tx.sensor_id = '{}'""".format(col_name, table_name, sensor_id))
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()
        return row
    except Exception as e:
        print("Error row: {}".format(e))
        return None

# ______________ Read excel file
def read_excel_file(file_path):
    try:
        # Read the Excel file into a DataFrame
        df = pd.read_excel(file_path)
        print("Leido archivo '{}'".format(file_path))
        return df
    except Exception as e:
        print("Error: Unable to read the Excel file. {}".format(e))

# ______________ Useless function to add tabs and create a row for some sort of table
def add_tabs(string_to_add):
    len_str = len(str(string_to_add).strip())
    num_spc = 34

    if len_str < num_spc:
        return "{}{}".format(str(string_to_add).strip(), " " * (num_spc-len_str))
    else:
        return "{} ".format(str(string_to_add).strip())

# ______________ Save table file to CSV
def save_table_csv(table_name, connection, file_path):
    try:
        # Select all data from the specified table
        query = """SELECT * FROM {}""".format(table_name)

        # Use pandas to read the SQL query result into a DataFrame
        df = pd.read_sql_query(query, connection)

        # Save the DataFrame to a CSV file
        df.to_csv(file_path, index=False)
        print("DataFrame saved to CSV file:", file_path)
    except Exception as e:
        print("Error: Unable to save data. {}".format(e))

# ______________ Update value on table
def update_table(connection, table_name, column_name, new_value, condition_value, data_type):
    try:
        # Convert values to the expected types if necessary
        upd_statement = ""
        if data_type == "str":
            upd_statement = "UPDATE {} SET {} = '{}' WHERE sensor_id = '{}';".format(table_name, column_name, new_value, condition_value)
        elif data_type == "num":
            upd_statement = "UPDATE {} SET {} = {} WHERE sensor_id = '{}';".format(table_name, column_name, new_value, condition_value)
        else:
            upd_statement = "UPDATE {} SET {} = null WHERE sensor_id = '{}';".format(table_name, column_name, new_value, condition_value)

        cursor = connection.cursor()
        cursor.execute(upd_statement)
        connection.commit()
        cursor.close()

        print("Actualizado, {} fila(s)".format(cursor.rowcount))
    except Exception as e:
        print(f"Error: Unable to update table {table_name}. {e}")

# ______________ Main
if __name__ == "__main__":
    try:
        print("-" * 90)
        print("""Script para comparar datos extraídos de un archivo de excel con datos provenientes de una base de datos.

La hoja de cálculo es resultado de una consulta a la base de datos, por lo que tiene columnas pertenecientes a la tabla de interés. Sus datos han sido actualizados y verificados por personal especializado. 

En la base de datos, se deben ver reflejados los datos del archivo excel. Por ser un número grande, creé este script para comparar los datos entre las dos fuentes, mostrar aquellos diferentes y actualizar aquellos el usuario requiera.

El script, primero guardará un respaldo en csv de los datos de la base de datos.

Creado por DarwioDev""")
        print("-" * 90)

        # DB connection parameters
        load_dotenv()
        host = os.environ['DB_HOST']
        port = os.environ['DB_PORT']
        db = os.environ['DB_NAME']
        user = os.environ['DB_USER']
        password = os.environ['DB_PASSWORD']

        # Call the function to connect to the PostgreSQL database
        db_connection = connect_to_postgres(host, db, user, password, port)
        # Create a SQLAlchemy engine
        engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(user, password, host, port, db))

        if db_connection and engine:
            # Get DB table column names
            table_name = os.environ['BD_TABLE_UPD_NAME']
            column_names = get_column_names(db_connection, table_name)

            # Save data of table to csv
            save_table_csv(table_name, engine, "{}_resp.csv".format(table_name))

            # Call the function to read and print the Excel file
            my_df = read_excel_file("datos_sensores_eolico-revisar.xlsx")
            # Wait for user input
            print("-" * 90)
            user_input = input("\n[ INFO ] ¿Iniciar lectura de archivo excel desde alguna linea? \nEscriba un número ó ignore: ").strip()

            # Check user input
            start = 0
            if len(user_input) > 0:
                if isinstance(user_input, int):
                    if int(user_input) > 0:
                        start = int(user_input)

            # Comparar data del archivo excel con data de la base
            for row in range(start, len(my_df)):
                # Obtener código de torre
                curr_sensorid = my_df.at[row ,'sensor_id']
                # Print info of row to compare
                print("")
                print("=" * 30)
                print("Excel row  #{}".format(row + 1))
                print("Sensor: {}".format(curr_sensorid))
                print("=" * 30)

                # Print rows that has different values
                print("-" * 90)
                print("{} {} Excel".format(add_tabs("Columna"), add_tabs("BaseDatos")))
                print("-" * 90)

                # Usar las columnas de interes para comparar
                cols_upd = []
                for col in column_names:
                    resp = retrieve_row(col, table_name, curr_sensorid, db_connection)
                    ps_resp = pd.Series(resp)

                    # Comprobar si son datos numericos
                    if ps_resp[0] != my_df.at[row ,col]:
                        # Comprobar si son strings
                        if str(ps_resp[0]).strip() != str(my_df.at[row ,col]).strip():
                            # Comprobar si son nulls
                            # if ps_resp[0] != None and my_df.at[row ,col] != np.nan:
                            print("{} {} {}".format(add_tabs(col), add_tabs(ps_resp[0]), my_df.at[row ,col]))
                            cols_upd.append({col: my_df.at[row ,col]})
                
                # Wait for user input
                print("-" * 90)
                user_input = input("[ INFO ] Presione ( A ) para actualizar campos, otra tecla para ignorar: ").upper().strip()

                # Check user input
                if user_input == 'A':
                    # Actualizar                     
                    for obj in cols_upd:
                        for key, value in obj.items():
                            user_input = input("""[ UPDATE ] Actualizar '{}' con '{}' ? ( Y ) Yes ( N ) No ( S ) Siguiente fila: """.format(key, value)).upper().strip()
                            if (user_input == 'Y'):
                                # Valor null a actualizar
                                if str(value) != 'None' and str(value) != 'nan' and str(value) != 'NaT':
                                    # Valor numerico a actualizar
                                    if isinstance(value, (int, float)):
                                        update_table(db_connection, table_name, key, value, curr_sensorid, 'num')
                                    else:
                                        # Valor tipo cadena
                                        update_table(db_connection, table_name, key, value, curr_sensorid, 'str')
                                else:
                                    update_table(db_connection, table_name, key, value, curr_sensorid, 'null')
                            elif (user_input == 'S'):
                                break
        else:
            print("[ ERROR ] No se pudo conectar a la base de datos")
    except Exception as e:
        print("Error: {}".format(e))
    finally:
        # Close the connection outside the try block
        db_connection.close()