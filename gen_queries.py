import argparse
from faker import Faker
import psycopg2

# Set up the Faker library
fake = Faker()

# Set up the database connection
conn = psycopg2.connect(database="<db>", user="<user>", password="<password>", host="<host>", port="<port>")
cur = conn.cursor()

# Set up the command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--num-queries", type=int, default=10, help="The number of queries to generate")
parser.add_argument("--insert-ratio", type=float, default=0.5, help="The ratio of insert queries to total queries")
parser.add_argument("--tables", type=str, required=True, help="A comma-separated list of table names to generate data for")
parser.add_argument("--output-file", type=str, default=None, help="The name of the file to write the queries to")
args = parser.parse_args()

# Split the table names into a list
table_names = [name.strip() for name in args.tables.split(",")]

# Generate data for each table
queries = []
for table_name in table_names:
    # Get the schema for the table
    cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
    schema = cur.fetchall()

    # Generate the queries
    for i in range(args.num_queries):
        if i < args.num_queries * args.insert_ratio:
            # Generate an insert query
            values = []
            for column in schema:
                column_name, data_type = column
                # Generate a fake value based on the data type
                if data_type == "integer":
                    value = fake.random_int()
                elif data_type == "text":
                    value = "'"+fake.text()+"'"
                    value=value.replace('\n','')
                elif data_type == "timestamp without time zone":
                    value = fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S')
                else:
                    # Pass for unsupported data types
                    pass
                values.append(value)
            query = f"INSERT INTO {table_name} ({','.join([column[0] for column in schema])}) VALUES ({','.join(['%s' for _ in range(len(schema))])})"
        else:
            # Generate an update query
            primary_key_column = schema[0][0]  # Assumes first column is primary key
            set_clauses = []
            values = []
            for column in schema[1:]:
                column_name, data_type = column
                # Generate a fake value based on the data type
                if data_type == "integer":
                    value = fake.random_int()
                elif data_type == "text":
                    value = "'"+fake.text()+"'"
                    value=value.replace('\n','')
                elif data_type == "timestamp without time zone":
                    value = fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S')
                else:
                    # Pass for unsupported data types
                    pass
                set_clauses.append(f"{column_name}=%s")
                values.append(value)
            values.append(fake.random_int())  # Set the primary key value for the WHERE clause
            query = f"UPDATE {table_name} SET {','.join(set_clauses)} WHERE {primary_key_column}=%s"

        # Append the query to the queries list
        queries.append(query % tuple(values))

# Write the queries to a file, if specified
if args.output_file:
    with open(args.output_file, "w") as f:
        f.write("\n".join(queries))
