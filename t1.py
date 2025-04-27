import pandas as pd
import mysql.connector
from mysql.connector import Error


def extract_projects_from_csv(file_path):
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Extract unique project names from the 'project' column
        unique_projects = df['project'].unique()

        # Create a DataFrame with project information
        project_data = []
        for i, project_name in enumerate(unique_projects):
            project_data.append({
                'id': i + 1,
                'name': project_name,
                'description': f"Apache {project_name} Project",
                'created_date': pd.Timestamp('2025-04-27')  # Using current date from context
            })

        return pd.DataFrame(project_data)

    except Exception as e:
        print(f"Error extracting projects: {e}")
        return None


def insert_projects_to_mysql(projects_df, db_config):
    try:
        # Establish connection
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            cursor = connection.cursor()

            # Create projects table if it doesn't exist
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS projects
                           (
                               id
                               INT
                               AUTO_INCREMENT
                               PRIMARY
                               KEY,
                               name
                               VARCHAR
                           (
                               255
                           ) NOT NULL,
                               description TEXT,
                               created_date DATETIME
                               )
                           """)

            # Insert projects
            for _, row in projects_df.iterrows():
                query = """
                        INSERT INTO projects (name, description, created_date)
                        VALUES (%s, %s, %s) \
                        """
                cursor.execute(query, (row['name'], row['description'], row['created_date']))

            connection.commit()
            print(f"Successfully inserted {len(projects_df)} projects into the database")

    except Error as e:
        print(f"Error connecting to MySQL: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")


def main():
    # File path to the CSV
    file_path = './data/apache_sprint_issues.csv'

    # Database configuration
    db_config = {
        'host': 'localhost',
        'database': 'sprint_iq',
        'user': 'root',
        'password': 'password'
    }

    # Extract projects
    projects_df = extract_projects_from_csv(file_path)

    if projects_df is not None and not projects_df.empty:
        print(f"Extracted {len(projects_df)} unique projects:")
        print(projects_df[['name', 'description']])

        # Insert into MySQL
        insert_projects_to_mysql(projects_df, db_config)
    else:
        print("No projects extracted or error occurred")


if __name__ == "__main__":
    main()
