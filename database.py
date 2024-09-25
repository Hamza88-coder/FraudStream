import psycopg2

# Configuration de la connexion à la base de données
db_config = {
    'dbname': 'mydb',
    'user': 'user',
    'password': 'password',
    'host': 'localhost',  # ou l'adresse IP de votre serveur PostgreSQL
    'port': 5432          # Port par défaut pour PostgreSQL
}

def create_table():
    # Établir la connexion à la base de données
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # Script de création de la table
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS transactions_fraude (
            id SERIAL PRIMARY KEY,
            Time NUMERIC,
            V1 NUMERIC,
            V2 NUMERIC,
            V3 NUMERIC,
            V4 NUMERIC,
            V5 NUMERIC,
            V6 NUMERIC,
            V7 NUMERIC,
            V8 NUMERIC,
            V9 NUMERIC,
            V10 NUMERIC,
            V11 NUMERIC,
            V12 NUMERIC,
            V13 NUMERIC,
            V14 NUMERIC,
            V15 NUMERIC,
            V16 NUMERIC,
            V17 NUMERIC,
            V18 NUMERIC,
            V19 NUMERIC,
            V20 NUMERIC,
            V21 NUMERIC,
            V22 NUMERIC,
            V23 NUMERIC,
            V24 NUMERIC,
            V25 NUMERIC,
            V26 NUMERIC,
            V27 NUMERIC,
            V28 NUMERIC,
            Amount NUMERIC
        );
        '''
        
        # Exécuter la requête de création de la table
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'transactions_fraude' créée avec succès.")

    except Exception as e:
        print(f"Erreur lors de la création de la table : {e}")
    finally:
        # Fermer le curseur et la connexion
        cursor.close()
        connection.close()

if __name__ == "__main__":
    create_table()
