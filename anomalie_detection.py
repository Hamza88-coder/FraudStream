from quixstreams import Application
from dotenv import load_dotenv
import pandas as pd
import os
import joblib
import psycopg2
import numpy as np

load_dotenv()


# Chargement du modèle de Random Forest
model_path = "random_forest_model.pkl"
random_forest_model = joblib.load(model_path)
feature_names = random_forest_model.feature_names_in_

# Définition des topics Kafka
app = Application(
    consumer_group="stock_anomaly_detector",
    auto_offset_reset="earliest",
)

input_topic = app.topic(os.environ['input_data'])
output_topic = app.topic(os.environ['output_topic'])

import pandas as pd

def save_fraud_to_db(fraud_row):
    print("Enregistrement de la fraude en cours...")

    # Connexion à la base de données
    conn = connect_to_db()
    cursor = conn.cursor()

    # Requête d'insertion SQL
    insert_query = """
    INSERT INTO transactions_fraude (
        Time, V1, V2, V3, V4, V5, V6, V7, V8, V9,
        V10, V11, V12, V13, V14, V15, V16, V17,
        V18, V19, V20, V21, V22, V23, V24, V25,
        V26, V27, V28, Amount
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s)
    """

    # Extraire les valeurs en fonction du type de fraud_row
    if isinstance(fraud_row, pd.Series):
        values = tuple(fraud_row[feature_names].values)
    elif isinstance(fraud_row, dict):
        values = tuple(fraud_row.get(col) for col in feature_names)
    else:
        raise ValueError("Le format de fraud_row doit être pd.Series ou dict.")

    

    # Exécuter la requête d'insertion avec les valeurs
    cursor.execute(insert_query, values)

    # Commit et fermeture de la connexion
    conn.commit()
    print("Transaction frauduleuse enregistrée avec succès.")
    cursor.close()
    conn.close()

def connect_to_db():
    """
    Établit une connexion à la base de données PostgreSQL.
    """

    # Utiliser les paramètres de connexion à partir des variables d'environnement
    conn = psycopg2.connect(
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host="localhost",  # Ou l'adresse de votre service PostgreSQL
        port="5432",
    )
    return conn


def fraude_detection(row):
    """
    Cette fonction applique le modèle de Random Forest pour déterminer si la transaction est une fraude.
    """
    # Noms des colonnes attendus par le modèle
    

    # Préparation des caractéristiques pour la prédiction sous forme de DataFrame avec les bons noms de colonnes
    row_df = pd.DataFrame([row], columns=feature_names)

    # Prédiction de fraude
    is_fraud = random_forest_model.predict(row_df)[0]

    # Ajout du résultat dans la ligne de données
    row['is_fraud'] = is_fraud
    print(is_fraud)

    return row


def main():
    print("Démarrage de l'application...")

    # Récupération des données en streaming depuis Kafka
    stream_df = app.dataframe(input_topic)
    

    # Appliquer la fonction de détection de fraude
    stream_df = stream_df.apply(fraude_detection).filter(lambda row: row['is_fraud'] == 1).apply(save_fraud_to_db)
    

    # Démarrer l'application avec le flux
    app.run(stream_df)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        print("Cleanup completed.")