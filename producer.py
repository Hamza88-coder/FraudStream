from quixstreams import Application
import os
import tqdm
import pandas as pd
import glob
import json
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

app = Application(consumer_group="data_source", auto_create_topics=True)

# Définir le topic à partir de la variable d'environnement
topic_name = os.environ["input_data"]
topic = app.topic(topic_name)

def main():
    """
    Lire des données depuis le dataset et publier chaque ligne dans Kafka
    """

    with app.get_producer() as producer:
        # Récupérer tous les fichiers CSV dans le répertoire spécifié
        files = glob.glob("credit_card/file.csv")
        files.sort()

        for file_path in tqdm.tqdm(files):
            print(f'Processing file: {file_path}')
            data = pd.read_csv(file_path)

            for index, row in data.iterrows():
                # Convertir la ligne en dictionnaire
                data_converted = row.to_dict()
                json_data = json.dumps(data_converted)  # Convertir le dictionnaire en JSON

                # Utiliser l'index de la ligne comme clé
                unique_key = str(index)

                # Publier les données dans le topic
                producer.produce(
                    topic=topic.name,
                    key=unique_key,  # Utiliser l'index comme clé
                    value=json_data,
                )

        print("All rows published")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
