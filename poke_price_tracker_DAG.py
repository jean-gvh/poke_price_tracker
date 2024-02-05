# Libraries for Web Scraping
from bs4 import BeautifulSoup
import requests
import logging

# Libraries for Data Processing
import pandas as pd 
from unidecode import unidecode
from datetime import datetime, timedelta
from io import StringIO
from google.cloud import storage 

# Import Airflow
import airflow
from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.providers.mysql.operators.mysql import MySqlOperator
from google.cloud.sql.connector import Connector


import sqlalchemy 

# VARIABLES GLOBALES
pokemon_name_list = """
    pingoleon+v+146%2F163,
    dracaufeu+ex+199%2F165,
    tyranocif+v+155%2F163,
    zekrom+tg05%2Ftg30,
    zarbi+v+177%2F195, 
    moufflair+v+181%2F195,
    regidrago+v+184%2F195,
    lugia+v+186%2F195,
    majaspic+v+tg13%2Ftg30,
    brasegali+vmax+tg15%2Ftg30,
    gardevoir+tg05%2Ftg30,
    pikachu+160%2F159, 
    suicune+v+gg38%2Fgg70,
    zeraora+vmax+gg42%2Fgg70,
    mewtwo+vstar+gg44%2Fgg70,
    darkrai+vstar+gg50%2Fgg70,
    motisma+v+177%2F196,
    ptéra+v+180%2F196,
    giratina+v+186%2F196,
    lugulabre+tg04%2Ftg30'
"""
AIRFLOW_VAR_POKE_LIST_VAR=pokemon_name_list


# Date du jour
date_today = datetime.now().strftime('%Y-%m-%d') 

# Informations de connexion pour GCP
client = storage.Client()
bucket_name = "bucket-temp-data" 
bucket_base_data ="bucket-base-data1"

# Définir les connexions à GCS et MySQL dans Airflow UI sous "Admin -> Connections"
gcs_conn_id = 'jean_guin_bucket'
mysql_conn_id = 'mysql_db_ebay_con' 




# FONCTIONS/Tâches

# Créer la connexion avec la page web
# Retourne une liste

def get_data():
    soup_list = []
    cards_list = [] 

    pokemon_name_list_var= Variable.get("AIRFLOW_VAR_POKE_LIST_VAR")
    pokemon_name_list = pokemon_name_list_var.split(",")


    for pokemon_name in pokemon_name_list:
        base_url = f'https://www.ebay.fr/sch/i.html?_from=R40&_nkw={pokemon_name}&_sacat=0&LH_Sold=1&LH_Complete=1&rt=nc&LH_PrefLoc=1&_ipg=100'
        
        # Récupérer le contenu de la première page
        r = requests.get(base_url)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            soup_list.append(soup)

            # Vérifier s'il y a plus d'une page
            pagination_items = soup.find('ol', {'class': 'pagination__items'})
            if pagination_items:
                # Récupérer le nombre total de pages
                num_pages = len(pagination_items.find_all('li'))

                # Récupérer les données des pages suivantes
                for page in range(2, num_pages + 1):
                    next_url = f'{base_url}&_pg={page}'
                    next_r = requests.get(next_url)
                    if next_r.status_code == 200:
                        next_soup = BeautifulSoup(next_r.text, 'html.parser')
                        soup_list.append(next_soup)
                    else:
                        print(f"La requête pour {pokemon_name} (page {page}) a échoué avec le code d'état {next_r.status_code}")
        else:
            print(f"La requête pour {pokemon_name} a échoué avec le code d'état {r.status_code}")
    

    for soup in soup_list:
        results = soup.find_all('div', {'class': "s-item__info clearfix"})
        for item in results:
            title_span = item.find('div', {'class': 's-item__title'})
            
            card_name = title_span.text if title_span else None
            
            # Trouver le span parent avec la classe 's-item__title--tag'
            title_tag_span = item.find('div', {'class': 's-item__title--tag'})
            
            # Vérifier si l'élément est trouvé avant d'essayer d'accéder à ses attributs
            if title_tag_span:
                # Trouver le span imbriqué avec la classe 'POSITIVE'
                sold_date_span = title_tag_span.find('span', {'class': 'POSITIVE'})
                sold_date = sold_date_span.text.replace('Vendu le', '') if sold_date_span else None
            else:
                sold_date = None
            
            # Initialiser location avec None avant la condition
            location = None

            detail_tag_div = item.find('div', {'class': 's-item__details clearfix'})
            
            if detail_tag_div:
                primary_location_tag_span = item.find('div', {'class': 's-item__detail s-item__detail--primary'})
                
                

                if primary_location_tag_span:
                    location_tag_span1 = item.find('span', {'class': 's-item__location s-item__itemLocation'})
                    
                    

                    if location_tag_span1:
                        location = location_tag_span1.find('span',{'class': 'ITALIC'})
                        location = location.text if location else None
            
            vendor_name_element = item.find('span', {'class': 's-item__seller-info-text'})
            vendor_name = vendor_name_element.text if vendor_name_element else None
            
            bids_numbers_element = item.find('span', {'class': 's-item__bids'})
            bids_numbers = bids_numbers_element.text.replace('\xa0enchères', '').replace('enchère','') if bids_numbers_element else None

            product = {
                'card_name': card_name,
                'card_price': item.find('span', {'class': 's-item__price'}).text.replace('€', '').replace(',', '.').strip(),
                'sold_date': sold_date,
                'vendor_name': vendor_name,
                'bids_numbers': bids_numbers,
                'sold_location' : location
            }
            cards_list.append(product)

    
    raw_df = pd.DataFrame(cards_list) 
   

    #Identify aimed bucket
    bucket = storage.Bucket(client, bucket_name)

    # Name of the file on the GCS once uploaded
    blob = bucket.blob('raw_data.csv')
    
    # Path of the local file
    bucket.blob('raw_data/raw_data.csv').upload_from_string(raw_df.to_csv(index=False), 'text/csv')

# Formatte le nom des cartes 
def card_name_formatting(df : pd.DataFrame):

    pokemon_name_list_var= Variable.get("AIRFLOW_VAR_POKE_LIST_VAR")
    pokemon_name_list = pokemon_name_list_var.split(",")
    pokemon_name_list = [name.strip() for name in pokemon_name_list if name.strip()]

    df['card_name'] = df['card_name'].apply(unidecode)
    df['card_name'] = df['card_name'].str.lower()
    
    pokemon_list_formatted = []
    
    for pokemon in pokemon_name_list:
        formatted_pokemon = pokemon.replace('%2B', '+').split('%2F')
        parts = formatted_pokemon[0].split('+') + [formatted_pokemon[1]]
        pokemon_list_formatted.append(parts)
    
    for row_index, row in df.iterrows():
        for pokemon_split_name in pokemon_list_formatted:
            conditions = all(part in row['card_name'] for part in pokemon_split_name)
            additional_conditions = ('carte' in row['card_name']) or ('fr' in row['card_name'])
            if conditions and additional_conditions:
                df.at[row_index, 'card_name'] = ' '.join(pokemon_split_name)
    
    df = df[df['card_name'].isin([' '.join(parts) for parts in pokemon_list_formatted])]
    df = df.reset_index()
    df = df.drop(columns='index')

    return df

# Applique différentes transformations au dataframe de données bruts

def transform():
    
    object_name = 'raw_data/raw_data.csv'

    # Créer une instance du client GCS
    

    # Obtenir le bucket spécifié
    bucket = client.get_bucket(bucket_name)

    # Obtenir l'objet spécifié dans le bucket
    blob = bucket.blob(object_name)

    # Télécharger le contenu de l'objet dans un DataFrame
    content = blob.download_as_text()
    df = pd.read_csv(StringIO(content))

    # On supprimela première ligne car elle contient de mauvaises infos
    # On supprime la première ligne car elle contient de mauvaises infos
    if not df.empty:
        df = df.dropna(subset=['sold_date'])
        df = df.reset_index(drop=True)

    # EDIT NE pas drop la colonne simplement séparé la colonne en 3 colonnes par les espaces puis ne garder que la 1 èere colonne du split qui contient le nom du vendeur
    # df.drop(columns='vendor_name',inplace=True)
    #df['vendor_name'] = df['vendor_name'].str.split(expand=True)

    df['vendor_name'] = df['vendor_name'].str.replace(r'\s+', '', regex=True)
    df['vendor_name'] = df['vendor_name'].replace(r'\((\d+)\)', r' (\1) ', regex=True)
    df[['vendor_name', 'vendor_reviews', 'vendor_ratings']] = df['vendor_name'].str.split(' ', expand=True) 
    df = df.dropna(subset=['vendor_ratings'])

    
    
    #On supprime les parenthèses de la colonne 'vendor_reviews'
    df['vendor_reviews'] = df['vendor_reviews'].str.replace('(', '')
    df['vendor_reviews'] = df['vendor_reviews'].str.replace(')', '').astype(int)

    # Supprimer le pourcentage de la colonne 'vendor_ratings'
    df['vendor_ratings'] = df['vendor_ratings'].str.replace('%', '').replace(',','.')
    df['vendor_ratings'] = df['vendor_ratings'].str.replace(',','.').astype(float)
    
    
    # Drop & Renommage de la colonne 'bids_numbers' pour garder une cohérence d'odre des colonnes
    df['bids_numbers2'] = df['bids_numbers']
    df.drop(columns='bids_numbers',inplace=True)
    df.rename(columns={'bids_numbers2':'bids_numbers'},inplace=True)
  

    # Renommez les colonnes résultantes
    df.rename(columns={0: 'vendor_name', 1: 'vendor_reviews', 2: 'vendor_ratings'}, inplace=True)

    # On supprime "EUR" puis le change le nom de la colonne pour spécifié la devise
    df['card_price'] = df['card_price'].str.replace('EUR','')
    df = df.rename(columns={'card_price': 'card_price_EUR'})


    df = card_name_formatting(df)

    # Conversion des colonnes 

    # Conversion df['card_price] en float
    
    df['card_price_EUR'] = pd.to_numeric(df['card_price_EUR'], errors='coerce')

    # Conversion df['sold_date'] en datetime64 
    
    # On supprime les accents de la colonne 'sold_date'
    df['sold_date'] = df['sold_date'].apply(unidecode)
    df['sold_date'] = df['sold_date'].str.strip()
    df['sold_date'] = df['sold_date'].astype(str)
    
    # Supprimer les points de la colonne 'sold_date'
    df['sold_date'] = df['sold_date'].str.replace('.', '')
    df['sold_date'] = df['sold_date'].str.replace('sept', 'sep')
    df['sold_date'] = df['sold_date'].str.replace('sept.', 'sep')
    df['sold_date'] = df['sold_date'].str.replace('janv', 'jan')
    df['sold_date'] = df['sold_date'].str.replace('fevr.', 'feb')
    df['sold_date'] = df['sold_date'].str.replace('fevr', 'feb')
    df['sold_date'] = df['sold_date'].str.replace('mars', 'mar')
    df['sold_date'] = df['sold_date'].str.replace('avri', 'apr')

    # Convertir la colonne 'sold_date' en type datetime si nécessaire
    df['sold_date'] = pd.to_datetime(df['sold_date'], format="%d %b %Y",errors='coerce')

    df = df.loc[df['sold_location'].isnull()]

    df.drop(columns='sold_location',inplace=True)

    

    # Remplacement des "None" ie pas d'enchères par "0" puis conversion en int

    df['bids_numbers'] = df['bids_numbers'].fillna(0)
    df['bids_numbers'] = pd.to_numeric(df['bids_numbers'], errors='coerce')
    df['bids_numbers'] = df['bids_numbers'].fillna(0)

    df.dropna(inplace=True)
    
    
    #Identifie le bucket cible
    bucket = storage.Bucket(client, bucket_name)

    # Name of the file on the GCS once uploaded
    blob = bucket.blob('clean_data.csv')
    
    
    
    # Path of the local file
    bucket.blob('clean_data/clean_data.csv').upload_from_string(df.to_csv(index=False), 'text/csv')




# Ajouter fonction qui check la qualité du dataframes crée avec les données nettoyés
# Checker 
        # Data type
        # Nombre colonnes dataframes
        # Nombre lignes dataframes
        # Regarder si le contenu des colonnes est cohérent ( nom des cartes, formattage de la date,etc...)

#Récupère le fichier .csv contenant les données nettoyées et vérifie si il y a de nouvelles données en filtrant la date
    # Si noouvelles données, exporte les nouvelles données en csv et utilise xcom 
    # Utilise xcom pour spécifié qu'aucune nouvelles données n'ont été trouvées

def new_data_check(**kwargs):
    
    date_today = datetime.now().strftime('%Y-%m-%d')  # Format de date correspondant à celui dans votre DataFrame
    #old_data = pd.read_csv(r"C:\Users\Jean_Professionel\OneDrive - ESEO\Bureau\Pokemon\PokePrice Tracker\old_data.csv")
    

    object_name = 'clean_data/clean_data.csv'

    # Créer une instance du client GCS
    

    # Obtenir le bucket spécifié
    bucket = client.get_bucket(bucket_name)

    # Obtenir l'objet spécifié dans le bucket
    blob = bucket.blob(object_name)

    # Télécharger le contenu de l'objet dans un DataFrame
    content = blob.download_as_text()
    
    new_data = pd.read_csv(StringIO(content))
    
    new_data['sold_date'] = new_data['sold_date'].astype(str)
    

    new_data_df = new_data[new_data['sold_date'].str.contains(date_today)]
    new_data_df['sold_date'] = pd.to_datetime(new_data_df['sold_date'], format="%Y-%m-%d") 
    
    
    # on assigne la longeur du nouveaux df et de l'ancien dans 2 variables
    #new_data_df_length = new_data_df.shape[0]
    #old_data_df_length = old_data.shape[0] 
    # On crée une liste contenant les nouvelles ID pour le nouveaux afin de pouvoir le sinsérer dans la base de données par la suite
    #liste_incremente = [i + old_data_df_length for i in range(1, new_data_df_length +1)]

    if not new_data_df.empty:
        kwargs['ti'].xcom_push(key='new_data', value='new data has been exported')
        try:
            new_data_df = new_data_df.drop_duplicates(keep='first', ignore_index=True)
            bucket = storage.Bucket(client, bucket_name)  
            blob = bucket.blob('new_data.csv') 
            bucket.blob('new_data/new_data.csv').upload_from_string(new_data_df.to_csv(index=False), 'text/csv')
        except:
            logging.error("Échec de l'exportation")
            
    else:
        kwargs['ti'].xcom_push(key='new_data', value='no new data')


# Vérifie si il y a de nouvelles données trouvées ou non pour rediriger vers une tâche spécifique

def check_for_new_data(**kwargs):
    # Récupérez les données à partir de XCom
    ti = kwargs['ti']
    new_data_csv = ti.xcom_pull(task_ids='new_data_check_task', key='new_data')

    # Votre logique pour vérifier les nouvelles données
    if new_data_csv =='new data has been exported':
        return 'new_data_branch'  # Branchez ici si de nouvelles données sont présentes
    else:
        return 'no_new_data_branch'  # Branchez ici si aucune nouvelle donnée n'est présente

# Print qu'aunce nouvelles données n'a été trouvée; cette fonction s'éxécute en fonctionement de l'embranchement      
def no_new_data():
    print("no new data")


# MàJ de la base_data
def data_update():

    object_name = 'new_data/new_data.csv'

    # Obtenir le bucket spécifié
    bucket = client.get_bucket(bucket_name)

    # Obtenir l'objet spécifié dans le bucket
    blob = bucket.blob(object_name)

    # Télécharger le contenu de l'objet dans un DataFrame
    content = blob.download_as_text()
    
    new_data_df = pd.read_csv(StringIO(content)) 

    # Récupération des données de base 

    object_name2 = 'base_data/old_data.csv'

    # Obtenir le bucket spécifié
    bucket = client.get_bucket(bucket_base_data)

    # Obtenir l'objet spécifié dans le bucket
    blob = bucket.blob(object_name2)

    # Télécharger le contenu de l'objet dans un DataFrame
    content = blob.download_as_text() 

    # Concaténation des données anciennes et nouvelles et mapping des max reviews
    base_data_df = pd.read_csv(StringIO(content)) 
    base_data_df = base_data_df.loc[base_data_df['sold_date'] != date_today]


    full_data_df = pd.concat([base_data_df, new_data_df], ignore_index=True) 

    

    max_reviews = full_data_df.groupby('vendor_name')['vendor_reviews'].max()
    full_data_df['vendor_reviews'] = full_data_df['vendor_name'].map(max_reviews) 

    full_data_df = full_data_df.drop_duplicates(keep='first', ignore_index=True)

    # Exportation des données mise a jour en csv 
    
    bucket = storage.Bucket(client, bucket_base_data)
    blob = bucket.blob('old_data.csv')
    bucket.blob('base_data/old_data.csv').upload_from_string(full_data_df.to_csv(index=False), 'text/csv')

# Split les nouvelles en 3 dfs pour les exportés dans la DB par la suite 
def data_split():
    
    object_name = 'base_data/old_data.csv'
    bucket = client.get_bucket(bucket_base_data)
    blob = bucket.blob(object_name)
    content = blob.download_as_text()
    new_data_df = pd.read_csv(StringIO(content)) 

    new_data_df.reset_index(inplace=True)
    new_data_df.drop(columns='index',inplace=True)
    new_data_df['sale_Id'] = new_data_df.index + 1
    new_data_df['card_ID'] = new_data_df['sale_Id']
    new_data_df['vendor_ID'] = pd.factorize(new_data_df['vendor_name'])[0] + 1
    new_data_df = new_data_df.loc[new_data_df['sold_date']==date_today]
   
    # Création du dataframe "ebay_sales_data" contenant les infos relatives à la vente de la carte
    ebay_sales_data_col = ['ebay_sale_ID','card_price_EUR','sold_date','bids_numbers','sale_card_ID','vendor_ID']
    pokemon_card_col = ['sale_Id','card_price_EUR','sold_date','bids_numbers','card_ID','vendor_ID']
    ebay_sales_data_df = new_data_df[['sale_Id','card_price_EUR', 'sold_date', 'bids_numbers','card_ID','vendor_ID']].copy()
    ebay_sales_data_df.rename(columns={'card_ID':'sale_card_ID'}, inplace=True)
    ebay_sales_data_df.rename(columns={'sale_Id':'ebay_sale_ID'}, inplace=True)
    ebay_sales_data_df.rename(columns={'vendor_ID':'seller_ID'}, inplace=True)
    ebay_sales_data_df.drop(columns='ebay_sale_ID',inplace=True)
    ebay_sales_data_df.drop(columns='bids_numbers',inplace=True)
    #ebay_sales_data_df.drop_duplicates(inplace=True)
    
    # Création du dataframe "ebay_seller" contenant les infos relatives aux vendeurs ebay.
    ebay_seller_df = new_data_df[['vendor_ID','vendor_name', 'vendor_reviews', 'vendor_ratings']].copy()
    ebay_seller_df.rename(columns={'vendor_ID':'seller_ID'}, inplace=True)
    ebay_seller_df.rename(columns={'vendor_name':'seller_name'}, inplace=True)
    ebay_seller_df.rename(columns={'vendor_reviews':'seller_reviews'}, inplace=True)
    ebay_seller_df.rename(columns={'vendor_ratings':'seller_ratings'}, inplace=True)
    ebay_seller_df.drop_duplicates(subset='seller_name', keep='first', inplace=True)
    ebay_seller_df.reset_index(inplace=True)
    ebay_seller_df.drop(columns='index',inplace=True)
    ebay_seller_df.drop(columns='seller_ID',inplace=True)

    #ebay_seller_df.drop_duplicates(inplace=True)
    
    # Création du dataframe contenant les infos de la carte pokemon 
    pokemon_card_df = new_data_df[['card_ID','card_name']].copy()
    pokemon_card_df.drop(columns='card_ID',inplace=True)
    #pokemon_card_df.drop_duplicates(inplace=True)

    # Exportation des 3 dataframes en fichier .csv 
    
    # Ebay_sales_data_df
    bucket = storage.Bucket(client, bucket_name)
    blob = bucket.blob('ebay_sales_data.csv')
    bucket.blob('split_data/ebay_sales_data.csv').upload_from_string(ebay_sales_data_df.to_csv(index=False), 'text/csv') 

    # Ebay_seller_df
    bucket = storage.Bucket(client, bucket_name)
    blob = bucket.blob('ebay_seller_data.csv')
    bucket.blob('split_data/ebay_seller_data.csv').upload_from_string(ebay_seller_df.to_csv(index=False), 'text/csv') 

    # pokemon_card_df
    bucket = storage.Bucket(client, bucket_name)
    blob = bucket.blob('pokemon_card_data.csv')
    bucket.blob('split_data/pokemon_card_data.csv').upload_from_string(pokemon_card_df.to_csv(index=False), 'text/csv') 

    # Message de succès 
    print('Splitting de la data réussie') 
   
# function to return the database connection
def getconn():
    
    connector = Connector() 
    
    conn = connector.connect(
        "fiery-iridium-412613:europe-west9:poke-price-tracker-db",
        "pymysql",
        user="root",
        password="Tictact0c",
        db="ebay"
    )
    return conn


# Fonction qui envoie les nouvelles données partitionnées selon le schéma de la base de données dans la base de données Cloud SQL

def split_data_to_MySql_db():
    
     # Créer la connexion à la base de données
    try:
        connector = Connector() 
        logging.info("Connecteur créé")
    except:
        logging.error("La création du connecteur a échoué")
    
    # Créer la connexion à la base de données
    try:
        conn = connector.connect(
            "fiery-iridium-412613:europe-west9:poke-price-tracker-db",
            "pymysql",
            user="root",
            password="Tictact0c",
            db="ebay"
        )
        logging.info("Connexion à la base de données réussie")
    except:
        logging.error("La connexion à la base de données a échoué")

    try:
        # Créer l'engine directement avec la connexion
        engine = sqlalchemy.create_engine(
            "mysql+pymysql://",
            pool_pre_ping=True,
            creator=lambda: conn
        )
        logging.info("Engine créé avec succès")
    except:
        logging.error("Échec de la création du moteur")
    
    try:
        #Récupération de ebay_sales_data
        object_name = 'split_data/ebay_sales_data.csv'
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(object_name)
        content = blob.download_as_text()
        ebay_sales_data_df = pd.read_csv(StringIO(content)) 
        logging.info("ebay_sales_data_df.csv récupéré")

    except:
        logging.error("la récupération de ebay_sales_data_df.csv à échoué")
    try:
        # Récupération de ebay_seller_data
        object_name = 'split_data/ebay_seller_data.csv'
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(object_name)
        content = blob.download_as_text()
        ebay_seller_df = pd.read_csv(StringIO(content))  
        print("ebay_seller_data_df.csv récupéré")
        logging.info("ebay_seller_data.csv DL avec succès")

    except:
        logging.error("la récupération de ebay_seller_df.csv à échoué")

    
    try:
        # Récupération de pokemon_card_data
        object_name = 'split_data/pokemon_card_data.csv'
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(object_name)
        content = blob.download_as_text()
        pokemon_card_df = pd.read_csv(StringIO(content)) 
        logging.info("pokemon_card_df.csv récupéré")
    
    except:
        logging.error("la récupération de pokemon_card_data_df.csv à échoué")
    
    try:
        ebay_seller_df.to_sql('ebay_seller', con=engine, if_exists='append', index=False)
        logging.info("ebay_seller_df => DB avec succès")
    except Exception as e:
        logging.error(f"Les données n'ont pas pu être exportées vers ebay_seller : {e}")

    try:
        pokemon_card_df.to_sql('pokemon_card', con=engine, if_exists='append', index=False)
        logging.info("pokemon_card_df => DB avec succès")
    except Exception as e:
        logging.error(f"Les données n'ont pas pu être exportées vers pokemon_card : {e}")

    try:
        ebay_sales_data_df.to_sql('ebay_sales_data', con=engine, if_exists='append', index=False)
        logging.info("ebay_sales_data_df => DB avec succès")
    except Exception as e:
        logging.error(f"Les données n'ont pas pu être exportées vers ebay_sales_data : {e}")

    # Fermez la session et la connexion à la base de données
    try:
        connector.close()
        logging.info("Connexion fermée avec succès")
    except:
        logging.error("Échec de la fermeture de la connexion")
    

# Fonction qui supprime les fichiers crée (seulement si de nouvelles données ont été collectées)
        
def delete_files_in_folder():
    folder_list = ['split_data/','new_data/','raw_data/','clean_data/']
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    
    for folder in folder_list:
        blobs = bucket.list_blobs(prefix=folder)
        
        for blob in blobs:
            blob.delete()


# CODE 
        
#soup_list = get_data(pokemon_name_list)
#raw_df = parse_output(soup_list)
#clean_df = transform(raw_df)
#load_data_gcp_bucket(client,bucket_name,clean_df)

# Création du DAG

dag_id = "poke_dag"

default_args = {
    'owner': 'jean',
    'start_date': airflow.utils.dates.days_ago(1),
    
}


with DAG(
    dag_id=dag_id, 
    default_args=default_args, 
    schedule_interval= '55 22 * * *',
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=10),
    catchup=False
) as dag:
   

    get_data_task = PythonOperator(
        task_id='get_data_task',
        python_callable=get_data,
        provide_context=True,
        dag=dag
    ) 

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable = transform,
        provide_context =True,
        dag=dag
    )

    new_data_check_task = PythonOperator(
    task_id='new_data_check_task',
    python_callable=new_data_check,
    provide_context=True,
    dag=dag,
)

check_for_new_data_task = BranchPythonOperator(
    task_id='check_for_new_data',
    python_callable=check_for_new_data,
    provide_context=True,
    dag=dag,
)
    

# Embranchement
no_new_data_branch = DummyOperator(task_id='no_new_data_branch', dag=dag)
new_data_branch = DummyOperator(task_id='new_data_branch', dag=dag)

no_new_data_task = PythonOperator(
    task_id='no_new_data_task',
    python_callable=no_new_data,
    provide_context=True,
    dag=dag,
)

data_update_task = PythonOperator(
    task_id='data_update_task',
    python_callable=data_update,
    provide_context=True,
    dag=dag,
)

data_split_task = PythonOperator(
    task_id='data_split_task',
    python_callable=data_split,
    provide_context=True,
    dag=dag,
)

split_data_to_MySql_db_task = PythonOperator(
    task_id='split_data_to_MySql_db_task',
    python_callable=split_data_to_MySql_db,
    provide_context=True,
    dag=dag,
) 

delete_folders_task = PythonOperator(
    task_id='delete_folders_task',
    python_callable=delete_files_in_folder,
    provide_context=True,
    dag=dag,
) 

# Définissez les dépendances appropriées
get_data_task >> transform_task >> new_data_check_task >> check_for_new_data_task
check_for_new_data_task >> [new_data_branch, no_new_data_branch]
no_new_data_branch >> no_new_data_task
new_data_branch >> data_update_task >> data_split_task >> split_data_to_MySql_db_task >> delete_folders_task
