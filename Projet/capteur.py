import pandas as pd
import numpy as np
import datetime
from kafka import *
from minio import Minio
from minio.error import S3Error
import datetime
import json

"""
Mini-Projet : Traitement de l'Intelligence Artificielle
Contexte : Allier les concepts entre l'IA, le Big Data et IoT

Squelette pour simuler un capteur qui est temporairement stocké sous la forme de Pandas
"""

"""
    Dans ce fichier capteur.py, vous devez compléter les méthodes pour générer les données brutes vers Pandas 
    et par la suite, les envoyer vers Kafka grace au fichier consummer.py.
    ---
    
    n'oubliez pas de creér de la donner avec des valeur nulles, de fausse valeur ( par exemple negatives pour les valeur
    qui initialement doivent etre entre 0 et 100 ), et de la valeur faussement typer ( je veux par exemple une valeur
    string qui doit a la base être en int)

"""

def generate_dataFrame(col):
    """
    Cette méthode permet de générer un DataFrame Pandas pour alimenter vos data
    """
    df = pd.DataFrame(columns=col)
    add_data(df)
    return df

def add_data(df: pd.DataFrame):
    """
    Cette méthode permet d'ajouter de la donnée vers votre DataFrame Pandas

    # on va creer une boucle infinie, vous devez créer et ajouter des données dans Pandas à valeur aléatoire.
    # Chaque itération comportera 1 ligne à ajouter. ex : timestamp = datetime.timedelta(seconds=1)
    #  on va creer une liste avec les elements créees
    # ajouter une ligne au DataFrame
    # retourner le dataframe
    """


def write_data_minio(df: pd.DataFrame):
    """
    Cette méthode permet d'écrire le DataFrame vers Minio.
    (Obligatoire)
    ## on va tester si la bucket existe , dans le cas contraire on la crer
    ## on pousse le dataframe sur minio
    #decommentez le code du dessous

    """

    #client = Minio(
    #   "localhost:9000",
    #    secure=False,
    #    access_key="minio",
    #    secret_key="minio123"
    #)



if __name__ == "__main__":

    """""
    creer une liste column qui contient le header de votre dataframe 
    decommentez le code du dessous
    
    """""

    #df = generate_dataFrame(columns)
    #write_data_minio(df)