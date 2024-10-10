"""
Este módulo contiene funciones para descargar conjuntos de datos de malware
desde URLs específicas, guardarlos en el sistema y cargarlos como DataFrames de
PySpark.

Funciones:
- download_if_not_exists(url: str, file_path: str) -> None: Descargar un
    archivo, si no existe, en la ruta especificada.
- load_data(base_url: str) -> DataFrame: Descargar conjuntos de datos de
    malware desde URLs específicas y cargarlos en un DataFrame de PySpark.
"""

import os
import requests
from pyspark.sql import SparkSession, DataFrame



def download_if_not_exists(url: str, file_path: str) -> None:
    """
    Descargar un archivo desde una URL y guardarlo en el sistema si no existe.

    Args:
        url (str): La URL desde donde se descargará el archivo.
        file_path (str): La ruta en el sistema donde se guardará el archivo.
    """
    if os.path.exists(file_path):
        print(f'{file_path} ya existe.')
        return

    print(f'Descargando {file_path}...')
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        print(f'{file_path} guardado exitosamente.')
    except requests.exceptions.Timeout:
        print(f'Error: La solicitud a {url} ha excedido el tiempo de espera.')
    except requests.exceptions.HTTPError as err:
        print(f'Error HTTP: {err}')
    except requests.exceptions.RequestException as err:
        print(f'Error en la solicitud: {err}')
    except OSError as e:
        print(f'Error de sistema al intentar guardar el archivo: {e}')

def load_data(base_url: str
    = 'https://raw.githubusercontent.com/IvanLetteri/MTA-KDD-19/master/'
) -> DataFrame:
    """
    Descargar conjuntos de datos de malware desde URLs específicas y cargarlos
    en DataFrames de PySpark.

    Los archivos se guardan en un directorio local. Si ya existen, no se
    vuelven a descargar. Devuelve un DataFrame combinado con todos los datos.

    Args:
        base_url (str): La URL base desde la cual se descargarán los datos.
    
    Returns:
        DataFrame: Un DataFrame combinado que contiene los datos de malware y
        de tráfico legítimo.
    """
    # Definir las URL
    urls = {
        'mta': f'{base_url}datasetLegitimate33featues.csv',
        'leg': f'{base_url}datasetMalware33featues.csv'
    }

    # Rutas relativas para guardar los archivos CSV
    data_dir = './data'
    os.makedirs(data_dir, exist_ok=True)

    file_paths = {
        key: os.path.join(data_dir, f'dataset{key.capitalize()}33featues.csv')
        for key in urls
    }

    # Descargar los archivos si no existen
    for key, url in urls.items():
        download_if_not_exists(url, file_paths[key])

    # Obtener una sesión de Spark
    spark = SparkSession.builder \
        .appName('MalwareAnalysis') \
        .getOrCreate()

    # Cargar un PySpark
    mta_df = spark.read.csv(file_paths['mta'], header=True, inferSchema=True)
    leg_df = spark.read.csv(file_paths['leg'], header=True, inferSchema=True)

    # Unir ambos DataFrames
    return mta_df.union(leg_df)
