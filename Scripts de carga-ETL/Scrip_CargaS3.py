# Carga de Herramientas

import pandas as pd
import boto3
import os
import pandas as pd

# Conectamos con S3

client = boto3.client('s3', aws_access_key_id="AKIAVKIZVMSSTP7Q77M6",
                        aws_secret_access_key="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",
                        region_name="us-east-1")

# Funciones

# Funcion para pasar de int64 a int32
def ETL_int64(DF):
    for column in DF:
        if DF[column].dtype == 'int64':
            DF[column] = DF[column].astype(int)
        else:
            pass
    
# Carga Data Frames
def ETL(directorio, trabajado):

    # Obtenemos la lista de archivos del directorio especificado
    lista_csv = os.listdir(directorio)

    # Recorremos la lista generada para realizar distintas acciones en los archivos del directorio
    for nombre_csv in lista_csv:
        # Generamos los DF
        print('Trabajando el archivo: ', nombre_csv)
        df = pd.read_csv(directorio+'\%s' % nombre_csv)
        
        # Normalizamos enteros
        ETL_int64(df)
        
        # Generamos metadata de las columnas para evitar errores con los indices
        lista_header = df.columns
        lista_header = lista_header.tolist()
        
        # Exportamos csvs en un directorio especificado
        df.to_csv(trabajado+'\%s' % nombre_csv, index = False, header = lista_header)
        
        # Carga a S3
        ruta_archivo = trabajado+"\%s" % nombre_csv
        
        with open(ruta_archivo, 'rb') as archivo:
            client.put_object(
                    ACL='public-read',
                    Body=archivo,
                    Bucket='olist-data-henry',
                    Key= nombre_csv[:-12]+'/%s' % nombre_csv
                )
            

            
print('Por favor ingresar el directorio de la data')
directorio = input()

print('Por favor ingresar el directorio para guardar la data trabajada')
trabajado = input()

ETL(directorio,trabajado)