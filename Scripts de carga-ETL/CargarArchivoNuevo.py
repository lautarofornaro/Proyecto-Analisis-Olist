# Carga de Herramientas

import pandas as pd
import boto3
import os



session = boto3.Session( aws_access_key_id='AKIAVKIZVMSSTP7Q77M6', aws_secret_access_key='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')

s3 = session.resource('s3')

my_bucket = s3.Bucket('olist-data-henry')

client = boto3.client('s3', aws_access_key_id="AKIAVKIZVMSSTP7Q77M6",
                        aws_secret_access_key="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",
                        region_name="us-east-1")

def CargarArchivo(nombre_archivo,direccion_archivo):
    
    a=1
    nombre_archivo = nombre_archivo[:-4] + "_%s" % str(a) + ".csv"
    for my_bucket_object in my_bucket.objects.all():

        if nombre_archivo == my_bucket_object.key[len(my_bucket_object.key)-len(nombre_archivo):]:
            a = a + 1
            nombre_archivo = nombre_archivo[:-6] + "_%s" % str(a) + ".csv"

    print('Cargando el archivo: ', nombre_archivo[:-6])
    
    with open(direccion_archivo, 'rb') as archivo:
            client.put_object(
                    ACL='public-read',
                    Body=archivo,
                    Bucket='olist-data-henry',
                    Key= nombre_archivo[:-14]+'/%s' %nombre_archivo
                )
            
print('Por favor ingresar el nombre del csv')
nombre_archivo = input()

print('Por favor ingresar el directorio completo para acceder al archivo')
direccion_archivo = input()

CargarArchivo(nombre_archivo,direccion_archivo)
    
    


