# Importamos las librerias n utilizar

import pandas as pd
import os


# Esta funcion se encarga del cargado de una carpeta n nuestro S3 Bucket
def carga_carpeta_bucket(directorio,nombre_bucket,cliente):

    # Obtenemos la lista de archivos del directorio especificado
    lista_csv = os.listdir(directorio)

    # Recorremos la lista generada para ir subiendo los archivos al Bucket
    for nombre_csv in lista_csv:
        
        #Obtenemos la ruta del archivo que tenemos que subir
        print('Subiendo el archivo: ', nombre_csv)
        ruta_archivo = directorio+"\%s" % nombre_csv
        
        with open(ruta_archivo, 'rb') as archivo:
            cliente.put_object(
                    ACL='public-read',
                    Body=archivo,
                    Bucket=nombre_bucket,
                    Key= nombre_csv[:-12]+'/%s' % nombre_csv
                )
    return "La carpeta se cargo con exito"
            

# Esta funcion se encarga de cargar un archivo con data nueva de un archivo ya existente con el mismo nombre, le agrega un prefijo para poder utilizarse
# como particion
def cargar_archivo(nombre_archivo,direccion_archivo,bucket,nombre_bucket,cliente):
    
    n=1
    
    # Agrega el sufijo al archivo
    nombre_archivo = nombre_archivo[:-4] + "_%s" % str(n) + ".csv"
    for my_bucket_object in bucket.objects.all():

        if nombre_archivo == my_bucket_object.key[len(my_bucket_object.key)-len(nombre_archivo):]:
            n = n + 1
            nombre_archivo = nombre_archivo[:-6] + "_%s" % str(n) + ".csv"

    # Con la direccion del archivo empezamos a subirlo al Bucket S3
    print('Subiendo el archivo: ', nombre_archivo[:-6])
    
    with open(direccion_archivo, 'rb') as archivo:
            cliente.put_object(
                    ACL='public-read',
                    Body=archivo,
                    Bucket=nombre_bucket,
                    Key= nombre_archivo[:-14]+'/%s' %nombre_archivo
                )
    return "El archivo se cargo con exito"
          