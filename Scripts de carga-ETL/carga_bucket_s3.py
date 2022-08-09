# Cargamos la libreria a utilizar y traemos las funciones creadas
import boto3
from funciones import *

# Ingresamos las claves de acceso de AWS para poder subir informacion al S3 Bucket

print("Ingresar la clave de acceso de AWS")
clave_de_acceso = input()

print("Ingresar la clave de acceso secreta de AWS")
clave_de_acceso_secreta = input()



client = boto3.client('s3', aws_access_key_id=clave_de_acceso,
                        aws_secret_access_key=clave_de_acceso_secreta,
                        region_name="us-east-1")


session = boto3.Session( aws_access_key_id=clave_de_acceso, aws_secret_access_key=clave_de_acceso_secreta)

s3 = session.resource('s3')

# Especificamos el nombre del Bucket que vamos a utilizar

print("Ingresar el nombre del Bucket de S3 a utilizar")
nombre_bucket = input()
my_bucket = s3.Bucket(nombre_bucket)

#Tenemos las opciones de subir una carpeta entera a nuestro Bucket S3 o la opcion de cargar un archivo con informacion nueva de un archivo 
#ya existente en nuestro Bucket S3
        
print("¿Que queres hacer?")  
print("1-Cargar una carpeta a un Bucket S3")
print("2-Agregar un archivo con informacion nueva de un archivo ya existente")
print("3-Salir")
opcion_elegida = 0

while opcion_elegida not in [1,2,3]:
    opcion_elegida = int(input())
    if opcion_elegida == 1:
        print('Por favor ingresar el directorio que quieras subir al Bucket S3')
        directorio = input()
        
        carga_carpeta_bucket(directorio,nombre_bucket,client)
        
    elif opcion_elegida == 2:
        print('Por favor ingresar el nombre del csv completo(con su extension .csv)')
        nombre_archivo = input()
        
        print('Por favor ingresar el directorio completo donde se encuentra el archivo')
        direccion_archivo = input()

        cargar_archivo(nombre_archivo,direccion_archivo,my_bucket,nombre_bucket,client)

    elif opcion_elegida == 3:
        break
    else:
        print("La opcion no es valida, ingresar nuevamente")

        
    


