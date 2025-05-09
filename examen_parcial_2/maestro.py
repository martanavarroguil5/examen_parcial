import socket
import os

# Configuración de la red
HOST = 'localhost'
PORT = 12345

def enviar_archivos_a_workers(archivos):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen(1)
        print("Esperando conexión del worker...")
        conn, addr = s.accept()
        with conn:
            print(f"Conexión establecida con {addr}")
            for archivo in archivos:
                # Verificar si el archivo existe
                if not os.path.exists(archivo):
                    print(f"El archivo {archivo} no existe.")
                    continue  # Saltar al siguiente archivo
                
                # Enviar archivo al worker
                print(f"Enviando archivo {archivo} al worker...")
                with open(archivo, 'rb') as f:
                    data = f.read()
                    print(f"Enviando {len(data)} bytes del archivo {archivo}")
                    conn.sendall(data)
            print("Archivos enviados. Esperando resultados del worker...")

            # Recibir resultados de los workers
            resultados = b""
            while True:
                chunk = conn.recv(4096)  # Aumentar el tamaño del buffer
                if not chunk:
                    break
                resultados += chunk

            if resultados:
                print("Resultados completos recibidos:", resultados.decode('utf-8'))
            else:
                print("No se recibieron resultados.")

if __name__ == "__main__":
    archivos = ['datos_mal.csv', 'datos_mal.JSON', 'datos_mal2.JSON']
    enviar_archivos_a_workers(archivos)
