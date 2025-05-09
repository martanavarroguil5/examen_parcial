import socket
import json
import csv

# Configuración de la red
HOST = 'localhost'
PORT = 12345

def procesar_csv(data):
    """
    Procesa un archivo CSV desordenado y lo reorganiza en el formato (titulo, fecha, contenido).
    """
    processed_data = []
    for row in data:
        print("Procesando fila CSV:", row)  # Imprime las filas procesadas
        if len(row) >= 4:  # Asegurarnos de que haya al menos 4 columnas
            title = row[0]  # Nombre
            date = row[2]   # Fecha
            content = row[3]  # Contenido
            processed_data.append((title, date, content))
        else:
            print("Fila inválida en CSV:", row)  # Imprime si la fila no tiene suficientes columnas
    return processed_data

def procesar_json(data):
    """
    Procesa un archivo JSON desordenado y lo reorganiza en el formato (titulo, fecha, contenido).
    """
    processed_data = []
    for item in data:
        print("Procesando item JSON:", item)  # Imprime el item procesado
        title = item.get('info') or item.get('title')  # Título
        date = item.get('date')  # Fecha
        content = item.get('content') or item.get('body')  # Contenido

        if title and date and content:  # Asegurarnos de que no haya valores nulos
            processed_data.append((title, date, content))
        else:
            print("Item inválido en JSON:", item)  # Imprime si el item no tiene los campos correctos
    return processed_data

def recibir_archivos_y_procesar():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print("Conectado al maestro. Esperando archivos...")

        # Recibir archivos del maestro
        while True:
            data = b""
            while True:
                chunk = s.recv(1024)
                if not chunk:
                    break
                data += chunk
                # Imprimir los fragmentos recibidos para depuración
                print(f"Recibido un fragmento de {len(chunk)} bytes")

            if not data:
                break

            print("Archivo recibido. Procesando...")  # Verifica que el archivo se está recibiendo
            print("Tamaño total del archivo recibido:", len(data))  # Muestra el tamaño total del archivo recibido

            try:
                # Procesar el archivo recibido
                if b".csv" in data:
                    # Procesar archivo CSV
                    csv_data = data.decode('utf-8').splitlines()
                    csv_reader = csv.reader(csv_data)
                    processed_data = procesar_csv(csv_reader)
                elif b".JSON" in data:  # Cambio aquí para aceptar "JSON" en mayúsculas
                    # Procesar archivo JSON
                    json_data = json.loads(data.decode('utf-8'))
                    processed_data = procesar_json(json_data)

                print("Datos procesados:", processed_data)

                # Enviar los resultados al maestro
                resultados = json.dumps(processed_data)
                print(f"Enviando los resultados al maestro: {resultados}")  # Imprimir los resultados enviados
                try:
                    s.sendall(resultados.encode('utf-8'))
                    print("Resultados enviados correctamente")
                except Exception as e:
                    print(f"Error al enviar los resultados: {e}")

            except Exception as e:
                print(f"Error al procesar los datos: {e}")
                s.sendall(b"Error al procesar los datos")  # Enviar error al maestro si algo sale mal

if __name__ == "__main__":
    recibir_archivos_y_procesar()
