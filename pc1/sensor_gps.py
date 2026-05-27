#!/usr/bin/env python3
"""
Sensor GPS (PC1).
Agrega señales GPS de vehículos que transitan cerca de una intersección
y reporta velocidad promedio y nivel de congestión inferido.
Publica eventos via ZMQ PUSH hacia el broker.
"""

import os
import random
import sys
import time
import zmq

# Modifica el path de ejecución de Python para poder importar módulos desde el directorio raíz
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importaciones de configuraciones, modelos de datos y utilidades comunes del sistema
from common.config_loader import load_config
from common.models import EventoGPS
from common.utils import generar_intersecciones_3x3, generar_timestamp_iso, log_componente

# Configuración del nombre del módulo para los logs y frecuencia de envío
COMPONENTE = "sensor_gps"
INTERVALO_SEGUNDOS = 4.0  # El sensor GPS transmite datos cada 4 segundos

# Configuración de los perfiles de tráfico con sus rangos de velocidad y probabilidades (pesos)
PERFILES_GPS = {
    "bajo":  {"velocidad": (40.0, 80.0), "peso": 0.60}, # 60% de probabilidad de tráfico fluido
    "medio": {"velocidad": (20.0, 40.0), "peso": 0.28}, # 28% de probabilidad de tráfico moderado
    "alto":  {"velocidad": (4.0,  18.0), "peso": 0.12}, # 12% de probabilidad de embotellamiento
}


def _inferir_nivel(velocidad: float) -> str:
    """
    Función de soporte para determinar el nivel de congestión exacto.
    Garantiza que no haya discrepancias entre la velocidad final y la etiqueta del estado.
    """
    if velocidad >= 40.0:
        return "bajo"
    if velocidad >= 20.0:
        return "medio"
    return "alto"


def _generar_evento(sensor_id: str, interseccion: str) -> EventoGPS:
    """
    Genera un objeto de tipo EventoGPS simulando datos de vehículos en tiempo real.
    """
    # Extrae las claves del diccionario ("bajo", "medio", "alto")
    niveles = list(PERFILES_GPS.keys())
    # Extrae los pesos configurados en la constante para pasárselos al generador probabilístico
    pesos = [PERFILES_GPS[n]["peso"] for n in niveles]
    
    # Elige un nivel de congestión inicial basado en las probabilidades asignadas
    nivel = random.choices(niveles, weights=pesos, k=1)[0]

    # Genera una velocidad aleatoria (float) expandiendo la tupla de rangos del perfil electo
    velocidad = round(random.uniform(*PERFILES_GPS[nivel]["velocidad"]), 1)
    
    # Recalcula el nivel desde la velocidad final para mantener una consistencia matemática estricta
    nivel_final = _inferir_nivel(velocidad)   

    # Instancia y retorna el modelo tipado listo para ser enviado
    return EventoGPS(
        sensor_id=sensor_id,
        interseccion=interseccion,
        nivel_congestion=nivel_final,
        velocidad_promedio=velocidad,
        timestamp=generar_timestamp_iso(), # Genera el timestamp en formato ISO 8601
    )


def main():
    # 1. CARGA DE CONFIGURACIÓN DE RED
    config = load_config()
    host_pc1 = config["pc1"]["host"]                 # Dirección IP o Host de la PC central (PC1)
    puerto = config["ports"]["sensor_to_broker"]     # Puerto asignado para la comunicación Sensor -> Broker

    # Carga la matriz de simulación urbana (ej: I1 a I9)
    intersecciones = generar_intersecciones_3x3()

    # 2. INICIALIZACIÓN DEL SOCKET ZERO MQ
    ctx = zmq.Context()
    socket = ctx.socket(zmq.PUSH) # Socket de tipo PUSH: solo salida de datos hacia una cola asíncrona
    endpoint = f"tcp://{host_pc1}:{puerto}"
    socket.connect(endpoint) # Se conecta al endpoint receptor provisto por el Broker

    # Pausa de seguridad para sincronización de sockets en la red
    time.sleep(0.5)

    log_componente(COMPONENTE, f"PUSH conectado a {endpoint}")

    # 3. BUCLE INFINITO DE TELEMETRÍA
    try:
        while True:
            # Itera y procesa cada punto geográfico/intersección del mapa simulado
            for interseccion in intersecciones:
                # Formatea el identificador único para este sensor GPS (ej: "gps_i1")
                sensor_id = f"gps_{interseccion.lower()}"
                
                # Ejecuta la lógica de generación del evento simulado
                evento = _generar_evento(sensor_id, interseccion)
                
                # Transmite la cadena JSON serializada por el pipeline de red
                socket.send_string(evento.to_json())
                
                # Registra en la salida estándar/logs el evento transmitido
                log_componente(
                    COMPONENTE,
                    f"{interseccion} | vel={evento.velocidad_promedio} km/h"
                    f" | congestion={evento.nivel_congestion}",
                )
            
            # Duerme el hilo principal durante el tiempo configurado antes de la siguiente ráfaga
            time.sleep(INTERVALO_SEGUNDOS)
            
    # 4. MANEJO DE SEÑALES DE SALIDA
    except KeyboardInterrupt:
        # Permite interrumpir la ejecución de manera elegante con Ctrl+C sin romper los puertos
        log_componente(COMPONENTE, "Detenido manualmente.", nivel="WARN")
    finally:
        # Libera los recursos del socket y destruye el contexto de red de ZeroMQ del sistema operativo
        socket.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
