#!/usr/bin/env python3
"""
Sensor GPS (PC1).
Agrega señales GPS de vehículos que transitan cerca de una intersección
y reporta velocidad promedio y nivel de congestión inferido.
Publica eventos via ZMQ PUB (Tópico: "gps") hacia el broker.
"""

import os
import random
import sys
import time

import zmq

# Modifica el path de Python para poder importar módulos desde la carpeta raíz del proyecto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import load_config
from common.models import EventoGPS
from common.utils import generar_intersecciones_3x3, generar_timestamp_iso, log_componente

COMPONENTE = "sensor_gps"
INTERVALO_SEGUNDOS = 4.0     # Frecuencia de publicación (cada cuánto despierta)

# Perfiles de velocidad por nivel de congestión (km/h)
PERFILES_GPS = {
    "bajo":  {"velocidad": (40.0, 80.0), "peso": 0.60},
    "medio": {"velocidad": (20.0, 40.0), "peso": 0.28},
    "alto":  {"velocidad": (4.0,  18.0), "peso": 0.12},
}


def _inferir_nivel(velocidad: float) -> str:
    """Infiere matemáticamente el nivel de congestión a partir de la velocidad."""
    if velocidad >= 40.0:
        return "bajo"
    if velocidad >= 20.0:
        return "medio"
    return "alto"


def _generar_evento(sensor_id: str, interseccion: str) -> EventoGPS:
    """
    Simula la recolección de datos de telemetría GPS de los vehículos
    que transitan por la intersección para promediar velocidades.
    """
    niveles = list(PERFILES_GPS.keys())
    pesos = [PERFILES_GPS[n]["peso"] for n in niveles]
    nivel = random.choices(niveles, weights=pesos, k=1)[0]

    velocidad = round(random.uniform(*PERFILES_GPS[nivel]["velocidad"]), 1)
    nivel_final = _inferir_nivel(velocidad)   # Recalcular desde velocidad para mantener consistencia

    return EventoGPS(
        sensor_id=sensor_id,
        interseccion=interseccion,
        nivel_congestion=nivel_final,
        velocidad_promedio=velocidad,
        timestamp=generar_timestamp_iso(),
    )


def main():
    # 1. CARGA DE CONFIGURACIÓN Y RED
    config = load_config()
    host_pc1 = config["pc1"]["host"]
    puerto = config["ports"]["sensor_to_broker"]

    # Obtiene el mapa de calles de la red vial (I1 a I9)
    intersecciones = generar_intersecciones_3x3()

    # 2. CONFIGURACIÓN DEL SOCKET ZERO MQ (Patrón PUB/SUB)
    ctx = zmq.Context()
    socket = ctx.socket(zmq.PUB)  # Configurado correctamente como PUB según la Figura 1
    endpoint = f"tcp://{host_pc1}:{puerto}"
    socket.connect(endpoint)

    # Pausa de cortesía obligatoria para evitar la pérdida inicial de mensajes en ZMQ
    time.sleep(0.5)

    log_componente(
        COMPONENTE, 
        f"PUB conectado a {endpoint} | Tópico='gps'"
    )

    # 3. BUCLE PRINCIPAL DE TRANSMISIÓN
    try:
        while True:
            for interseccion in intersecciones:
                sensor_id = f"gps_{interseccion.lower()}"
                evento = _generar_evento(sensor_id, interseccion)
                
                # Envío Multipart: [Tópico en bytes, Cuerpo del JSON en bytes]
                socket.send_multipart([b"gps", evento.to_json().encode("utf-8")])
                
                log_componente(
                    COMPONENTE,
                    f"{interseccion} | vel={evento.velocidad_promedio} km/h"
                    f" | congestion={evento.nivel_congestion}",
                )
            # Espera antes de la siguiente ronda de telemetría
            time.sleep(INTERVALO_SEGUNDOS)
            
    # 4. CONTROL DE CIERRE LIMPIO
    except KeyboardInterrupt:
        log_componente(COMPONENTE, "Detenido manualmente.", nivel="WARN")
    finally:
        # Cierra el socket y destruye el contexto para liberar recursos del sistema operativo
        socket.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
