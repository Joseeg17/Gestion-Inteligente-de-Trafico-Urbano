#!/usr/bin/env python3
"""
Sensor de cámara (PC1).
Simula una cámara de tráfico en cada intersección.
Mide volumen de vehículos y velocidad promedio.
Publica eventos via ZMQ PUSH hacia el broker.
"""

import json
import os
import random
import sys
import time
import zmq

# Modifica el path de Python para permitir importaciones desde la raíz del proyecto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importaciones de módulos compartidos de la arquitectura
from common.config_loader import load_config
from common.models import EventoCamara
from common.utils import generar_intersecciones_3x3, generar_timestamp_iso, log_componente

# Configuración de metadatos y tiempos del componente
COMPONENTE = "sensor_camara"
INTERVALO_SEGUNDOS = 2.0  # Tiempo de espera entre ráfagas de lecturas de sensores

# Perfiles estadísticos para simular escenarios de tráfico realistas
# Formato: "estado": {"volumen": (mín, máx), "velocidad": (mín, máx)}
PERFILES = {
    "normal":     {"volumen": (5, 20),  "velocidad": (35.0, 60.0)},
    "congestion": {"volumen": (25, 55), "velocidad": (8.0, 25.0)},
    "pico":       {"volumen": (20, 35), "velocidad": (18.0, 35.0)},
}


def _generar_evento(sensor_id: str, interseccion: str) -> EventoCamara:
    """
    Genera un objeto de evento sintético pero realista basado en probabilidades.
    
    Usa una selección ponderada para determinar el estado del tráfico:
      - 60% de probabilidad de tráfico Normal.
      - 25% de probabilidad de Congestión.
      - 15% de probabilidad de Hora Pico.
    """
    perfil = random.choices(
        list(PERFILES.values()),
        weights=[0.60, 0.25, 0.15], # Pesos o probabilidades asignadas a cada perfil
        k=1,                        # Cantidad de elementos a seleccionar
    )[0]
    
    # Desempaqueta rangos del perfil seleccionado y genera valores aleatorios
    volumen = random.randint(*perfil["volumen"])
    velocidad = round(random.uniform(*perfil["velocidad"]), 1) # Redondea a 1 decimal
    
    # Retorna una instancia del modelo de datos validado
    return EventoCamara(
        sensor_id=sensor_id,
        interseccion=interseccion,
        volumen=volumen,
        velocidad_promedio=velocidad,
        timestamp=generar_timestamp_iso(), # Genera marca de tiempo actual en formato ISO 8601
    )


def main():
    # 1. CARGA DE CONFIGURACIÓN Y RED
    config = load_config()
    host_pc1 = config["pc1"]["host"]                 # IP o Host del Broker (PC1)
    puerto = config["ports"]["sensor_to_broker"]     # Puerto PULL del Broker

    # Genera una lista de strings simulando una red vial de 3x3 (ej: ["I1", "I2", ..., "I9"])
    intersecciones = generar_intersecciones_3x3()

    # 2. CONFIGURACIÓN DE ZERO MQ
    ctx = zmq.Context()
    socket = ctx.socket(zmq.PUSH) # Socket de tipo PUSH (envía datos de forma unidireccional)
    endpoint = f"tcp://{host_pc1}:{puerto}"
    
    # Se conecta al broker (el broker es el que hace el .bind())
    socket.connect(endpoint)

    # Pausa de cortesía para mitigar la pérdida de mensajes iniciales ("Slow Joiner" de ZMQ)
    time.sleep(0.5)

    log_componente(COMPONENTE, f"PUSH conectado a {endpoint} | {len(intersecciones)} intersecciones")

    # 3. BUCLE DE TRANSMISIÓN INFINITA
    try:
        while True:
            # En cada ciclo, simula y envía los datos de todas las esquinas/intersecciones
            for interseccion in intersections:
                # Construye un identificador único para el sensor (ej: "cam_i1")
                sensor_id = f"cam_{interseccion.lower()}"
                
                # Invoca la función para estructurar las métricas de tráfico
                evento = _generar_evento(sensor_id, interseccion)
                
                # Serializa el objeto EventoCamara a un JSON String y lo inyecta al pipeline PUSH
                socket.send_string(evento.to_json())
                
                # Registra localmente la telemetría enviada
                log_componente(
                    COMPONENTE,
                    f"{interseccion} | vol={evento.volumen} veh | vel={evento.velocidad_promedio} km/h",
                )
            
            # Duerme el hilo para espaciar las ráfagas de datos y no saturar la red
            time.sleep(INTERVALO_SEGUNDOS)
            
    # 4. MANEJO DE CIERRE LIMITADO
    except KeyboardInterrupt:
        # Permite una desconexión controlada si el usuario presiona Ctrl+C
        log_componente(COMPONENTE, "Detenido manualmente.", nivel="WARN")
    finally:
        # Cierra el socket y destruye el contexto limpiando buffers de memoria del S.O.
        socket.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
