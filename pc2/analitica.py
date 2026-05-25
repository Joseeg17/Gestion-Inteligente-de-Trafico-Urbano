#!/usr/bin/env python3
"""
Analítica (PC2).
- SUB: recibe eventos desde el broker (puerto broker_to_analitica).
- PUSH: envía comandos a control de semáforos (analitica_to_semaforos).
- PUSH: envía datos para persistencia a BD principal y réplica.
"""

import json
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

import zmq  # Librería para comunicación distribuida (ZeroMQ)

# Añade el directorio padre al path para importar módulos comunes
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import load_config  # Carga configuración del sistema
from common.models import ComandoSemaforo      # Modelo de comando para semáforos
from common.utils import generar_timestamp_iso, log_componente  # Utilidades

COMPONENTE = "analitica"

# Umbrales configurables para clasificar el tráfico
UMBRAL_VOLUMEN_CONGESTION = 28
UMBRAL_VELOCIDAD_BAJA = 22.0
UMBRAL_VEH_POR_SEG_CONGESTION = 1.8


# Clasificación de eventos provenientes de cámaras
def _clasificar_camara(payload: Dict[str, Any]) -> str:
    volumen = int(payload.get("volumen", 0))
    vel = float(payload.get("velocidad_promedio", 0.0))
    # Si hay mucho volumen o baja velocidad → congestión
    if volumen >= UMBRAL_VOLUMEN_CONGESTION or vel < UMBRAL_VELOCIDAD_BAJA:
        return "congestion"
    return "trafico_normal"


# Clasificación de eventos provenientes de espiras
def _clasificar_espira(payload: Dict[str, Any]) -> str:
    veh = int(payload.get("vehiculos_contados", 0))
    intervalo = int(payload.get("intervalo_segundos", 1))
    intervalo = max(intervalo, 1)  # Evita división por cero
    tasa = veh / float(intervalo)  # Vehículos por segundo
    if tasa >= UMBRAL_VEH_POR_SEG_CONGESTION:
        return "congestion"
    return "trafico_normal"


# Clasificación de eventos provenientes de GPS
def _clasificar_gps(payload: Dict[str, Any]) -> str:
    nivel = str(payload.get("nivel_congestion", "bajo")).strip().lower()
    vel = float(payload.get("velocidad_promedio", 99.0))
    # Priorización si congestión alta o velocidad muy baja
    if nivel == "alto" or vel < 12.0:
        return "priorizacion"
    # Congestión media
    if nivel == "medio" or vel < 20.0:
        return "congestion"
    return "trafico_normal"


# Genera un comando de semáforo a partir de la clasificación
def _comando_desde_clasificacion(
    clasificacion: str,
    interseccion: str,
    tipo_evento: str,
) -> ComandoSemaforo:
    """
    Mapea la clasificación a un comando (VERDE o ROJO).
    """
    if clasificacion == "priorizacion":
        return ComandoSemaforo(
            interseccion=interseccion,
            estado="VERDE",
            duracion=55,
            motivo=f"prioridad_{tipo_evento}",
        )
    if clasificacion == "congestion":
        return ComandoSemaforo(
            interseccion=interseccion,
            estado="VERDE",
            duracion=45,
            motivo="congestion",
        )
    # Caso normal: rojo estándar
    return ComandoSemaforo(
        interseccion=interseccion,
        estado="ROJO",
        duracion=30,
        motivo="trafico_normal",
    )


# Valida y normaliza el evento entrante
def _extraer_evento(data: Dict[str, Any]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Normaliza mensajes del tipo {"tipo": "camara"|"espira"|"gps", ...}
    """
    tipo = data.get("tipo")
    if tipo not in ("camara", "espira", "gps"):
        return None, None

    # Campos obligatorios según tipo de sensor
    campos_requeridos = {
        "camara": ["sensor_id", "interseccion", "volumen", "velocidad_promedio", "timestamp"],
        "espira": [
            "sensor_id",
            "interseccion",
            "vehiculos_contados",
            "intervalo_segundos",
            "timestamp_inicio",
            "timestamp_fin",
        ],
        "gps": ["sensor_id", "interseccion", "nivel_congestion", "velocidad_promedio", "timestamp"],
    }

    # Verifica que todos los campos estén presentes
    for c in campos_requeridos[tipo]:
        if c not in data:
            return None, None

    return tipo, data


# Envía un diccionario como JSON por socket
def _enviar_json(socket: zmq.Socket, mensaje: Dict[str, Any]) -> None:
    socket.send_string(json.dumps(mensaje, ensure_ascii=False))


def main():
    # Carga configuración (hosts y puertos)
    config = load_config()
    host_pc1 = config["pc1"]["host"]
    host_pc2 = config["pc2"]["host"]
    host_pc3 = config["pc3"]["host"]

    p = config["ports"]
    port_broker_analitica = p["broker_to_analitica"]
    port_analitica_sem = p["analitica_to_semaforos"]
    port_analitica_bd_ppal = p["analitica_to_db_principal"]
    port_analitica_bd_rep = p["analitica_to_db_replica"]

    # Contexto ZeroMQ
    ctx = zmq.Context()

    # SUB: recibe eventos del broker
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{host_pc1}:{port_broker_analitica}")
    sub.setsockopt(zmq.SUBSCRIBE, b"")  # Se suscribe a todos los mensajes

    # PUSH: envía comandos a semáforos
    push_sem = ctx.socket(zmq.PUSH)
    push_sem.connect(f"tcp://{host_pc2}:{port_analitica_sem}")

    # PUSH: persistencia a BD principal
    push_bd_ppal = ctx.socket(zmq.PUSH)
    push_bd_ppal.connect(f"tcp://{host_pc3}:{port_analitica_bd_ppal}")

    # PUSH: persistencia a BD réplica
    push_bd_rep = ctx.socket(zmq.PUSH)
    push_bd_rep.connect(f"tcp://{host_pc2}:{port_analitica_bd_rep}")

    # Pequeña espera para evitar pérdida de mensajes iniciales
    time.sleep(0.3)

    # Log de conexiones
    log_componente(
        COMPONENTE,
        f"SUB conectado a tcp://{host_pc1}:{port_broker_analitica} | "
        f"PUSH semáforos tcp://{host_pc2}:{port_analitica_sem} | "
        f"PUSH BD ppal tcp://{host_pc3}:{port_analitica_bd_ppal} | "
        f"PUSH BD réplica tcp://{host_pc2}:{port_analitica_bd_rep}",
    )

    # Poller para manejar eventos sin bloqueo
    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    try:
        while True:
            socks = dict(poller.poll(timeout=1000))  # Espera eventos
            if sub not in socks:
                continue

            raw = sub.recv_string()  # Recibe mensaje
            try:
                data = json.loads(raw)  # Parsea JSON
            except json.JSONDecodeError:
                log_componente(COMPONENTE, f"JSON inválido: {raw!r}", nivel="ERROR")
                continue

            # Verifica que sea un diccionario
            if not isinstance(data, dict):
                log_componente(COMPONENTE, f"Mensaje no es objeto JSON: {data!r}", nivel="WARN")
                continue

            # Extrae y valida evento
            tipo_evt, payload = _extraer_evento(data)
            if tipo_evt is None or payload is None:
                log_componente(COMPONENTE, f"Evento ignorado o incompleto: {data!r}", nivel="WARN")
                continue

            inter = str(payload["interseccion"])

            # Clasificación según tipo de evento
            if tipo_evt == "camara":
                clasificacion = _clasificar_camara(payload)
            elif tipo_evt == "espira":
                clasificacion = _clasificar_espira(payload)
            else:
                clasificacion = _clasificar_gps(payload)

            # Genera comando de semáforo
            comando = _comando_desde_clasificacion(clasificacion, inter, tipo_evt)
            ts_proceso = generar_timestamp_iso()

            # Log del procesamiento
            log_componente(
                COMPONENTE,
                f"Procesado | tipo={tipo_evt} | {inter} | clasificacion={clasificacion} | "
                f"comando={comando.estado}/{comando.duracion}s",
            )

            # Envía comando a semáforos
            cmd_dict = comando.to_dict()
            _enviar_json(push_sem, cmd_dict)

            # Construye objeto de persistencia
            persistencia: Dict[str, Any] = {
                "origen": "analitica",
                "tipo_evento": tipo_evt,
                "interseccion": inter,
                "clasificacion": clasificacion,
                "evento_original": payload,
                "comando": cmd_dict,
                "timestamp_proceso": ts_proceso,
            }

            # Envía a BD principal y réplica
            _enviar_json(push_bd_ppal, persistencia)
            _enviar_json(push_bd_rep, persistencia)

    except KeyboardInterrupt:
        log_componente(COMPONENTE, "Interrumpido por teclado (Ctrl+C).", nivel="WARN")
    finally:
        # Cierre limpio de sockets y contexto
        sub.close(linger=0)
        push_sem.close(linger=0)
        push_bd_ppal.close(linger=0)
        push_bd_rep.close(linger=0)
        ctx.term()


# Punto de entrada del programa
if __name__ == "__main__":
    main()
