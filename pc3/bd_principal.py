#!/usr/bin/env python3
"""
Base de datos principal (PC3).
- Recibe por PULL los registros de analítica (puerto analitica_to_db_principal).
- Persiste eventos y acciones de semáforo en SQLite (pc3_principal.db).
- Expone un socket REP para consultas de sincronización cuando PC3 regresa
  tras una caída: devuelve el último estado conocido de cada intersección.
"""

import json
import os
import sys
import zmq

# Modifica el path de Python para permitir importaciones desde la raíz del proyecto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importa las funciones del conector de base de datos y utilidades globales
from common.config_loader import load_config
from common.db import (
    consultar_estado_general,
    guardar_accion,
    guardar_evento,
    inicializar_db,
)
from common.utils import log_componente

# Configuración de constantes globales del componente
COMPONENTE = "bd_principal"
DB_NAME = "pc3_principal.db"


def _procesar_mensaje(data: dict, db_path: str) -> None:
    """
    Valida y persiste un mensaje de analítica en la BD principal.
    Desglosa el payload tanto para registrar el evento base como la orden de tráfico generada.
    """
    # 1. VALIDACIÓN DE CAMPOS CRÍTICOS EN EL JSON PRINCIPAL
    campos_req = ["tipo_evento", "interseccion", "evento_original", "timestamp_proceso"]
    if not all(k in data for k in campos_req):
        log_componente(COMPONENTE, f"Mensaje incompleto descartado: {list(data.keys())}", nivel="WARN")
        return

    # Extracción y casteo de variables raíz
    tipo_evento  = str(data["tipo_evento"])
    interseccion = str(data["interseccion"])
    ts           = str(data["timestamp_proceso"])
    evento_orig  = data["evento_original"]

    # Valida que el nodo 'evento_original' sea una estructura de objeto válida
    if not isinstance(evento_orig, dict):
        log_componente(COMPONENTE, "evento_original no es objeto JSON.", nivel="WARN")
        return

    # Extrae el ID del sensor de origen o le asigna fallback por seguridad
    sensor_id  = str(evento_orig.get("sensor_id", "desconocido"))
    datos_json = json.dumps(data, ensure_ascii=False)

    # 2. PERSISTENCIA DEL EVENTO DE TELEMETRÍA
    id_ev = guardar_evento(
        db_path=db_path,
        tipo_evento=tipo_evento,
        sensor_id=sensor_id,
        interseccion=interseccion,
        datos_json=datos_json,
        timestamp=ts,
    )
    log_componente(
        COMPONENTE,
        f"Evento guardado id={id_ev} | tipo={tipo_evento} | {interseccion} | sensor={sensor_id}",
    )

    # 3. COMPROBACIÓN Y PERSISTENCIA DE LA ACCIÓN DEL SEMÁFORO
    comando = data.get("comando")
    if not isinstance(comando, dict):
        log_componente(COMPONENTE, f"Sin bloque 'comando' en mensaje de {interseccion}.", nivel="WARN")
        return

    # Verifica la integridad estructural del comando de control vial
    campos_cmd = ("interseccion", "estado", "duracion", "motivo")
    if not all(k in comando for k in campos_cmd):
        log_componente(COMPONENTE, f"Comando incompleto: {comando!r}", nivel="WARN")
        return

    # Inserta la acción de semáforo tomada por analítica en la tabla SQLite secundaria
    id_ac = guardar_accion(
        db_path=db_path,
        interseccion=str(comando["interseccion"]),
        estado=str(comando["estado"]),
        duracion=int(comando["duracion"]),
        motivo=str(comando["motivo"]),
        timestamp=ts,
    )
    log_componente(
        COMPONENTE,
        f"Acción guardada id={id_ac} | {comando['interseccion']} -> "
        f"{comando['estado']} ({comando['duracion']}s) | {comando['motivo']}",
    )


def _manejar_sincronizacion(rep_socket: zmq.Socket, db_path: str) -> None:
    """
    Responde a una solicitud de sincronización de estado (Patrón REQ/REP).
    PC2 puede preguntar 'SYNC_ESTADO' y recibe el resumen general acumulado en la BD.
    """
    try:
        # Intenta recibir el comando de texto de forma no bloqueante
        solicitud = rep_socket.recv_string(flags=zmq.NOBLOCK)
    except zmq.Again:
        # Si no hay datos listos en la cola del socket REP, sale de la función inmediatamente
        return

    log_componente(COMPONENTE, f"Solicitud de sincronización recibida: {solicitud!r}")

    # Evalúa la palabra clave de sincronización eliminando espacios y mayúsculas
    if solicitud.strip().upper() == "SYNC_ESTADO":
        # Ejecuta la consulta analítica en SQLite
        resumen = consultar_estado_general(db_path)
        # Responde con el objeto JSON serializado al cliente REQ
        rep_socket.send_string(json.dumps(resumen, ensure_ascii=False))
        log_componente(
            COMPONENTE,
            f"Sincronización respondida | total_eventos={resumen['total_eventos']} "
            f"| total_acciones={resumen['total_acciones_semaforo']}",
        )
    else:
        # Envía un JSON con un mensaje de error si el comando es inválido para no colgar el patrón REQ/REP
        rep_socket.send_string(json.dumps({"error": "solicitud_desconocida"}))


def main():
    # 1. CONFIGURACIÓN E INICIALIZACIÓN DE LA BASE DE DATOS
    config = load_config()
    host_pc3    = config["pc3"]["host"]                         # IP o Host donde corre este script (PC3)
    puerto_pull = config["ports"]["analitica_to_db_principal"]  # Puerto para recolectar datos de analítica

    # Resuelve y configura la ruta física absoluta de SQLite
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path  = os.path.join(base_dir, DB_NAME)
    inicializar_db(db_path) # Crea las tablas en el fichero si no existen previamente
    log_componente(COMPONENTE, f"SQLite listo en {db_path}")

    # 2. CONFIGURACIÓN DE RED ZERO MQ (MULTIPLE SOCKET BIND)
    ctx = zmq.Context()

    # Socket PULL: recibe de manera asíncrona y unidireccional los datos procesados
    pull = ctx.socket(zmq.PULL)
    endpoint_pull = f"tcp://{host_pc3}:{puerto_pull}"
    pull.bind(endpoint_pull)
    log_componente(COMPONENTE, f"PULL escuchando en {endpoint_pull} (analítica -> BD principal)")

    # Socket REP: canal síncrono bidireccional (Pregunta/Respuesta) para recuperación ante caídas
    rep = ctx.socket(zmq.REP)
    puerto_sync = puerto_pull + 100   # Derivación matemática del puerto (Ej: 5557 + 100 = 5657)
    endpoint_rep = f"tcp://{host_pc3}:{puerto_sync}"
    rep.bind(endpoint_rep)
    log_componente(COMPONENTE, f"REP sincronización en {endpoint_rep}")

    # 3. REGISTRO EN EL ORQUESTADOR DE EVENTOS (POLLER)
    # Permite monitorear la llegada de bytes en múltiples sockets de forma concurrente sin hilos
    poller = zmq.Poller()
    poller.register(pull, zmq.POLLIN) # Monitorea entrada de datos en el canal PULL
    poller.register(rep,  zmq.POLLIN) # Monitorea entrada de datos en el canal REP

    mensajes_procesados = 0

    # 4. BUCLE ASÍNCRONO CENTRAL
    try:
        while True:
            # Espera activa (bloqueante) hasta que ocurra un evento en los sockets registrados (Timeout: 2000ms)
            eventos = dict(poller.poll(timeout=2000))

            # CANAL A: Llegada de nuevos datos de analítica desde la PC2
            if pull in eventos:
                raw = pull.recv_string()
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    log_componente(COMPONENTE, f"JSON inválido: {raw!r}", nivel="ERROR")
                    continue

                if not isinstance(data, dict):
                    log_componente(COMPONENTE, f"Mensaje no es objeto: {data!r}", nivel="WARN")
                    continue

                # Invoca la subrutina interna de persistencia
                _procesar_mensaje(data, db_path)
                mensajes_procesados += 1

                # Métrica informativa: Cada 50 inserciones vuelca una estadística general al log
                if mensajes_procesados % 50 == 0:
                    resumen = consultar_estado_general(db_path)
                    log_componente(
                        COMPONENTE,
                        f"Resumen | procesados={mensajes_procesados} "
                        f"| total_eventos={resumen['total_eventos']} "
                        f"| total_acciones={resumen['total_acciones_semaforo']}",
                    )

            # CANAL B: Peticiones entrantes de sincronización o auditoría
            if rep in eventos:
                _manejar_sincronizacion(rep, db_path)

    # 5. DESCONEXIÓN Y CIERRE SEGURO DEL COMPONENTE
    except KeyboardInterrupt:
        log_componente(COMPONENTE, "Detenido manualmente.", nivel="WARN")
        # Genera un último reporte en consola sobre el estado final antes de morir
        resumen = consultar_estado_general(db_path)
        log_componente(
            COMPONENTE,
            f"Total al cierre | eventos={resumen['total_eventos']} "
            f"| acciones={resumen['total_acciones_semaforo']}",
        )
    finally:
        # Libera de inmediato los puertos e inactiva los sockets y el contexto de red
        pull.close(linger=0)
        rep.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
