#!/usr/bin/env python3
"""
Control de semáforos (PC2).
Recibe comandos PUSH/PULL desde analítica en el puerto analitica_to_semaforos.
"""

import json
import os
import sys

import zmq  # Librería para comunicación distribuida

# Añade el directorio padre al path para importar módulos comunes
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import load_config  # Carga configuración del sistema
from common.utils import generar_intersecciones_3x3, log_componente  # Utilidades

COMPONENTE = "control_semaforos"
ESTADOS_PERMITIDOS = frozenset({"VERDE", "ROJO"})  # Estados válidos


class Semaforo:
    """Representa un semáforo en una intersección (solo ROJO o VERDE)."""

    def __init__(self, interseccion: str, estado: str = "ROJO"):
        self.interseccion = interseccion
        # Inicializa estado en mayúsculas, por defecto ROJO
        self.estado = estado.upper() if estado else "ROJO"

    def aplicar(self, nuevo_estado: str) -> bool:
        """
        Cambia el estado si es válido y distinto al actual.
        Devuelve True si hubo cambio.
        """
        s = nuevo_estado.strip().upper()

        # Valida que el estado esté permitido
        if s not in ESTADOS_PERMITIDOS:
            return False

        # Si no hay cambio, no hace nada
        if s == self.estado:
            return False

        # Aplica cambio
        self.estado = s
        return True


def _normalizar_comando(msg: dict) -> dict | None:
    """Valida campos mínimos del JSON recibido."""
    if not isinstance(msg, dict):
        return None

    # Verifica campos obligatorios
    for clave in ("interseccion", "estado", "duracion", "motivo"):
        if clave not in msg:
            return None

    return msg


def main():
    # Carga configuración (host y puerto)
    config = load_config()
    host_pc2 = config["pc2"]["host"]
    puerto = config["ports"]["analitica_to_semaforos"]

    # Genera intersecciones (grid 3x3) y crea semáforos iniciales en ROJO
    intersecciones = generar_intersecciones_3x3()
    semaforos = {codigo: Semaforo(codigo, "ROJO") for codigo in intersecciones}

    # Configura socket PULL para recibir comandos desde analítica
    ctx = zmq.Context()
    socket = ctx.socket(zmq.PULL)
    endpoint = f"tcp://{host_pc2}:{puerto}"
    socket.bind(endpoint)

    log_componente(COMPONENTE, f"PULL escuchando en {endpoint} (esperando PUSH de analítica)")

    try:
        while True:
            # Recibe mensaje
            raw = socket.recv_string()
            try:
                data = json.loads(raw)  # Parsea JSON
            except json.JSONDecodeError:
                log_componente(COMPONENTE, f"Mensaje JSON inválido: {raw!r}", nivel="ERROR")
                continue

            # Valida comando
            cmd = _normalizar_comando(data)
            if cmd is None:
                log_componente(COMPONENTE, f"Comando incompleto o no es objeto: {data!r}", nivel="WARN")
                continue

            # Extrae campos del comando
            inter = str(cmd["interseccion"]).strip()
            estado_nuevo = str(cmd["estado"]).strip().upper()
            duracion = cmd["duracion"]
            motivo = str(cmd["motivo"]).strip()

            # Verifica que la intersección exista
            if inter not in semaforos:
                log_componente(COMPONENTE, f"Intersección desconocida: {inter}", nivel="WARN")
                continue

            # Verifica estado válido
            if estado_nuevo not in ESTADOS_PERMITIDOS:
                log_componente(
                    COMPONENTE,
                    f"Estado no permitido (solo VERDE/ROJO): {estado_nuevo!r}",
                    nivel="WARN",
                )
                continue

            sem = semaforos[inter]
            anterior = sem.estado

            # Aplica el cambio de estado
            cambio = sem.aplicar(estado_nuevo)

            if cambio:
                # Log del cambio
                log_componente(
                    COMPONENTE,
                    (
                        f"CAMBIO | {inter}: {anterior} -> {sem.estado} | "
                        f"duracion_s={duracion} | motivo={motivo}"
                    ),
                )

                # Salida por consola
                print(
                    f"[{inter}] Semáforo: {anterior} -> {sem.estado} "
                    f"(duración solicitada: {duracion}s, motivo: {motivo})"
                )
            else:
                # Si no hubo cambio (estado igual)
                if anterior == estado_nuevo:
                    log_componente(
                        COMPONENTE,
                        f"Sin cambio | {inter} ya en {anterior} | motivo={motivo}",
                    )

    except KeyboardInterrupt:
        # Manejo de interrupción manual
        log_componente(COMPONENTE, "Interrumpido por teclado (Ctrl+C).", nivel="WARN")
    finally:
        # Cierre limpio de recursos
        socket.close(linger=0)
        ctx.term()


# Punto de entrada del programa
if __name__ == "__main__":
    main()
