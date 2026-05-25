#!/usr/bin/env python3
"""
Healthcheck (PC3).
Envía periódicamente mensajes "heartbeat" a PC2 usando ZeroMQ (PUSH).
"""

import time
import zmq  # Librería de mensajería distribuida

from common.config_loader import load_config  # Carga configuración
from common.utils import log_componente       # Logging común

COMPONENTE = "healthcheck"


def main():
    # Carga la configuración del sistema
    config = load_config()

    # Obtiene host de PC2 (destino del heartbeat)
    host_pc2 = config["pc2"]["host"]

    # Obtiene puerto configurado para healthcheck
    puerto = config["ports"]["healthcheck"]

    # Inicializa el contexto de ZeroMQ
    ctx = zmq.Context()

    # Crea un socket tipo PUSH (envío unidireccional)
    socket = ctx.socket(zmq.PUSH)

    # Construye endpoint TCP
    endpoint = f"tcp://{host_pc2}:{puerto}"

    # Conecta al receptor (PC2)
    socket.connect(endpoint)

    # Log de conexión inicial
    log_componente(
        COMPONENTE,
        f"Heartbeat conectado a {endpoint}"
    )

    try:
        while True:
            # Envía mensaje de latido (heartbeat)
            socket.send_string("heartbeat")

            # Registra envío
            log_componente(
                COMPONENTE,
                "Heartbeat enviado"
            )

            # Espera antes del siguiente envío
            time.sleep(3)

    except KeyboardInterrupt:
        # Permite detener el proceso manualmente
        log_componente(
            COMPONENTE,
            "Health check detenido",
            nivel="WARN"
        )

    finally:
        # Cierre limpio del socket y contexto
        socket.close(linger=0)
        ctx.term()


# Punto de entrada del programa
if __name__ == "__main__":
    main()
