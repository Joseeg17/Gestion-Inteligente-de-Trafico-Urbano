##!/usr/bin/env python3
"""
Broker ZMQ (PC1).
Patrón PUB/SUB (sensores → broker) → PUB (broker → analítica):
  - Sensores publican eventos via PUB con tópico = tipo de sensor.
  - Broker se suscribe a los 3 tópicos (camara, espira, gps).
  - Reenvía todos los eventos a analítica (PC2) via PUB sin tópico.

Esto cumple el patrón PUB/SUB descrito en el enunciado.
"""

import json
import os
import sys

import zmq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import load_config
from common.utils import log_componente

COMPONENTE = "broker"

# Tópicos a los que el broker se suscribe (uno por tipo de sensor)
TOPICOS = [b"camara", b"espira", b"gps"]

# Prefijos de sensor_id para inferir tipo si no viene en el mensaje
PREFIJO_TIPO = {
    "cam_":    "camara",
    "espira_": "espira",
    "gps_":    "gps",
}


def _inferir_tipo(data: dict) -> str | None:
    sensor_id = str(data.get("sensor_id", ""))
    for prefijo, tipo in PREFIJO_TIPO.items():
        if sensor_id.startswith(prefijo):
            return tipo
    return None


def main():
    config = load_config()
    host_pc1     = config["pc1"]["host"]
    puerto_sub   = config["ports"]["sensor_to_broker"]     # sensores publican aquí
    puerto_pub   = config["ports"]["broker_to_analitica"]  # broker publica aquí

    ctx = zmq.Context()

    # SUB: recibe de los sensores (cada sensor publica con su tópico)
    sub = ctx.socket(zmq.SUB)
    sub.bind(f"tcp://{host_pc1}:{puerto_sub}")
    for topico in TOPICOS:
        sub.setsockopt(zmq.SUBSCRIBE, topico)
    log_componente(COMPONENTE, f"SUB bound tcp://{host_pc1}:{puerto_sub} | tópicos={[t.decode() for t in TOPICOS]}")

    # PUB: reenvía eventos a analítica (PC2)
    pub = ctx.socket(zmq.PUB)
    pub.bind(f"tcp://{host_pc1}:{puerto_pub}")
    log_componente(COMPONENTE, f"PUB bound tcp://{host_pc1}:{puerto_pub}")

    contadores: dict[str, int] = {}

    try:
        while True:
            # Con PUB/SUB el mensaje llega como [tópico, cuerpo]
            partes = sub.recv_multipart()
            if len(partes) < 2:
                log_componente(COMPONENTE, f"Mensaje malformado: {partes!r}", nivel="WARN")
                continue

            topico_bytes = partes[0]
            cuerpo       = partes[1].decode("utf-8")

            try:
                data = json.loads(cuerpo)
            except json.JSONDecodeError:
                log_componente(COMPONENTE, f"JSON inválido: {cuerpo!r}", nivel="ERROR")
                continue

            if not isinstance(data, dict):
                log_componente(COMPONENTE, f"Mensaje no es objeto: {data!r}", nivel="WARN")
                continue

            # Inyectar "tipo" si no viene
            if "tipo" not in data:
                tipo = _inferir_tipo(data)
                if tipo is None:
                    tipo = topico_bytes.decode("utf-8")   # fallback: usar el tópico
                data["tipo"] = tipo

            tipo = data["tipo"]
            contadores[tipo] = contadores.get(tipo, 0) + 1

            # Reenviar a analítica (sin tópico — analítica suscribe a "")
            pub.send_string(json.dumps(data, ensure_ascii=False))

            log_componente(
                COMPONENTE,
                f"Reenviado | tipo={tipo} | inter={data.get('interseccion')} "
                f"| total_{tipo}={contadores[tipo]}",
            )

    except KeyboardInterrupt:
        log_componente(COMPONENTE, "Detenido manualmente.", nivel="WARN")
    finally:
        sub.close(linger=0)
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
