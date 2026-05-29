#!/usr/bin/env python3
"""
Cliente de monitoreo interactivo (PC3).
Interfaz de línea de comandos para consultar el sistema y enviar órdenes.
"""

import json
import os
import sys
from datetime import datetime

import zmq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import load_config
from common.utils import generar_intersecciones_3x3

INTERSECCIONES = generar_intersecciones_3x3()

# =======================
# COLORES ANSI (simplificado)
# =======================
GRN  = "\033[92m"
RED  = "\033[91m"
YLW  = "\033[93m"
BOLD = "\033[1m"
RST  = "\033[0m"

AYUDA = f"""
{BOLD}══════════════════════════════════════════════════════════════
      SISTEMA DE MONITOREO Y CONTROL — TRÁFICO URBANO
══════════════════════════════════════════════════════════════{RST}

{BOLD}Comandos disponibles:{RST}
  {GRN}consultar{RST} <INTER>
  {GRN}historico{RST} <INTER> [N]
  {GRN}rango{RST} <HH:MM> <HH:MM> [INT]
  {YLW}ambulancia{RST} <INTER>
  {YLW}priorizar{RST} <INTER>
  ayuda
  salir

{BOLD}Intersecciones válidas:{RST}
  I11  I12  I13
  I21  I22  I23
  I31  I32  I33
"""

# =======================
# FORMATEO
# =======================

def _color_estado(estado: str) -> str:
    if estado == "VERDE":
        return f"{GRN}{BOLD}VERDE{RST}"
    if estado == "ROJO":
        return f"{RED}{BOLD}ROJO{RST}"
    return f"{YLW}{estado}{RST}"


def _formatear_consultar(data: dict) -> str:
    if "error" in data:
        return f"{RED}Error: {data['error']}{RST}"
    if "info" in data:
        return f"{YLW}{data['info']}{RST}"

    estado = _color_estado(data.get("estado", "?"))
    return (
        f"\n{BOLD}Intersección:{RST} {data.get('interseccion')}\n"
        f"{BOLD}Estado:{RST} {estado}\n"
        f"{BOLD}Motivo:{RST} {data.get('motivo', '-')}\n"
        f"{BOLD}Último cambio:{RST} {data.get('ultimo_cambio', '-')}\n"
        f"{BOLD}Eventos:{RST} {data.get('total_eventos', 0)}"
    )


def _formatear_historico(data: dict) -> str:
    if "error" in data:
        return f"{RED}{data['error']}{RST}"

    historico = data.get("historico", [])
    if not historico:
        return f"{YLW}Sin registros{RST}"

    lineas = [f"\n{BOLD}Histórico {data.get('interseccion')}{RST}"]

    for r in historico:
        estado = _color_estado(r.get("estado", "?"))
        lineas.append(
            f"{r.get('timestamp')}  {estado}  {r.get('duracion')}s  {r.get('motivo')}"
        )

    return "\n".join(lineas)


def _formatear_rango(data: dict) -> str:
    if "error" in data:
        return f"{RED}{data['error']}{RST}"

    acciones = data.get("acciones", [])
    total = data.get("total", 0)

    lineas = [
        f"\n{BOLD}Rango {data.get('desde')} → {data.get('hasta')} ({total}){RST}"
    ]

    if not acciones:
        lineas.append(f"{YLW}Sin registros{RST}")
    else:
        for r in acciones[:40]:
            estado = _color_estado(r.get("estado", "?"))
            lineas.append(
                f"{r.get('timestamp')} {r.get('interseccion')} {estado} {r.get('duracion')}s"
            )

    return "\n".join(lineas)


def _formatear_ambulancia(data: dict) -> str:
    if "error" in data:
        return f"{RED}{data['error']}{RST}"

    if data.get("ok"):
        return (
            f"\n{GRN}{BOLD}✓ Prioridad aplicada{RST}\n"
            f"Intersección: {data.get('interseccion')}\n"
            f"Acción: {data.get('accion')}"
        )

    return f"{RED}No confirmada{RST}"


# =======================
# MAIN
# =======================

def main():
    config = load_config()
    host = config["pc3"]["host"]
    puerto = config["ports"]["monitoreo_to_analitica"]

    ctx = zmq.Context()
    req = ctx.socket(zmq.REQ)
    req.setsockopt(zmq.RCVTIMEO, 5000)

    endpoint = f"tcp://{host}:{puerto}"
    req.connect(endpoint)

    print(AYUDA)
    print(f"{YLW}Conectado a {endpoint}{RST}\n")

    def reset_socket():
        nonlocal req
        req.close()
        req = ctx.socket(zmq.REQ)
        req.setsockopt(zmq.RCVTIMEO, 5000)
        req.connect(endpoint)

    try:
        while True:
            try:
                entrada = input(f"{YLW}monitoreo > {RST}").strip()
            except (EOFError, KeyboardInterrupt):
                break

            if not entrada:
                continue

            partes = entrada.split()
            cmd = partes[0].lower()

            if cmd == "salir":
                break

            if cmd == "ayuda":
                print(AYUDA)
                continue

            # ---------------------
            # VALIDACIÓN
            # ---------------------

            if cmd == "consultar":
                if len(partes) < 2:
                    print("Uso: consultar <INTER>")
                    continue
                inter = partes[1].upper()
                msg = f"consultar {inter}"
                fmt = _formatear_consultar

            elif cmd == "historico":
                inter = partes[1].upper()
                limite = partes[2] if len(partes) > 2 else "5"
                msg = f"historico {inter} {limite}"
                fmt = _formatear_historico

            elif cmd == "rango":
                desde = partes[1]
                hasta = partes[2]
                inter = partes[3].upper() if len(partes) > 3 else ""
                msg = f"rango {desde} {hasta} {inter}".strip()
                fmt = _formatear_rango

            elif cmd in ("ambulancia", "priorizar"):
                inter = partes[1].upper()
                print(f"{YLW}Enviando ambulancia a {inter}...{RST}")
                msg = f"ambulancia {inter}"
                fmt = _formatear_ambulancia

            else:
                print(f"{RED}Comando desconocido{RST}")
                continue

            # ---------------------
            # ENVÍO
            # ---------------------

            req.send_string(msg)

            try:
                data = json.loads(req.recv_string())
                print(fmt(data))
                print()
            except zmq.Again:
                print(f"{RED}Timeout: sin respuesta{RST}")
                reset_socket()

    finally:
        req.close()
        ctx.term()


if __name__ == "__main__":
    main()
