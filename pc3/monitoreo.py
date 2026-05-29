#!/usr/bin/env python3
"""
Monitoreo (PC3).
- Hilo heartbeat: envía pulso periódico a health_check de PC2.
- Loop REP: atiende consultas del cliente (cliente_monitoreo.py).
  Comandos: consultar, historico, ambulancia/priorizar.
- Comando ambulancia: reenvía orden de VERDE urgente a analítica (PC2)
  mediante un socket REQ separado.
"""

import json
import os
import sqlite3
import sys
import threading
import time

import zmq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config_loader import load_config
from common.db import consultar_historico_rango
from common.utils import generar_intersecciones_3x3, generar_timestamp_iso, log_componente

COMPONENTE   = "monitoreo"
DB_PRINCIPAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pc3_principal.db")
INTERSECCIONES = generar_intersecciones_3x3()


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def enviar_heartbeat(host_pc2: str, puerto: int) -> None:
    ctx  = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    push.connect(f"tcp://{host_pc2}:{puerto}")
    log_componente(COMPONENTE, f"Heartbeat → tcp://{host_pc2}:{puerto}")
    try:
        while True:
            push.send_string("heartbeat")
            log_componente(COMPONENTE, "Heartbeat enviado")
            time.sleep(3)
    except Exception as exc:
        log_componente(COMPONENTE, f"Heartbeat error: {exc}", nivel="ERROR")
    finally:
        push.close(linger=0)
        ctx.term()


# ---------------------------------------------------------------------------
# Consultas BD
# ---------------------------------------------------------------------------

def consultar_interseccion(interseccion: str) -> str:
    try:
        conn   = sqlite3.connect(DB_PRINCIPAL)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM eventos WHERE interseccion = ?", (interseccion,)
        )
        total = cursor.fetchone()[0]
        cursor.execute(
            """SELECT estado, motivo, timestamp
               FROM acciones_semaforo
               WHERE interseccion = ?
               ORDER BY id DESC LIMIT 1""",
            (interseccion,),
        )
        accion = cursor.fetchone()
        conn.close()
        if accion:
            return json.dumps({
                "interseccion":  interseccion,
                "estado":        accion[0],
                "motivo":        accion[1],
                "ultimo_cambio": accion[2],
                "total_eventos": total,
            }, ensure_ascii=False)
        return json.dumps({"interseccion": interseccion, "info": "sin datos aún"})
    except sqlite3.Error as exc:
        return json.dumps({"error": str(exc)})


def consultar_historico(interseccion: str, limite: int = 5) -> str:
    try:
        conn   = sqlite3.connect(DB_PRINCIPAL)
        cursor = conn.cursor()
        cursor.execute(
            """SELECT estado, motivo, duracion, timestamp
               FROM acciones_semaforo
               WHERE interseccion = ?
               ORDER BY id DESC LIMIT ?""",
            (interseccion, limite),
        )
        rows = [
            {"estado": r[0], "motivo": r[1], "duracion": r[2], "timestamp": r[3]}
            for r in cursor.fetchall()
        ]
        conn.close()
        return json.dumps({"interseccion": interseccion, "historico": rows}, ensure_ascii=False)
    except sqlite3.Error as exc:
        return json.dumps({"error": str(exc)})


# ---------------------------------------------------------------------------
# Comando ambulancia → analítica
# ---------------------------------------------------------------------------

def enviar_ambulancia(interseccion: str, host_pc2: str, puerto_analitica: int) -> str:
    """
    Envía una orden de prioridad VERDE urgente directamente a analítica (PC2).
    Usa REQ/REP para confirmar que analítica recibió la orden.
    """
    ctx = zmq.Context()
    req = ctx.socket(zmq.REQ)
    req.setsockopt(zmq.RCVTIMEO, 4000)
    endpoint = f"tcp://{host_pc2}:{puerto_analitica}"
    req.connect(endpoint)

    orden = json.dumps({
        "tipo":         "orden_directa",
        "interseccion": interseccion,
        "estado":       "VERDE",
        "duracion":     60,
        "motivo":       "ambulancia",
        "timestamp":    generar_timestamp_iso(),
    }, ensure_ascii=False)

    log_componente(COMPONENTE, f"Enviando orden AMBULANCIA → {interseccion} a {endpoint}")

    try:
        req.send_string(orden)
        confirmacion = req.recv_string()
        log_componente(COMPONENTE, f"Confirmación de analítica: {confirmacion}")
        return json.dumps({
            "ok":           True,
            "interseccion": interseccion,
            "accion":       "VERDE 60s",
            "motivo":       "ambulancia",
            "confirmacion": confirmacion,
        }, ensure_ascii=False)
    except zmq.Again:
        log_componente(COMPONENTE, "Analítica no respondió (timeout)", nivel="ERROR")
        return json.dumps({"error": "analítica no respondió (timeout 4s)"})
    finally:
        req.close(linger=0)
        ctx.term()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config   = load_config()
    host_pc2 = config["pc2"]["host"]
    puerto_hc          = config["ports"]["healthcheck"]
    puerto_rep         = config["ports"]["monitoreo_to_analitica"]
    puerto_analitica   = config["ports"].get("analitica_ordenes", 5562)

    # Hilo heartbeat
    threading.Thread(
        target=enviar_heartbeat,
        args=(host_pc2, puerto_hc),
        daemon=True,
    ).start()

    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    rep.bind(f"tcp://*:{puerto_rep}")
    log_componente(COMPONENTE, f"REP escuchando en tcp://*:{puerto_rep}")
    log_componente(COMPONENTE, f"Órdenes directas → analítica tcp://{host_pc2}:{puerto_analitica}")

    try:
        while True:
            mensaje = rep.recv_string()
            log_componente(COMPONENTE, f"Solicitud recibida: {mensaje!r}")

            partes  = mensaje.strip().split()
            comando = partes[0].lower() if partes else ""

            # ── consultar ─────────────────────────────────────────
            if comando == "consultar" and len(partes) >= 2:
                inter = partes[1].upper()
                if inter not in INTERSECCIONES:
                    rep.send_string(json.dumps({"error": f"intersección inválida: {inter}"}))
                else:
                    rep.send_string(consultar_interseccion(inter))

            # ── historico ─────────────────────────────────────────
            elif comando == "historico" and len(partes) >= 2:
                inter  = partes[1].upper()
                limite = int(partes[2]) if len(partes) >= 3 else 5
                if inter not in INTERSECCIONES:
                    rep.send_string(json.dumps({"error": f"intersección inválida: {inter}"}))
                else:
                    rep.send_string(consultar_historico(inter, limite))

            # ── rango ─────────────────────────────────────────────
            # Uso: rango <DESDE> <HASTA> [INTER]
            # Ejemplo: rango 06:00 09:00
            #          rango 06:00 09:00 INT_B2
            elif comando == "rango" and len(partes) >= 3:
                desde = partes[1]
                hasta = partes[2]
                inter = partes[3].upper() if len(partes) >= 4 else None
                if inter and inter not in INTERSECCIONES:
                    rep.send_string(json.dumps({"error": f"intersección inválida: {inter}"}))
                else:
                    try:
                        resultado = consultar_historico_rango(DB_PRINCIPAL, desde, hasta, inter)
                        rep.send_string(json.dumps(resultado, ensure_ascii=False))
                    except Exception as exc:
                        rep.send_string(json.dumps({"error": str(exc)}))

            # ── ambulancia / priorizar ────────────────────────────
            elif comando in ("ambulancia", "priorizar") and len(partes) >= 2:
                inter = partes[1].upper()
                if inter not in INTERSECCIONES:
                    rep.send_string(json.dumps({"error": f"intersección inválida: {inter}"}))
                else:
                    resultado = enviar_ambulancia(inter, host_pc2, puerto_analitica)
                    rep.send_string(resultado)

            else:
                rep.send_string(json.dumps({
                    "error": "comando_desconocido",
                    "uso":   "consultar <INTER> | historico <INTER> [N] | ambulancia <INTER>",
                }))

    except KeyboardInterrupt:
        log_componente(COMPONENTE, "Detenido manualmente.", nivel="WARN")
    finally:
        rep.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()

