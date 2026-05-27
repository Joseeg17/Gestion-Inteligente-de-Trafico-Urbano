#!/usr/bin/env python3
"""
Analítica (PC2) — con failover automático de BD.

Sockets:
  - SUB  : eventos del broker         (broker_to_analitica)
  - SUB  : estado de PC3 del hc       (healthcheck)
  - PUSH : comandos a semáforos       (analitica_to_semaforos)
  - PUSH : BD principal (PC3)         (analitica_to_db_principal)   ← activo/inactivo según failover
  - PUSH : BD réplica   (PC2)         (analitica_to_db_replica)     ← siempre activo

Lógica de failover:
  - Si health_check publica "PC3_DOWN": se deja de enviar a BD principal
    y todos los registros van solo a la réplica.
  - Si health_check publica "PC3_UP": se reactiva el envío a BD principal.
  - La réplica SIEMPRE recibe, independientemente del estado de PC3.
"""

import json
import os
import sys
import time
import threading
from typing import Any, Dict, Optional, Tuple

import zmq

# Modifica el path de Python para permitir importaciones desde la raíz del proyecto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importaciones de modelos e infraestructura común
from common.config_loader import load_config
from common.models import ComandoSemaforo
from common.utils import generar_timestamp_iso, log_componente

# Identificador para el registro de logs
COMPONENTE = "analitica"

# Constantes de negocio para umbrales de decisión analítica
UMBRAL_VOLUMEN_CONGESTION    = 28      # Cantidad límite de autos para activar congestión (Cámaras)
UMBRAL_VELOCIDAD_BAJA        = 22.0    # Velocidad límite inferior en km/h (Cámaras)
UMBRAL_VEH_POR_SEG_CONGESTION = 1.8     # Relación matemática límite de vehículos/segundo (Espiras)

# Constantes de control de estado provenientes del Health Check
MSG_UP   = "PC3_UP"
MSG_DOWN = "PC3_DOWN"


# ---------------------------------------------------------------------------
# Estado compartido de failover (Hilos concurrentes de forma segura)
# ---------------------------------------------------------------------------
class EstadoFailover:
    """
    Encapsula una variable booleana protegida por un Lock (Exclusión mutua)
    para evitar condiciones de carrera (Race Conditions) entre el hilo monitor y el principal.
    """
    def __init__(self):
        self._pc3_activo = True
        self._lock = threading.Lock() # Lock primitivo de sincronización

    @property
    def pc3_activo(self) -> bool:
        # Bloquea de forma segura antes de leer el estado
        with self._lock:
            return self._pc3_activo

    @pc3_activo.setter
    def pc3_activo(self, valor: bool) -> None:
        # Bloquea de forma segura antes de mutar la variable en memoria
        with self._lock:
            self._pc3_activo = valor


# ---------------------------------------------------------------------------
# Clasificación algorítmica de eventos
# ---------------------------------------------------------------------------

def _clasificar_camara(payload: Dict[str, Any]) -> str:
    """Evalúa volumen y velocidad instantánea para determinar congestión por cámaras."""
    volumen = int(payload.get("volumen", 0))
    vel     = float(payload.get("velocidad_promedio", 0.0))
    if volumen >= UMBRAL_VOLUMEN_CONGESTION or vel < UMBRAL_VELOCIDAD_BAJA:
        return "congestion"
    return "trafico_normal"


def _clasificar_espira(payload: Dict[str, Any]) -> str:
    """Determina congestión calculando la tasa matemática de paso por la espira."""
    veh      = int(payload.get("vehiculos_contados", 0))
    intervalo = max(int(payload.get("intervalo_segundos", 1)), 1) # Evita división por cero
    if (veh / float(intervalo)) >= UMBRAL_VEH_POR_SEG_CONGESTION:
        return "congestion"
    return "trafico_normal"


def _clasificar_gps(payload: Dict[str, Any]) -> str:
    """Clasifica los eventos GPS de vehículos priorizando caídas severas de velocidad."""
    nivel = str(payload.get("nivel_congestion", "bajo")).strip().lower()
    vel   = float(payload.get("velocidad_promedio", 99.0))
    if nivel == "alto" or vel < 12.0:
        return "priorizacion" # Estado crítico, requiere apertura inmediata del semáforo
    if nivel == "medio" or vel < 20.0:
        return "congestion"
    return "trafico_normal"


def _comando_desde_clasificacion(
    clasificacion: str, interseccion: str, tipo_evento: str
) -> ComandoSemaforo:
    """
    Fábrica lógica (Factory Pattern) que mapea el estado del tráfico deducido
    con un comando binario para los controladores de semáforos.
    """
    if clasificacion == "priorizacion":
        return ComandoSemaforo(interseccion=interseccion, estado="VERDE",
                               duracion=55, motivo=f"prioridad_{tipo_evento}")
    if clasificacion == "congestion":
        return ComandoSemaforo(interseccion=interseccion, estado="VERDE",
                               duracion=45, motivo="congestion")
    # Por defecto, si el tráfico es normal mantiene el orden de flujo estándar
    return ComandoSemaforo(interseccion=interseccion, estado="ROJO",
                           duracion=30, motivo="trafico_normal")


def _extraer_evento(
    data: Dict[str, Any]
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Validador de contratos/esquemas JSON de entrada.
    Asegura que cada payload contenga estrictamente todas las llaves requeridas por su sensor.
    """
    tipo = data.get("tipo")
    if tipo not in ("camara", "espira", "gps"):
        return None, None
    
    # Listas de campos obligatorios por tipología
    campos = {
        "camara": ["sensor_id", "interseccion", "volumen", "velocidad_promedio", "timestamp"],
        "espira": ["sensor_id", "interseccion", "vehiculos_contados",
                   "intervalo_segundos", "timestamp_inicio", "timestamp_fin"],
        "gps":    ["sensor_id", "interseccion", "nivel_congestion",
                   "velocidad_promedio", "timestamp"],
    }
    # Verifica la existencia de todos los campos definidos en el contrato
    if not all(c in data for c in campos[tipo]):
        return None, None
    return tipo, data


def _enviar_json(sock: zmq.Socket, mensaje: Dict[str, Any]) -> None:
    """Utilitario para serializar a string JSON y emitir datos por un socket ZMQ."""
    sock.send_string(json.dumps(mensaje, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Hilo monitor de health check
# ---------------------------------------------------------------------------

def _hilo_monitor_healthcheck(
    host_pc2: str,
    puerto_hc: int,
    estado: EstadoFailover,
) -> None:
    """
    Escucha las notificaciones del health_check por un canal SUB y actualiza el estado de failover.
    Corre de forma asíncrona en un hilo daemon secundario para no bloquear las decisiones de tráfico.
    """
    ctx_hilo = zmq.Context()
    sub_hc = ctx_hilo.socket(zmq.SUB)
    sub_hc.connect(f"tcp://{host_pc2}:{puerto_hc}")
    sub_hc.setsockopt(zmq.SUBSCRIBE, b"") # Se suscribe a todos los mensajes del canal de salud

    log_componente(COMPONENTE, f"Monitor HC conectado a tcp://{host_pc2}:{puerto_hc}")

    try:
        while True:
            try:
                # Lectura no bloqueante para poder iterar o pausar sin congelar recursos del sistema
                msg = sub_hc.recv_string(flags=zmq.NOBLOCK)
            except zmq.Again:
                time.sleep(0.5) # Espera medio segundo si la cola está vacía
                continue

            msg = msg.strip()

            # CASO DE FALLA: El supervisor reporta que la base de datos principal murió
            if msg == MSG_DOWN and estado.pc3_activo:
                estado.pc3_activo = False # Modifica la bandera compartida de forma atómica
                log_componente(
                    COMPONENTE,
                    "⚠  FAILOVER ACTIVADO — PC3 caído. "
                    "Persistencia redirigida a BD réplica (PC2).",
                    nivel="WARN",
                )

            # CASO DE RECUPERACIÓN: La base de datos principal volvió a la vida
            elif msg == MSG_UP and not estado.pc3_activo:
                estado.pc3_activo = True # Restaura el flujo regular
                log_componente(
                    COMPONENTE,
                    "✓  RECUPERACIÓN — PC3 disponible nuevamente. "
                    "Persistencia restaurada a BD principal (PC3).",
                )

    except Exception as exc:
        log_componente(COMPONENTE, f"Monitor HC error inesperado: {exc}", nivel="ERROR")
    finally:
        sub_hc.close(linger=0)
        ctx_hilo.term()


# ---------------------------------------------------------------------------
# Main (Hilo Principal - Loop de Mensajería)
# ---------------------------------------------------------------------------

def main():
    # 1. CARGA MAPAS DE DIRECCIONES DE RED
    config = load_config()
    host_pc1 = config["pc1"]["host"] # IP del Broker
    host_pc2 = config["pc2"]["host"] # IP local (Analítica / Réplica / HC)
    host_pc3 = config["pc3"]["host"] # IP de la Base de Datos Principal
    p        = config["ports"]

    ctx = zmq.Context()

    # 2. INICIALIZACIÓN DE LA MALLA DE SOCKETS ZERO MQ
    # Socket SUB para absorber telemetría del broker (PC1)
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{host_pc1}:{p['broker_to_analitica']}")
    sub.setsockopt(zmq.SUBSCRIBE, b"")

    # Socket PUSH para mandar comandos de semáforos locales
    push_sem = ctx.socket(zmq.PUSH)
    push_sem.connect(f"tcp://{host_pc2}:{p['analitica_to_semaforos']}")

    # Socket PUSH remoto hacia el gestor SQLite en la PC3
    push_bd_ppal = ctx.socket(zmq.PUSH)
    push_bd_ppal.connect(f"tcp://{host_pc3}:{p['analitica_to_db_principal']}")

    # Socket PUSH local para escribir siempre la réplica de seguridad en la PC2
    push_bd_rep = ctx.socket(zmq.PUSH)
    push_bd_rep.connect(f"tcp://{host_pc2}:{p['analitica_to_db_replica']}")

    time.sleep(0.3) # Sincronización intermedia de conexiones tcp

    # 3. LANZAMIENTO DEL CIRCUITO DE FAILOVER DE CONCURRENCIA
    estado = EstadoFailover()

    # Se inicia el monitor de salud en segundo plano (daemon=True garantiza cierre automático si main muere)
    t = threading.Thread(
        target=_hilo_monitor_healthcheck,
        args=(host_pc2, p["healthcheck"], estado),
        daemon=True,
    )
    t.start()

    log_componente(
        COMPONENTE,
        f"SUB broker tcp://{host_pc1}:{p['broker_to_analitica']} | "
        f"PUSH semáforos tcp://{host_pc2}:{p['analitica_to_semaforos']} | "
        f"PUSH BD ppal tcp://{host_pc3}:{p['analitica_to_db_principal']} | "
        f"PUSH BD réplica tcp://{host_pc2}:{p['analitica_to_db_replica']} | "
        f"Failover: ACTIVO",
    )

    # Registro de sockets en el orquestador Poller
    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    # 4. ENRUTAMIENTO, ANÁLISIS Y CIRCUITO REDUNDANTE DE BD
    try:
        while True:
            # Espera activa de datos con timeout de 1 segundo
            socks = dict(poller.poll(timeout=1000))
            if sub not in socks:
                continue

            raw = sub.recv_string()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                log_componente(COMPONENTE, f"JSON inválido: {raw!r}", nivel="ERROR")
                continue

            if not isinstance(data, dict):
                continue

            # Valida el esquema y extrae el payload limpio
            tipo_evt, payload = _extraer_evento(data)
            if tipo_evt is None:
                log_componente(COMPONENTE, f"Evento ignorado: {data!r}", nivel="WARN")
                continue

            inter = str(payload["interseccion"])

            # 5. MÁQUINA DE INFERENCIA ESTADÍSTICA
            if tipo_evt == "camara":
                clasificacion = _clasificar_camara(payload)
            elif tipo_evt == "espira":
                clasificacion = _clasificar_espira(payload)
            else:
                clasificacion = _clasificar_gps(payload)

            # Genera la estructura del comando final del semáforo
            comando    = _comando_desde_clasificacion(clasificacion, inter, tipo_evt)
            ts_proceso = generar_timestamp_iso()

            # Evalúa el flag atómico de failover para fines descriptivos en logs
            destino = "BD_PRINCIPAL" if estado.pc3_activo else "BD_REPLICA(failover)"
            log_componente(
                COMPONENTE,
                f"tipo={tipo_evt} | {inter} | {clasificacion} | "
                f"{comando.estado}/{comando.duracion}s | destino={destino}",
            )

            # Transmite la orden de color y temporizador al socket del semáforo
            cmd_dict = comando.to_dict()
            _enviar_json(push_sem, cmd_dict)

            # Estructura el payload unificado enriquecido para persistencia histórica
            persistencia: Dict[str, Any] = {
                "origen":           "analitica",
                "tipo_evento":      tipo_evt,
                "interseccion":     inter,
                "clasificacion":    clasificacion,
                "evento_original":  payload,
                "comando":          cmd_dict,
                "timestamp_proceso": ts_proceso,
            }

            # ARQUITECTURA DE DATOS REDUNDANTE:
            # La réplica local de la PC2 SIEMPRE recibe el mensaje para evitar pérdida de trazas históricas
            _enviar_json(push_bd_rep, persistencia)

            # La base de datos central en la PC3 solo recibe datos si la bandera de failover está en True
            if estado.pc3_activo:
                _enviar_json(push_bd_ppal, persistencia)

    # 6. PROCEDIMIENTO DE APAGADO DE COMPONENTES
    except KeyboardInterrupt:
        log_componente(COMPONENTE, "Interrumpido por teclado (Ctrl+C).", nivel="WARN")
    finally:
        # Cierre ordenado de recursos de sockets abiertos en memoria
        sub.close(linger=0)
        push_sem.close(linger=0)
        push_bd_ppal.close(linger=0)
        push_bd_rep.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
