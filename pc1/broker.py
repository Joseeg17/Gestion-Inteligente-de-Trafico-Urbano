#!/usr/bin/env python3
"""
Broker ZMQ (PC1).
Patrón PUSH/PULL -> PUB:
  - Recibe eventos de los 3 sensores via PULL (puerto sensor_to_broker).
  - Los reenvía a analítica (PC2) via PUB (puerto broker_to_analitica).

Agrega el campo "tipo" al mensaje si no viene incluido,
inferido del campo sensor_id (cam_ / espira_ / gps_).
"""

import json
import os
import sys
import zmq

# Modifica el path de Python para poder importar módulos desde la carpeta raíz del proyecto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importaciones de utilidades internas del proyecto
from common.config_loader import load_config
from common.utils import log_componente

# Identificador de este script para los logs
COMPONENTE = "broker"

# Diccionario de mapeo: Prefijos de sensor_id -> tipo de evento que espera analítica
PREFIJO_TIPO = {
    "cam_":     "camara",
    "espira_": "espira",
    "gps_":    "gps",
}


def _inferir_tipo(data: dict) -> str | None:
    """
    Analiza el 'sensor_id' dentro de los datos para deducir el tipo de dispositivo.
    Retorna el tipo como string o None si no coincide con ningún prefijo conocido.
    """
    # Obtiene el valor de 'sensor_id' de forma segura y lo convierte a string
    sensor_id = str(data.get("sensor_id", ""))
    
    # Busca si el ID comienza con alguno de los prefijos configurados
    for prefijo, tipo in PREFIJO_TIPO.items():
        if sensor_id.startswith(prefijo):
            return tipo  # Devuelve el tipo correspondiente (ej. "camara")
            
    return None  # No se pudo identificar el tipo


def main():
    # 1. CONFIGURACIÓN E INICIALIZACIÓN
    config = load_config()  # Carga el archivo de configuración del sistema
    host_pc1 = config["pc1"]["host"]  # Dirección IP o hostname de la PC1
    puerto_pull = config["ports"]["sensor_to_broker"]   # Puerto para recibir datos de sensores
    puerto_pub  = config["ports"]["broker_to_analitica"] # Puerto para publicar a analítica

    # Inicializa el contexto de ZeroMQ (administra los sockets)
    ctx = zmq.Context()

    # Configura el socket PULL para recolectar datos (los sensores usarán PUSH)
    pull = ctx.socket(zmq.PULL)
    pull.bind(f"tcp://{host_pc1}:{puerto_pull}")

    # Configura el socket PUB para retransmitir datos (Analítica usará SUB)
    pub = ctx.socket(zmq.PUB)
    pub.bind(f"tcp://{host_pc1}:{puerto_pub}")

    # Log de inicialización exitosa
    log_componente(
        COMPONENTE,
        f"PULL bound tcp://{host_pc1}:{puerto_pull} | "
        f"PUB bound tcp://{host_pc1}:{puerto_pub}",
    )

    # Diccionario para llevar el conteo de mensajes procesados por cada tipo de sensor
    contadores: dict[str, int] = {}

    # 2. BUCLE PRINCIPAL DE PROCESAMIENTO
    try:
        while True:
            # Bloquea el hilo hasta recibir un mensaje de texto por el socket PULL
            raw = pull.recv_string()

            # Intenta parsear la cadena recibida a un objeto JSON (diccionario)
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                # Si no es un JSON válido, registra el error y salta al siguiente mensaje
                log_componente(COMPONENTE, f"JSON inválido descartado: {raw!r}", nivel="ERROR")
                continue

            # Validación de estructura: el JSON debe ser un objeto/diccionario de Python
            if not isinstance(data, dict):
                log_componente(COMPONENTE, f"Mensaje no es objeto: {data!r}", nivel="WARN")
                continue

            # 3. ENRIQUECIMIENTO DE DATOS (Inyectar "tipo" si falta)
            if "tipo" not in data:
                tipo = _inferir_tipo(data)
                if tipo is None:
                    # Si no trae tipo y tampoco se puede inferir, se descarta por seguridad
                    log_componente(
                        COMPONENTE,
                        f"No se pudo inferir tipo para sensor_id={data.get('sensor_id')!r}",
                        nivel="WARN",
                    )
                    continue
                data["tipo"] = tipo # Inyecta el tipo inferido en el diccionario

            # 4. MÉTRICAS INTERNAS
            tipo = data["tipo"]
            # Incrementa el contador específico para este tipo de sensor (inicializa en 0 si es nuevo)
            contadores[tipo] = contadores.get(tipo, 0) + 1

            # 5. REENVÍO / PUBLICACIÓN
            # Convierte el diccionario modificado de vuelta a JSON string y lo envía vía PUB
            # 'ensure_ascii=False' permite mantener caracteres especiales (como tildes o eñes) intactos
            pub.send_string(json.dumps(data, ensure_ascii=False))

            # Registra la acción en el log con información del mensaje y estadísticas actuales
            log_componente(
                COMPONENTE,
                f"Reenviado | tipo={tipo} | inter={data.get('interseccion')} "
                f"| total_{tipo}={contadores[tipo]}",
            )

    # 6. LIMPIEZA Y SALIDA SEGURA
    except KeyboardInterrupt:
        # Captura el Ctrl+C para cerrar el programa de forma limpia y controlada
        log_componente(COMPONENTE, "Detenido manualmente.", nivel="WARN")
    finally:
        # Cierra los sockets inmediatamente (linger=0 asegura que no se queden esperando en memoria)
        pull.close(linger=0)
        pub.close(linger=0)
        # Destruye el contexto de ZeroMQ liberando los recursos del sistema operativo
        ctx.term()


if __name__ == "__main__":
    main()
