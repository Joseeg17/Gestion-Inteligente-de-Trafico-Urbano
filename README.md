# 🚦 Gestión Inteligente de Tráfico Urbano

Sistema distribuido para el monitoreo, análisis y control de tráfico urbano en tiempo real, basado en una arquitectura desacoplada con ZeroMQ.

---

## 📌 Descripción

Este sistema simula una ciudad en una cuadrícula 3x3 de intersecciones, donde diferentes sensores generan eventos de tráfico que son procesados en tiempo real.

El sistema permite:

- Detección de congestión en tiempo real
- Control automático de semáforos
- Persistencia distribuida de datos
- Monitoreo centralizado del estado del sistema
- Tolerancia a fallos entre nodos

---

## 🧱 Arquitectura del sistema

| Nodo | Máquina | Responsabilidad |
|------|--------|----------------|
| PC1 | Sensores + Broker | Generación y envío de eventos |
| PC2 | Analítica + Control + BD réplica + Healthcheck | Procesamiento y decisiones |
| PC3 | Base de datos principal + monitoreo | Persistencia y consultas |

---

## 🔌 Comunicación (ZeroMQ)

- PUB/SUB → eventos de sensores
- PUSH/PULL → comandos y persistencia
- REQ/REP → consultas de monitoreo
- PUSH → heartbeat

---

## ⚙️ Componentes del sistema

### 🟢 PC1
- `broker.py`
- sensores (camara / espira / gps)

---

### 🟡 PC2

- `analitica.py` → clasifica tráfico y genera decisiones
- `control_semaforos.py` → aplica cambios de estado
- `bd_replica.py` → persistencia secundaria
- `health_check.py` → monitoreo de disponibilidad de PC3

---

### 🔵 PC3

- `monitoreo.py` → consultas, lógica de respuesta y BD principal

---

## 🚦 Lógica de tráfico

| Estado | Condición | Acción |
|--------|----------|--------|
| Normal | Bajo volumen y velocidad estable | ROJO/VERDE estándar |
| Congestión | Alto flujo o baja velocidad | Extensión de verde |
| Priorización | GPS o evento crítico | Verde inmediato |

---

## 🔁 Tolerancia a fallos

- PC2 ejecuta healthcheck contra PC3
- Si PC3 falla:
  - Se detecta caída por TCP
  - Se registra error en logs
- Cuando vuelve:
  - Se restablece conexión automáticamente

---

## 🧰 Tecnologías

- Python 3.10+
- ZeroMQ (pyzmq)
- SQLite
- JSON
- threading
- sockets TCP

---

## 📁 Estructura del proyecto

```bash
trafico_urbano/
├── config.json
├── pc1/
│   ├── sensor_camara.py
│   ├── sensor_espira.py
│   ├── sensor_gps.py
│   └── broker.py
├── pc2/
│   ├── analitica.py
│   ├── control_semaforos.py
│   ├── bd_replica.py
│   └── health_check.py
└── pc3/
    ├── bd_principal.py
    └── monitoreo.py
```

---

## Ejecución del sistema

### 1. Configuración

Editar el archivo:

```bash
config.json
```

---


---

## ▶️ Cómo ejecutar el sistema

### ⚠️ IMPORTANTE
Ejecutar en este orden:

1. PC3 primero
2. PC2 segundo
3. PC1 al final

---

## 🖥️ Terminales necesarias (6 en total)

---

### 🟦 PC1 (Terminal 1)

```bash
python broker.py
---
```

🟨 PC2 (Terminal 2 - Analítica)
```bash
python analitica.py
```
🟨 PC2 (Terminal 3 - Semáforos)
```bash
python control_semaforos.py
```
🟨 PC2 (Terminal 4 - BD réplica)
```bash
python bd_replica.py
```
🟨 PC2 (Terminal 5 - Healthcheck)
```bash
python healthcheck.py
```

🟩 PC3 (Terminal 6 - Monitoreo)
```bash
python monitoreo.py
```

🧪 Flujo del sistema
```bash
Sensores (PC1)
    ↓
Broker
    ↓
Analítica (PC2)
    ↓
 ┌──────────────┬──────────────┐
 ↓              ↓              ↓
Semáforos   BD réplica     PC3 monitoreo
```

## Autores

* Ricardo Hurtado Forero
* Jose Manuel Guerrero López
* Samuel Enrique Sabogal Giraldo

Pontificia Universidad Javeriana
Sistemas Distribuidos – 2026

---
