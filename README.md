# Gestion-Inteligente-de-Trafico-Urbano

Sistema distribuido para el monitoreo, análisis y control del tráfico en tiempo real, basado en una arquitectura desacoplada y comunicación mediante ZeroMQ.

## Descripción

Este proyecto implementa una plataforma distribuida que simula una ciudad organizada en una cuadrícula de 3x3 intersecciones. En cada intersección operan distintos sensores que generan eventos de tráfico, los cuales son procesados para:

* Detectar congestión
* Controlar semáforos automáticamente
* Permitir monitoreo y control manual
* Garantizar tolerancia a fallos

El sistema está dividido en tres nodos (PC1, PC2, PC3), cada uno con responsabilidades específicas.

---

## Arquitectura del Sistema

El sistema sigue una arquitectura distribuida por capas:

| Capa                     | Máquina | Responsabilidad                                          |
| ------------------------ | ------- | -------------------------------------------------------- |
| Sensores y Broker        | PC1     | Generación y publicación de eventos                      |
| Analítica y Control      | PC2     | Procesamiento, toma de decisiones y control de semáforos |
| Monitoreo y Persistencia | PC3     | Base de datos, consultas y control manual                |

### Patrones de comunicación (ZeroMQ)

* PUB/SUB: transmisión de eventos desde sensores
* PUSH/PULL: persistencia en base de datos
* REQ/REP: consultas del usuario

---

## Funcionalidades principales

* Simulación de sensores:

  * Cámara
  * Espira inductiva
  * GPS

* Detección de estados de tráfico:

  * Tráfico normal
  * Congestión
  * Priorización de vía

* Control automático de semáforos

* Consultas desde el módulo de monitoreo:

  * Estado actual
  * Histórico
  * Congestión histórica

* Comandos manuales:

  * `PRIORIZAR_VIA`
  * `CAMBIO_MANUAL`

---

## Reglas de tráfico

| Estado       | Condición                    | Acción                   |
| ------------ | ---------------------------- | ------------------------ |
| Normal       | Lq < 5, Vp > 35, Cv < 10     | Ciclo estándar           |
| Congestión   | Lq ≥ 5 OR Vp ≤ 20 OR Cv ≥ 20 | Extender tiempo en verde |
| Priorización | Comando manual               | Verde inmediato          |

---

## Tolerancia a fallos

El sistema implementa un mecanismo de failover automático:

* Health Check desde PC2 hacia PC3
* Si PC3 falla:

  * Se redirige la persistencia a la base de datos réplica en PC2
* Recuperación automática cuando PC3 vuelve a estar disponible

---

## Tecnologías utilizadas

* Python 3.10+
* ZeroMQ (pyzmq)
* SQLite
* JSON
* threading y time
* matplotlib (para métricas)

---

## Estructura del proyecto

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

### 2. Ejecución por nodo

#### PC1 (Sensores y Broker)

```bash
python sensor_camara.py
python sensor_espira.py
python sensor_gps.py
python broker.py
```

#### PC2 (Analítica y Control)

```bash
python analitica.py
python control_semaforos.py
python bd_replica.py
python health_check.py
```

#### PC3 (Base de datos y Monitoreo)

```bash
python bd_principal.py
python monitoreo.py
```
---

## Seguridad

* Validación de mensajes JSON
* Control de acceso centralizado en el módulo de monitoreo
* Trazabilidad mediante timestamps

---

## Autores

* Ricardo Hurtado Forero
* Jose Manuel Guerrero López
* Samuel Enrique Sabogal Giraldo

Pontificia Universidad Javeriana
Sistemas Distribuidos – 2026

---
