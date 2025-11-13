# 🧠 Genesys Cloud Transcript Extractor – Manifesto Técnico

## Propósito
Este servicio tiene como objetivo **automatizar la extracción de transcripciones** de conversaciones en Genesys Cloud (voz o chat) dentro de un rango de fechas configurable.

---

## Arquitectura
El sistema se compone de módulos simples y desacoplados:
- `auth.py` → maneja la autenticación OAuth2 con client credentials.
- `conversations.py` → lista las conversaciones con transcripciones.
- `transcripts.py` → descarga las transcripciones en formato JSON.
- `logger.py` → gestiona logs estructurados.
- `config.py` → centraliza la configuración desde `.env`.

La app sigue un **patrón de orquestación lineal**:
1. Autenticación  
2. Obtención de conversaciones  
3. Descarga de transcripciones  
4. Registro de actividad  

---

## Estándares de desarrollo
- Código **limpio y legible** (Clean Code).
- Sin dependencias innecesarias.
- Manejo explícito de errores.
- Nombres descriptivos y consistentes.
- Logs y comentarios claros.
- Modularidad total.

---

## Extensibilidad prevista
1. **Versión 2**  
   - Integración con base de datos (PostgreSQL o SQLite).
   - Reintentos automáticos y gestión de errores persistentes.

2. **Versión 3**  
   - Integración con Airflow o cron job.
   - API REST para ejecución remota.

3. **Versión 4 (Opcional)**  
   - Panel web de consulta.
   - Descarga masiva por usuario o cola.

---

## Flujo de mantenimiento para Codex
1. Validar credenciales (`GENESYS_CLIENT_ID`, `GENESYS_CLIENT_SECRET`, `GENESYS_REGION`).
2. Verificar fechas en `.env`.
3. Ejecutar `python src/main.py`.
4. Revisar `/output` y `/logs`.
5. Si se requiere debug, usar:
   ```bash
   python -m src.main
