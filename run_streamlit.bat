@echo off
REM Cambiar al directorio del script
cd /d %~dp0

REM Activar el entorno virtual
call .\.venv\Scripts\activate

REM Ejecutar Streamlit
streamlit run streamlitapp\00_Introduction.py

