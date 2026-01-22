from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets

from env_variables import USERS

# 1. Definimos el esquema de seguridad
security = HTTPBasic()

# 2. Función de validación
def verify_basic_auth(credentials: HTTPBasicCredentials = Depends(security)):
    username = credentials.username
    password = credentials.password

    if username not in USERS:
        raise HTTPException(status_code=401, headers={"WWW-Authenticate": "Basic"})

    correct_password = USERS[username]

    if not secrets.compare_digest(password, correct_password):
        raise HTTPException(status_code=401, headers={"WWW-Authenticate": "Basic"})

    return username
