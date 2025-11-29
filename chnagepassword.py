from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Dict
from uuid import uuid4
import hashlib

app = FastAPI()

users_db: Dict[str, Dict] = {}
tokens_db: Dict[str, str] = {}

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def seed_user():
    username = "trader1"
    password = "oldpassword"
    users_db[username] = {
        "id": 1,
        "username": username,
        "password_hash": hash_password(password),
    }

seed_user()

class LoginRequest(BaseModel):
    username: str
    password: str

class ChangePasswordRequest(BaseModel):
    username: str
    old_password: str
    new_password: str

def get_current_user(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401)

    token = auth_header[len("Bearer "):].strip()
    username = tokens_db.get(token)
    if not username:
        raise HTTPException(status_code=401)

    user = users_db.get(username)
    if not user:
        raise HTTPException(status_code=401)

    return user

@app.post("/login")
def login(body: LoginRequest):
    if not body.username or not body.password:
        raise HTTPException(status_code=400)

    user = users_db.get(body.username)
    if not user:
        raise HTTPException(status_code=401)

    if user["password_hash"] != hash_password(body.password):
        raise HTTPException(status_code=401)

    token = str(uuid4())
    tokens_db[token] = user["username"]
    return {"token": token}

@app.put("/user/password")
def change_password(body: ChangePasswordRequest):
    if not body.username or not body.old_password or not body.new_password:
        raise HTTPException(status_code=400)

    user = users_db.get(body.username)
    if not user:
        raise HTTPException(status_code=401)

    if user["password_hash"] != hash_password(body.old_password):
        raise HTTPException(status_code=401)

    try:
        user["password_hash"] = hash_password(body.new_password)
        tokens_to_delete = [
            token for token, username in tokens_db.items() if username == body.username
        ]
        for token in tokens_to_delete:
            del tokens_db[token]
    except:
        raise HTTPException(status_code=500)

    return Response(status_code=204)

@app.get("/protected")
def protected_route(current_user: Dict = Depends(get_current_user)):
    return {"message": f"Hello, {current_user['username']}!"}
