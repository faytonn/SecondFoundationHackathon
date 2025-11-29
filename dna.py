from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Dict, List
from uuid import uuid4
import hashlib

app = FastAPI()

users_db: Dict[str, Dict] = {}
tokens_db: Dict[str, str] = {}

# username -> list of DNA samples (strings)
dna_db: Dict[str, List[str]] = {}


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


class DnaSubmitRequest(BaseModel):
    username: str
    password: str
    dna_sample: str


class DnaLoginRequest(BaseModel):
    username: str
    dna_sample: str


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


def is_valid_dna(seq: str) -> bool:
    if not seq:
        return False
    if len(seq) % 3 != 0:
        return False
    allowed_chars = {"C", "G", "A", "T"}
    return all(c in allowed_chars for c in seq)


def codon_edit_distance(a: str, b: str) -> int:
    """
    Compute Levenshtein distance between two DNA sequences
    at the codon level (3-character groups).
    Operations: insertion, deletion, substitution (cost=1).
    """
    codons_a = [a[i:i+3] for i in range(0, len(a), 3)]
    codons_b = [b[i:i+3] for i in range(0, len(b), 3)]

    n = len(codons_a)
    m = len(codons_b)

    # Classic DP with rolling rows
    prev = list(range(m + 1))
    curr = [0] * (m + 1)

    for i in range(1, n + 1):
        curr[0] = i
        ca = codons_a[i - 1]
        for j in range(1, m + 1):
            cb = codons_b[j - 1]
            cost = 0 if ca == cb else 1
            curr[j] = min(
                prev[j] + 1,       # deletion
                curr[j - 1] + 1,   # insertion
                prev[j - 1] + cost # substitution
            )
        prev, curr = curr, prev

    return prev[m]


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
    except Exception:
        raise HTTPException(status_code=500)

    return Response(status_code=204)


@app.get("/protected")
def protected_route(current_user: Dict = Depends(get_current_user)):
    return {"message": f"Hello, {current_user['username']}!"}


# ---------- DNA ENDPOINTS ----------

@app.post("/dna-submit")
def dna_submit(body: DnaSubmitRequest):
    """
    Registers a DNA sample for an existing user account.
    Requirements (mirroring mission):

    - username, password, dna_sample must be non-empty
    - DNA only contains C, G, A, T
    - Length divisible by 3
    - Username/password must be valid
    - Multiple samples per user allowed
    - Duplicates are silently accepted
    """
    username = body.username.strip()
    password = body.password
    dna_sample = body.dna_sample.strip()

    # 400: invalid input
    if not username or not password or not dna_sample:
        raise HTTPException(status_code=400)
    if not is_valid_dna(dna_sample):
        raise HTTPException(status_code=400)

    user = users_db.get(username)
    if not user:
        # spec says: invalid credentials -> 401
        raise HTTPException(status_code=401)

    if user["password_hash"] != hash_password(password):
        raise HTTPException(status_code=401)

    # Register DNA sample (multiple allowed, duplicates OK)
    if username not in dna_db:
        dna_db[username] = []
    if dna_sample not in dna_db[username]:
        dna_db[username].append(dna_sample)

    return Response(status_code=204)


@app.post("/dna-login")
def dna_login(body: DnaLoginRequest):
    """
    Authenticates user using DNA sample.

    - 400 if invalid input (empty / bad chars / length % 3 != 0)
    - 401 if:
        * user doesn't exist
        * no DNA registered
        * no registered sample matches within threshold
    - 200 OK with {"token": "..."} if match succeeds
    """
    username = body.username.strip()
    dna_sample = body.dna_sample.strip()

    # 400: invalid input
    if not username or not dna_sample:
        raise HTTPException(status_code=400)
    if not is_valid_dna(dna_sample):
        raise HTTPException(status_code=400)

    user = users_db.get(username)
    if not user:
        raise HTTPException(status_code=401)

    registered_samples = dna_db.get(username)
    if not registered_samples:
        # no DNA registered for this user
        raise HTTPException(status_code=401)

    # Try to match against all registered samples
    matched = False
    for ref in registered_samples:
        ref_codons = len(ref) // 3
        allowed_diff = ref_codons // 100000  # floor(Ca / 100000)

        # If no differences allowed and lengths differ, we can short-circuit
        if allowed_diff == 0 and len(ref) != len(dna_sample):
            continue

        dist = codon_edit_distance(ref, dna_sample)
        if dist <= allowed_diff:
            matched = True
            break

    if not matched:
        raise HTTPException(status_code=401)

    # DNA auth successful – issue token like normal login
    token = str(uuid4())
    tokens_db[token] = username
    return {"token": token}