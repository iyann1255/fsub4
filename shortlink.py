import secrets
import string

ALPHABET = string.ascii_letters + string.digits

def gen_code(n: int = 10) -> str:
    return "".join(secrets.choice(ALPHABET) for _ in range(n))
