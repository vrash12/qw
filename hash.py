import os, hashlib, base64

new_password = "12345678"
salt = os.urandom(16)

# halve N to 16384 → memory needs = 128 * 16384 * 8 = 16 MiB
key = hashlib.scrypt(
    new_password.encode('utf-8'),
    salt=salt,
    n=16384,    # ≤ default maxmem
    r=8,
    p=1,
    dklen=64
)

b64_salt = base64.b64encode(salt).decode('ascii')
b64_key  = base64.b64encode(key ).decode('ascii')
hash_str = f"scrypt:16384:8:1${b64_salt}${b64_key}"

print(hash_str)
