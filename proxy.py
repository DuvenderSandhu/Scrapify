import requests
import time

proxy = {
    "http": "http://tl-c8c9ce2df202f54d955865e4ebb8ff2c31713b0eea7d6b034312a7708c5f008e-country-IN:74s2i9dkssdh@proxy.toolip.io:31114"
}

start = time.time()
try:
    response = requests.get("https://whatismyipaddress.com/", proxies=proxy, timeout=30)
    print("Proxy Time:", time.time() - start, "seconds")
    print("Response:", response.text[:100])
except Exception as e:
    print("Proxy Error:", e)