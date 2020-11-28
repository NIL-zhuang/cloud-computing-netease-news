import time
import os
while True:
    print("=== start crawl ===")
    os.system("./crawl.sh")
    os.system("./move.sh")
    print("=== finish crawl ===")
    time.sleep(1789)
