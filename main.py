import time
import os
while True:
    os.system("./crawl.sh")
    os.system("./move.sh")
    time.sleep(3600)
