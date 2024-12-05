import time

from config import  machine_info
from mb_comm import reset_production_btn

while True:
    reset_production_btn(machine_info)
    time.sleep(2)