import os
#import schedule
import time
from datetime import date, datetime
import subprocess

dirname = os.path.dirname(os.path.abspath(__file__))


def ethRestart():
    ## ETHERNET
    # Todo Add machine IP here
    hostname2 = "172.27.1.66"
    response2 = os.system("ping -c 1 " + hostname2)
    if response2 == 0:
        pingstatus2 = "Active"
        with open(os.path.join(dirname, f'logs/network_{date.today()}.log'), 'a') as f:
            pname = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} --ETHERNET-- {pingstatus2}\n'
            f.write(pname)
    else:
        pingstatus2 = "Error"
        with open(os.path.join(dirname, f'logs/network_{date.today()}.log'), 'a') as f:
            pname = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} --ETHERNET-- {pingstatus2}\n'
            f.write(pname)

        try:
            # Todo add connection name here
            sp3 = subprocess.Popen(["/bin/bash", "-i", "-c", "nmcli connection up id 'Ifupdown (eth0)'"])
            sp3.communicate()
        except Exception as e:
            print("ERROR eth0 up", e)


#schedule.every(600).seconds.do(ethRestart)

try:
    while True:
        ethRestart()
        time.sleep(60)
        # ethRestart()
except Exception as e:
    print(e)
#####################*****************************######################****************************###################



