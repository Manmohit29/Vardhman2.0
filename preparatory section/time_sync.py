import subprocess
import time

while True:
    try:
        output = subprocess.check_output(['timedatectl']).decode()
        
        # Check if the clock is synchronized
        if 'System clock synchronized: yes' in output:
            synchronized = 1
        else:
            synchronized = 0
        print(f"synchronized : {synchronized}")
    except Exception as e:
        print(f"Error : while executing subprocess: {e}")
        synchronized = 0
    # Write the status to a file
    try:
        with open('time_sync_status.txt', 'w') as file:
            file.write(str(synchronized))
        
        print("Clock status written to file.")
    except Exception as e:
        print(f"Error while writing into file : {e}")
    time.sleep(0.5)
##################################################################
