import os
import time
import subprocess
from config import machine_info


def is_ping_successful(ip):
    """Check if the given IP is reachable."""
    try:
        subprocess.run(["ping", "-c", "1", ip], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except subprocess.CalledProcessError:
        return False


def toggle_network_admin():
    """Toggle the network interface (down and up) using sudo."""
    try:
        print("Toggling network interface eth0...")
        os.system("sudo ifconfig eth0 down")
        time.sleep(1)
        os.system("sudo ifconfig eth0 up")
        print("Network interface eth0 toggled successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    target_ip = "172.27.1.69"
    while True:
        if not is_ping_successful(target_ip):
            print(f"Ping to {target_ip} failed. Toggling network interface eth0...")
            toggle_network_admin()
        else:
            print(f"Ping to {target_ip} is successful. No action needed.")

        # Wait for 1 minute before the next check
        time.sleep(15)
