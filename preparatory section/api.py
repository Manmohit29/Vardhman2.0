import requests
import logging
import os
from datetime import datetime, date
from config import machine_info
from sync_data import SyncDBHelper
import json

MACHINE = machine_info["name"]

dirname = os.path.dirname(os.path.abspath(__file__))

log = logging.getLogger()

SEND_DATA = False

HOST_IP = '172.27.1.66'
HOST = f'http://{HOST_IP}:8080'
HOST_FP = f'http://{HOST_IP}:8000'
HEADERS = {"Content-Type": 'application/json'}
accessToken = machine_info['access_token']
urlGetPO = "/getPoAndProduct"
url_telemetry = f'{HOST}/api/v1/{accessToken}/telemetry'
url_upload_report = f'{HOST_FP}/upload_report'
url_run_create = f'{HOST_FP}/run_data'
url_run_update = f'{HOST_FP}/update_run_data'
url_stop_create = f'{HOST_FP}/stop_data'
url_stop_update = f'{HOST_FP}/update_stop_data'
urlGetEmails = f"{HOST}/api/v1/{accessToken}/attributes?sharedKeys=Email_List"
url_employee_login = f'{HOST_FP}/employee_login/'
sync_db = SyncDBHelper()


def post_utility_data(payload: dict):
    # log.info(payload)
    if SEND_DATA:
        try:
            req = requests.post(url_telemetry, json=payload, headers=HEADERS, timeout=5)
            log.info(f"UTILITY DATA Status Code: {req.status_code}")
            sync_data = sync_db.get_sync_data()
            if sync_data:
                for data in sync_data:
                    sync_payload = json.loads(data[2])
                    log.info(f"Data type of url {type(data[1])} and {data[1]}")
                    request_response = requests.post(data[1], json=sync_payload, headers=HEADERS, timeout=1)
                    log.info(f"[+] Sync payload status code : {request_response.status_code}")
                    sync_db.delete_sync_data(data[0])
            else:
                log.info("Sync data is not available")

        except Exception as e:
            sync_db.add_sync_data(url_telemetry, payload)
            log.error(f"ERROR: {e}")


def create_run_data(run_data: list):
    payload = {
        "run_data_id": run_data[0],
        "date_": run_data[1],
        "shift": run_data[2],
        "time_": run_data[3],
        "start_time": run_data[4],
        "stop_time": run_data[5],
        "duration": run_data[6],
        "meters": run_data[7],
        "energy_start": run_data[8],
        "energy_stop": run_data[9],
        "fluid_total": run_data[11] - run_data[10],
        "air_total": run_data[12],
        "water_total": run_data[13],
        "run_category": run_data[14],
        "operator_name": run_data[15],
        "po_number": run_data[16],
        "operation_name": run_data[17],
        "machine": MACHINE
    }
    log.info(f"create_run_data {payload}")
    if SEND_DATA:
        try:
            req = requests.post(url_run_create, json=payload, headers=HEADERS, timeout=5)
            log.info(f"RUN DATA Create Status Code: {req.status_code}")
        except Exception as e:
            sync_db.add_sync_data(url_run_create, payload)
            log.error(f"ERROR: {e}")


def post_run_data(**kwargs):
    payload = {
        "run_data_id": 0,
        "time_": 0,
        "stop_time": 0,
        "duration": 0,
        "meters": 0,
        "energy_stop": 0,
        "fluid_total": 0,
        "air_total": 0,
        "water_total": 0,
        "machine": MACHINE
    }
    payload = kwargs
    payload.update({"machine": MACHINE})

    log.info(f"post_run_data {payload}")
    if SEND_DATA:
        try:
            req = requests.post(url_run_update, json=payload, headers=HEADERS, timeout=5)
            log.info(f"RUN DATA UPDATE Status Code: {req.status_code}")
            return req.status_code
        except Exception as e:
            sync_db.add_sync_data(url_run_update, payload)
            log.error(f"ERROR: {e}")


def create_stop_data(stop_data: list):
    payload = {
        "stop_data_id": stop_data[0],
        "date_": stop_data[1],
        "shift": stop_data[2],
        "time_": stop_data[3],
        "start_time": stop_data[4],
        "stop_time": stop_data[5],
        "duration": stop_data[6],
        "energy_start": stop_data[7],
        "energy_stop": stop_data[8],
        "fluid_total": stop_data[10] - stop_data[9],
        "air_total": stop_data[11],
        "water_total": stop_data[12],
        "stop_category": stop_data[13],
        "operator_name": stop_data[14],
        "po_number": stop_data[15],
        "operation_name": stop_data[16],
        "machine": MACHINE
    }
    log.info(payload)
    if SEND_DATA:
        try:
            req = requests.post(url_stop_create, json=payload, headers=HEADERS, timeout=5)
            log.info(f"STOP DATA Create Status Code: {req.status_code}")
        except Exception as e:
            sync_db.add_sync_data(url_stop_create, payload)
            log.error(f"ERROR: {e}")


def post_stop_data(**kwargs):
    payload = {
        "stop_data_id": 0,
        "time_": 0,
        "stop_time": 0,
        "duration": 0,
        "energy_stop": 0,
        "fluid_total": 0,
        "air_total": 0,
        "water_total": 0,
        "machine": MACHINE
    }
    payload = kwargs
    payload.update({"machine": MACHINE})
    # log.info(payload)
    if SEND_DATA:
        try:
            req = requests.post(url_stop_update, json=payload, headers=HEADERS, timeout=5)
            log.info(f"STOP DATA UPDATE Status Code: {req.status_code}")
            return req.status_code
        except Exception as e:
            sync_db.add_sync_data(url_stop_update, payload)
            log.error(f"ERROR: {e}")


def getEmailList():
    try:
        # print(urlGetPass)
        req = requests.get(urlGetEmails, timeout=10)
        res_email = req.json()['shared']
        log.info(res_email)
        # print(res_email)
        if res_email:
            with open(os.path.join(dirname, f'logs/email_log_{date.today()}.txt'), 'a') as f:
                pname = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} --- email-- {res_email}\n'
                f.write(pname)
        if res_email:
            emails = res_email['Email_List'].strip()
            emails = emails.split(",")
            email_list = list(filter(lambda x: x != "", emails))
        else:
            email_list = None
    except Exception as e:
        log.error(e)
        email_list = None
    return email_list


def get_employee_login(emp_id: str, password: str):
    try:
        # print(urlGetPass)
        req = requests.get(f"{url_employee_login}{emp_id.strip()}/{password.strip()}", timeout=10)
        res_emp = req.json()['message']
        log.info(f"login response from server: {res_emp}")
        # print(res_email)
        with open(os.path.join(dirname, f'logs/emp_login_{date.today()}.txt'), 'a') as f:
            pname = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} --- login--{emp_id}--{password}--{req.text}--{req.status_code}\n'
            f.write(pname)
    except Exception as e:
        log.error(e)
        res_emp = False
    return res_emp


def post_now_production(production_data):
    payload = {
        "production": production_data
    }
    log.info(payload)
    url_now = f"{HOST_FP}/now_production"
    if SEND_DATA:
        try:
            req = requests.post(url_now, json=payload, headers=HEADERS, timeout=5)
            log.info(f"NOW Production Status Code: {req.text}")
            return req.status_code
        except Exception as e:
            sync_db.add_sync_data(url_now, payload)
            log.error(f"ERROR: {e}")


def post_now_stoppage(stoppage_data):
    payload = {
        "stoppage": stoppage_data
    }
    log.info(payload)
    url_now_stop = f"{HOST_FP}/now_stoppage"
    if SEND_DATA:
        try:
            req = requests.post(url_now_stop, json=payload, headers=HEADERS, timeout=5)
            log.info(f"NOW Stoppage Status Code: {req.text}")
            return req.status_code
        except Exception as e:
            sync_db.add_sync_data(url_now_stop, payload)
            log.error(f"ERROR: {e}")


def upload_report(file_path, fileName):
    try:
        report = {'report_file': (fileName, open(file_path, 'rb'))}
        print(report)
        url = f'{url_upload_report}'
        req = requests.post(url, files=report)
        log.info(f'Status Code:{req.status_code} {req.text}')
        if req.status_code == 200:
            return req.text
    except Exception as e:
        log.error(f'ERROR uploading file: {e}')
        return False


def send_po_data(data):
    # log.info(payload)
    url_po = f"{HOST_FP}/create_po_data"
    if SEND_DATA:
        try:
            req = requests.post(url_po, json=data, headers=HEADERS, timeout=5)
            log.info(f"Send PO data status code : {req.status_code}")
        except Exception as e:
            sync_db.add_sync_data(url_po, data)
            log.error(f"ERROR: {e}")
