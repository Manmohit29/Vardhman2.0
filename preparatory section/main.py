import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
import time
from datetime import datetime, timedelta
from Conversions import getShift, f_list
from database import DBHelper
import schedule
from mb_comm import read_values, write_po, reset_run, reset_stoppage, write_fluid, write_hmi_data, \
    write_category_data, write_po_list, write_prod_coil, read_manual_hmi
from PO_Read import read_po_number
from api import post_utility_data, upload_report, getEmailList, post_now_production, post_now_stoppage
from Fluid_Read import get_Fluid, get_kwh
from Excel_Report import generate_report, send_mail
import os
from datetime import datetime, date
from config import machine_info

dirname = os.path.dirname(os.path.abspath(__file__))
file_path = "time_sync_status.txt"
firstCall = False
SAMPLE_RATE = 1
MachineDataSampleRate = 60
PO_LIST_SampleRate = 15
PO_SAMPLE_RATE = 1
FLUID_SAMPLE_RATE = 1
MANUAL_SAMPLE_RATE = 5
log_level = logging.INFO

FORMAT = ('%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger()

fileHandler = TimedRotatingFileHandler(os.path.join(dirname, f'logs/app_log'),
                                       when='midnight', interval=1)
fileHandler.setFormatter(logFormatter)
fileHandler.suffix = "%Y-%m-%d.log"
log.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
log.addHandler(consoleHandler)
log.setLevel(log_level)
today = (datetime.now() - timedelta(hours=7)).strftime("%F")
c = DBHelper()
oeeData = {}
po_flag = False


def utility_calculations():
    global today, shift, production, stoppages
    st = time.time()
    prevS, prevD, prev_status, prev_po_id = c.getMiscData()
    try:
        daily_data, utilization, meters = c.get_daily_calculations(prevD)
        po_number, article, greige_glm, finish_glm, construction = c.get_po_details()
        operator_name = c.get_operator_name()
        mc_status = c.get_status()
        payload = {}
        payload["type"] = 'utility'
        payload["utilization"] = round(utilization, 2)
        payload["meters"] = round(meters, 2)
        payload["energy"] = round(daily_data[0], 2)
        payload["fluid_total"] = round(daily_data[1], 2)
        payload["air_total"] = round(daily_data[2], 2)
        payload["water_total"] = round(daily_data[3], 2)
        payload["operator"] = operator_name
        payload["po_number"] = "-" if po_number is None else po_number
        payload["article"] = "-" if article is None else article
        payload["greige_glm"] = "-" if greige_glm is None else greige_glm
        payload["finish_glm"] = "-" if finish_glm is None else finish_glm
        payload["construction"] = "-" if construction is None else construction
        payload["mc_status"] = mc_status

        if production is not None:
            payload['fluid_flow'] = production['fluid_flow']
            payload['speed'] = production['speed']
            payload['air_flow'] = production['air_flow']
            payload['operation'] = c.get_operation_name(production['operation'])
            if mc_status:
                payload['sub_running'] = c.get_current_run_name()
                payload['sub_stoppage'] = '-'
            else:
                payload['sub_running'] = '-'
                payload['sub_stoppage'] = c.get_current_stop_name()

        log.info(payload)
        post_utility_data(payload)
    except Exception as E:
        log.error(E)
    et = time.time()
    # log.info(f"utility_calculations : {et - st}")


def check_po_number():
    st = time.time()
    po_number, article, greige_glm, finish_glm, construction = read_po_number()
    if po_number:
        write_po(machine_info, po_number)
        c.add_po_data(po_number, article, greige_glm, finish_glm, construction)
    et = time.time()
    # log.info(f"check po : {et - st}")


def read_fluid_flow():
    st = time.time()
    nh_rate, mass_flow, nh_total = get_Fluid(1)
    # nh_rate, mass_flow, nh_total = [0,0,0]
    log.info(f"{nh_rate},{mass_flow},{nh_total}")
    kWh_reg = get_kwh(2)
    # kWh_reg = [0,0]
    if kWh_reg is not None:
        # log.info(f"read_kwh : {kWh_reg}")
        kWh = f_list(kWh_reg)[0]
        log.info(f"read_kwh : {kWh}")
        c.updateLastEnergy(kWh)
    if nh_rate is not None:
        write_fluid(machine_info, nh_rate, mass_flow)
    if nh_total is not None:
        c.updateLastHeat(nh_total)
    curr_stop_cat = c.get_current_stop_name()
    curr_run_cat = c.get_current_run_name()
    if curr_stop_cat:
        write_category_data(machine_info, stop_cat=curr_stop_cat)
    else:
        write_category_data(machine_info, stop_cat="None")
    if curr_run_cat:
        write_category_data(machine_info, run_cat=curr_run_cat)
    else:
        write_category_data(machine_info, run_cat="None")
    et = time.time()
    # log.info(f"read_fluid_flow : {et - st}")


def send_hmi_data(date_, shift):
    st = time.time()
    length = c.get_daily_hmi_length(date_)
    shift_stoppages = c.get_shift_hmi_mins(date_, shift)
    # print(shift_stoppages)
    hmi_data = [length, shift_stoppages]

    write_hmi_data(machine_info, hmi_data)
    et = time.time()
    # log.info(f"send_hmi_data : {et - st}")


def read_manual_data():
    # st = time.time()
    manual_data = read_manual_hmi(machine_info)
    if manual_data:
        c.add_manual_data(manual_data)
    # et = time.time()
    # log.info(f"read_manual_data : {et - st}")


def send_po_list():
    st = time.time()
    prevS, prevD, prev_status, prev_po_id = c.getMiscData()
    po_list, meters_list = c.get_daily_po_data(prevD)
    # print(shift_stoppages)
    meters_list = [int(meter) for meter in meters_list]
    write_po_list(machine_info, po_list, meters_list)
    # write_prod_coil(machine_info)
    et = time.time()


schedule.every(PO_SAMPLE_RATE).seconds.do(check_po_number)
schedule.every(FLUID_SAMPLE_RATE).seconds.do(read_fluid_flow)
schedule.every(MANUAL_SAMPLE_RATE).seconds.do(read_manual_data)
schedule.every(MachineDataSampleRate).seconds.do(utility_calculations)
schedule.every(PO_LIST_SampleRate).seconds.do(send_po_list)

if __name__ == '__main__':
    try:
        while True:
            # Define the file path
            # try:
            #     # Open the file in read mode
            #     with open(file_path, "r") as file:
            #         # Read the content of the file
            #         time_sync_status = int(file.read().strip())
            #         log.info(f"Time sync status is : {time_sync_status}")
            # except FileNotFoundError:
            #     log.error(f"File '{file_path}' not found.")
            #     time_sync_status = 0
            # except Exception as e:
            #     log.error(f"An error occurred: {e}")
            #     time_sync_status = 0
            # if time_sync_status:
            st = time.time()
            today = (datetime.now() - timedelta(hours=7)).strftime("%F")
            shift = getShift()
            prevS, prevD, prev_status, prev_po_id = c.getMiscData()
            # machine_info = c.getMachines()
            # print(prevS, prevD, prev_status, prev_po_id)
            try:
                production, stoppages, status, operator, po_number = read_values(machine_info)
                production['kWh'] = stoppages['kWh'] = c.get_last_energy()
                production['fluid_total'] = stoppages['fluid_total'] = c.get_last_nh()
                if po_number == "":
                    po_number = 0
                po_1 = po_number
                po_2 = c.get_po_number()
                log.info(f"{po_1} {po_2}")
                if po_1 != po_2:
                    c.update_po_id(po_number)
                    time.sleep(1)
                    po_3 = c.get_po_number()
                    po_4 = c.get_po_id_number(po_number)
                    log.info(f"{po_number} {po_3} {po_4}")
                    if po_4 != po_3 or (po_number != 0 and po_4 == 0):
                        log.info("SQLITE ERROR: PO NOT UPDATED")
                        c.disconnect()
                        c = DBHelper()
                        c.update_po_id(po_number)
                        prev_po_id = c.get_po_id_number(po_number)
                        if po_number != 0 and prev_po_id == 0:
                            c.add_po_data(po_number, '', '', '', '')
                            prev_po_id = c.get_po_id_number(po_number)
                            c.update_po_id(po_number)
                    else:
                        prev_po_id = c.get_po_id()
                        c.update_po_id(po_number)
                    log.info(prev_po_id)
                    # if prev_po_id != 0 and prev_po_id is not None:
                    po_flag = True

                if operator != c.get_operator_id() and operator is not None:
                    # print(type(operator))
                    c.update_operator_id(operator)

                if prev_status is None:
                    c.update_status(status)
                    prev_status = status

                if prev_status == 1:

                    if production is not None:
                        if po_flag:
                            c.close_run()
                            reset_run(machine_info)
                            # last_run_id = c.get_last_run_id()
                            production, stoppages, status, operator, po_number = read_values(machine_info)
                            production['kWh'] = stoppages['kWh'] = c.get_last_energy()
                            production['fluid_total'] = stoppages['fluid_total'] = c.get_last_nh()
                            c.add_run_data(prevD, prevS, prev_po_id, None, production)
                            po_flag = False
                        else:
                            last_run_id = c.get_last_run_id()
                            write_prod_coil(machine_info)
                            c.add_run_data(prevD, prevS, prev_po_id, last_run_id, production)
                        # c.add_production_data(prevD, prevS, prev_po_id, production)
                elif prev_status == 0:
                    if stoppages is not None:
                        if po_flag:
                            c.close_stoppage()
                            reset_stoppage(machine_info)
                            # last_stop_id = c.get_last_stop_id()
                            production, stoppages, status, operator, po_number = read_values(machine_info)
                            production['kWh'] = stoppages['kWh'] = c.get_last_energy()
                            production['fluid_total'] = stoppages['fluid_total'] = c.get_last_nh()
                            c.add_stoppage_data(prevD, prevS, prev_po_id, None, stoppages)
                            po_flag = False
                        else:
                            last_stop_id = c.get_last_stop_id()
                            c.add_stoppage_data(prevD, prevS, prev_po_id, last_stop_id, stoppages)
                # log.info(f'{status} {prev_status}')

                if status != prev_status and status is not None:
                    if status == 1:
                        c.close_stoppage()
                        reset_stoppage(machine_info)
                        last_run_id = c.get_last_run_id()
                        c.add_run_data(prevD, prevS, prev_po_id, last_run_id, production)
                    elif status == 0:
                        c.close_run()
                        reset_run(machine_info)
                        last_stop_id = c.get_last_stop_id()
                        c.add_stoppage_data(prevD, prevS, prev_po_id, last_stop_id, stoppages)
                    c.update_status(status)

                if status == prev_status:
                    # print(production['run_category'], c.get_current_run_category())
                    if status == 1 and (c.get_current_run_category() != production['run_category']):
                        c.close_run()
                        reset_run(machine_info)
                        # last_run_id = c.get_last_run_id()
                        production, stoppages, status, operator, po_number = read_values(machine_info)
                        production['kWh'] = stoppages['kWh'] = c.get_last_energy()
                        production['fluid_total'] = stoppages['fluid_total'] = c.get_last_nh()
                        c.add_run_data(prevD, prevS, prev_po_id, None, production)
                    elif status == 0 and (c.get_current_stop_category() != stoppages['stop_category']):
                        c.close_stoppage()
                        reset_stoppage(machine_info)
                        # last_stop_id = c.get_last_stop_id()
                        production, stoppages, status, operator, po_number = read_values(machine_info)
                        production['kWh'] = stoppages['kWh'] = c.get_last_energy()
                        production['fluid_total'] = stoppages['fluid_total'] = c.get_last_nh()
                        c.add_stoppage_data(prevD, prevS, prev_po_id, None, stoppages)
                    po_flag = False

                if shift != prevS:
                    try:
                        prod_data = c.get_now_production(prevD, prevS)
                        log.info(prod_data)
                        post_now_production(prod_data)
                    except Exception as e:
                        log.error(f"ERROR POSTING Production--{e}")
                    try:
                        stop_data = c.get_now_stoppage(prevD, prevS)
                        log.info(stop_data)
                        post_now_stoppage(stop_data)
                    except Exception as e:
                        log.error(f"ERROR POSTING Stoppage--{e}")
                    c.updateCurrShift(shift)
                    reset_run(machine_info)

                if today != prevD:
                    try:
                        try:
                            path_, file_name = generate_report(prevD, c)
                            if path_:
                                response = upload_report(path_, file_name)
                                with open(os.path.join(dirname, f'logs/upload_log_{date.today()}.txt'), 'a') as f:
                                    pname = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} --- upload-- {response}\n'
                                    f.write(pname)
                            email_list = getEmailList()
                            send_mail(send_from='at1iiot@vardhman.com',
                                      send_to=email_list,
                                      subject=f'Stenter-5 Report for {prevD}',
                                      text='Please find attached report.',
                                      files=[path_], server="172.28.0.254", port=25,
                                      username='at1iiot@vardhman.com', password='',
                                      use_tls=False)
                        except:
                            log.error(f"Error generating report")
                        #     path_ = None
                        #     file_name = None
                        # if path_:
                        #     response = upload_report(path_, file_name)
                        #     with open(os.path.join(dirname, f'logs/upload_log_{date.today()}.txt'), 'a') as f:
                        #         pname = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} --- upload-- {response}\n'
                        #         f.write(pname)
                    except Exception as e:
                        log.error(f"ERROR generating report: {e}")
                    c.updateCurrDate(today)
                if firstCall:
                    try:
                        try:
                            path_, file_name = generate_report("2022-09-22", c)
                            try:
                                c.update_email_list()
                                email_list = getEmailList()
                            except:
                                email_list = c.get_email_list()
                                log.error("Error fetching emails.")
                            # send_mail(send_from='at1iiot@vardhman.com',
                            #           send_to=email_list,
                            #           subject=f'Stenter-5 Report for {"2022-09-22"}',
                            #           text='Please find attached report.',
                            #           files=[path_], server="172.28.0.254", port=25,
                            #           username='at1iiot@vardhman.com', password='',
                            #           use_tls=False)
                        except:
                            log.error(f"Error generating report")
                            path_ = None
                            file_name = None
                        if path_:
                            response = upload_report(path_, file_name)
                            with open(os.path.join(dirname, f'logs/upload_log_{date.today()}.txt'), 'a') as f:
                                pname = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} --- upload-- {response}\n'
                                f.write(pname)
                    except Exception as e:
                        log.error(f"ERROR Mailing and Uploading report: {e}")
                    firstCall = False
                send_hmi_data(prevD, prevS)
            except Exception as e:
                log.error(e)
            time.sleep(0.5)
            schedule.run_pending()
            et = time.time()
            # log.info(f"main : {et - st}")
        # else:
        #     log.error("Time is not sync")
        #     time.sleep(1)
    except KeyboardInterrupt:
        log.error('ERROR: Main program stopped')
    except Exception as e:
        log.error(f'ERROR: {e}')
