import logging
import time
from pyModbusTCP.client import ModbusClient
from Conversions import f_list, word_list_to_long, byte_list_to_word, long_list_to_word
from api import get_employee_login
import re

log = logging.getLogger()
TIMEOUT = 1


def initiate(ip, port, unitId):
    return ModbusClient(host=ip, port=port, unit_id=unitId, auto_open=True, auto_close=True, timeout=TIMEOUT)


def read_values(m_data: dict):
    """For initializing the modbus tcp client and reading data from it"""
    try:
        mb_client = initiate(m_data['ip_addr'], m_data['port'], m_data['unit_id'])
        run_data = mb_client.read_holding_registers(m_data['run_reg_start'], m_data['run_length'])
        # log.info(run_data)
        run_data_flt = f_list(run_data[0:15], False)
        # log.info(run_data_flt)
        mass_flow = word_list_to_long(run_data[14:16], False)[0] / 10
        # log.info(f"mass flow- {mass_flow}")
        run_data_int = word_list_to_long(run_data[18:], False)
        log.info(run_data_int)
        status = run_data_int[0]
        stop_reason = run_data_int[1]
        log.info(f"stop_reason {stop_reason}")
        run_reason = run_data_int[2]
        log.info(f"run_reason {run_reason}")
        if status == 1 and run_reason == 0:
            run_data_int[0] = status = 0
        elif status == 0 and stop_reason == 0 and run_reason != 0:
            run_data_int[0] = status = 1
        operator = run_data_int[3]
        # print(run_data_int)
        if status == 1:
            run_data_int.pop(1)
        elif status == 0:
            run_data_int.pop(2)
        # print(run_data_int)
        run_data_total = run_data_flt + run_data_int
        production = dict(zip(m_data['production'], run_data_total))
        log.info(f"production {production}")
        stop_data = mb_client.read_holding_registers(m_data['stop_reg_start'], m_data['stop_length'])
        stop_data_flt = f_list(stop_data[0:15], False)
        stop_data_total = stop_data_flt + run_data_int
        # print(stop_data_total, stop_data_flt)
        stoppage = dict(zip(m_data['stoppage'], stop_data_total))
        # print(production, stoppage, status)
        po_data = mb_client.read_holding_registers(m_data['po_reg_start'], m_data['po_reg_length'])
        log.info(f"reading po from hmi : {po_data}")
        po_data[:] = list(filter(lambda x: x > 0, po_data))
        data = []
        for number in po_data:
            data.append(number & 0xFF)
            data.append((number >> 8) & 0xFF)
        po_number = bytearray(data).decode('utf-8')[:]
        regex = r'^\w+'
        po_number = re.findall(regex, po_number)
        if po_number:
            po_number = po_number[0]
        else:
            po_number = ''
        # print(po_data, po_number)
        production['fluid_flow'] = mass_flow
        stoppage['fluid_flow'] = mass_flow
        stoppage['operation'] = production['operation']

        login_check = mb_client.read_holding_registers(m_data["login_check"], 1)
        log.info(f"LOGIN CHECK --{login_check}--")
        if login_check[0] == 1:
            mb_client.write_multiple_registers(m_data["login_check"], [1])
            emp_id_reg = mb_client.read_holding_registers(m_data["employee_id"], 2)
            emp_id = word_list_to_long(emp_id_reg)[0]
            password_reg = mb_client.read_holding_registers(m_data["password"], 4)
            password_reg[:] = list(filter(lambda x: x > 0, password_reg))
            pass_data = []
            for number in password_reg:
                pass_data.append(number & 0xFF)
                pass_data.append((number >> 8) & 0xFF)
            password = bytearray(pass_data).decode('utf-8')[:]
            try:
                logged_in = get_employee_login(str(emp_id), password)
                log.info(f"login data--{emp_id},{password_reg},{password},{logged_in}")
            except Exception as e:
                log.error(f"Error in login : {e}")
            if logged_in:
                mb_client.write_single_register(m_data["login_success"], 1)
            else:
                mb_client.write_single_register(m_data["login_success"], 0)
            time.sleep(0.5)
            user_reg = mb_client.read_holding_registers(m_data["current_user"], 2)
            current_user = word_list_to_long(user_reg, False)
            log.info(f"USER data {current_user}")
        user_reg = mb_client.read_holding_registers(m_data["current_user"], 2)
        try:
            current_user = word_list_to_long(user_reg, False)[0]
        except:
            current_user = 0
        log.info(f"USER data {current_user}")
        log.info(f"Production Meters: {production['meter']}")
        # log.info(f"Po type: {po_number} {type(po_number)}")
        # log.info(f"{production}, {stoppage}")
        return production, stoppage, status, current_user, po_number
    except Exception as e:
        log.error("Error in reading Client:" + str(m_data['ip_addr']) + str(e))
    return None, None, None, None, None


def write_po(m_data: dict, po_number: str):
    mb_client = initiate(m_data['ip_addr'], m_data['port'], m_data['unit_id'])
    log.info(f"Sending PO Number: {po_number}")
    for i in range(5):
        try:
            po_number = list(po_number.encode())
            if len(po_number) % 2 == 1:
                po_number.append(0)
            po_number = byte_list_to_word(po_number, False)
            while len(po_number) < m_data['po_reg_length']:
                po_number.append(0)
            # print("WRITE PO",po_number)
            po_register_data = mb_client.write_multiple_registers(m_data['po_reg_start'], po_number)
            if po_register_data:
                break
            # log.info(po_register_data)
        except Exception as e:
            log.error(f"ERROR: {e}")
            time.sleep(i / 10)
    return None


def write_po_list(m_data, po_number_list, meter_list):
    mb_client = initiate(m_data['ip_addr'], m_data['port'], m_data['unit_id'])
    log.info(f"Writing PO LIST:  {len(po_number_list)}")
    log.info(f"Writing METERS LIST:  {len(meter_list)}")
    meter_words = long_list_to_word(meter_list, False)
    while len(po_number_list) < m_data['po_numbers']:
        po_number_list.append("-")
    while len(meter_words) < m_data['length_list_length']:
        meter_words.append(0)
    # log.info(f"{po_number_list}, {meter_words}")
    for i in range(5):
        try:
            register_data = mb_client.write_multiple_registers(m_data['length_list_start'], meter_words)
            po_list = []
            log.debug(len(po_number_list))
            for po_number in po_number_list:
                log.debug(po_number)
                po_number = list(po_number.encode())
                log.debug(po_number)
                while len(po_number) < 20:
                    po_number.append(0)
                po_list += po_number
            po_num_list = byte_list_to_word(po_list, False)
            while len(po_num_list) < m_data['po_list_length'] * 2:
                po_num_list.append(0)
            po_num_list1 = po_num_list[:100]
            po_num_list2 = po_num_list[100:]
            log.debug(f"{len(po_num_list)}%{len(po_num_list1)}%{len(po_num_list2)}")
            while len(po_num_list1) < m_data['po_list_length']:
                po_num_list1.append(0)
            # print("WRITE PO",po_number)
            # 'po_list_start': 1200,
            # 'po_list_length': 100,
            # 'length_list_start': 1400,
            # 'length_list_length': 36
            po_register_data1 = mb_client.write_multiple_registers(m_data['po_list_start1'], po_num_list1)
            po_register_data2 = mb_client.write_multiple_registers(m_data['po_list_start2'], po_num_list2)
            if po_register_data1 and po_register_data2 and register_data:
                break
            # log.info(po_register_data)
        except Exception as e:
            log.error(f"ERROR: {e}")
            time.sleep(i / 10)
    return None


def write_fluid(m_data: dict, fluid: int, mass_flow: int):
    mb_client = initiate(m_data['ip_addr'], m_data['port'], m_data['unit_id'])
    log.info(f"Sending Fluid flow value: {fluid} {mass_flow}")
    for i in range(5):
        try:
            fluid_register_data = mb_client.write_multiple_registers(m_data['fluid_reg_start'], [fluid])
            mass_flow_reg1 = mb_client.write_multiple_registers(m_data['flow_run_start'], [mass_flow])
            mass_flow_reg2 = mb_client.write_multiple_registers(m_data['flow_stop_start'], [mass_flow])
            # log.info(f"{mass_flow_reg1} {mass_flow_reg2}")
            if fluid_register_data:
                mb_client.write_single_coil(m_data['fluid_signal'], True)
                time.sleep(0.1)
                mb_client.write_single_coil(m_data['fluid_signal'], False)
                break
            # log.info(po_register_data)
        except Exception as e:
            log.error(f"ERROR: {e}")
            time.sleep(i / 10)
    return None


def write_hmi_data(m_data: dict, hmi_data: list):
    mb_client = initiate(m_data['ip_addr'], m_data['port'], m_data['unit_id'])
    log.info(f"Sending Length and Stop min: {hmi_data}")
    hmi_data_words = long_list_to_word(hmi_data, False)
    for i in range(5):
        try:
            register_data = mb_client.write_multiple_registers(m_data['hmi_data_start'], hmi_data_words)
            # log.info(register_data)
        except Exception as e:
            log.error(f"ERROR: {e}")
            time.sleep(i / 10)
    return None


def write_category_data(m_data: dict, **kwargs):
    mb_client = initiate(m_data['ip_addr'], m_data['port'], m_data['unit_id'])
    # log.info(f"Sending Length and Stop min: {hmi_data}")
    # print(kwargs)
    if 'run_cat' in kwargs:
        run_cat = list(kwargs['run_cat'].encode())
        if len(run_cat) % 2 == 1:
            run_cat.append(0)
        # print(run_cat)
        run_cat = byte_list_to_word(run_cat, False)
        # print(run_cat)
        while len(run_cat) < m_data['curr_run_length']:
            run_cat.append(0)
        # print(run_cat)
        for i in range(5):
            try:
                data = mb_client.write_multiple_registers(m_data['curr_run_category'], run_cat)
                # log.info(fluid_register_data)
                if data:
                    break
            except Exception as e:
                log.error(f"ERROR: {e}")
                time.sleep(i / 10)
    if 'stop_cat' in kwargs:
        stop_cat = list(kwargs['stop_cat'].encode())
        if len(stop_cat) % 2 == 1:
            stop_cat.append(0)
        # print(stop_cat)
        stop_cat = byte_list_to_word(stop_cat, False)
        # print(stop_cat)
        while len(stop_cat) < m_data['curr_stop_length']:
            stop_cat.append(0)
        # print(stop_cat)
        for i in range(5):
            try:
                data = mb_client.write_multiple_registers(m_data['curr_stop_category'], stop_cat)
                # log.info(fluid_register_data)
                if data:
                    break
            except Exception as e:
                log.error(f"ERROR: {e}")
                time.sleep(i / 10)
    return None


def reset_run(m_data: dict):
    mb_client = initiate(m_data['ip_addr'], m_data['port'], m_data['unit_id'])
    log.info("Resetting Running registers")
    for i in range(5):
        try:
            register_data = mb_client.write_multiple_registers(m_data['run_reg_start'], [0] * 14)
            if register_data:
                break
            # log.info(register_data)
        except Exception as e:
            log.error(f"ERROR: {e}")
            time.sleep(i / 10)
    return None


def reset_stoppage(m_data: dict):
    mb_client = initiate(m_data['ip_addr'], m_data['port'], m_data['unit_id'])
    log.info("Resetting Stoppage registers")
    for i in range(5):
        try:
            register_data = mb_client.write_multiple_registers(m_data['stop_reg_start'], [0] * 14)
            if register_data:
                break
            # log.info(register_data)
        except Exception as e:
            log.error(f"ERROR: {e}")
            time.sleep(i / 10)
    return None


def write_prod_coil(m_data: dict):
    mb_client = initiate(m_data['ip_addr'], m_data['port'], m_data['unit_id'])
    log.info(f"Production Coil Status {mb_client.read_coils(m_data['prod_coil'])}")
    for i in range(1):
        try:
            register_data = mb_client.write_single_coil(m_data['prod_coil'], True)
            # log.info(fluid_register_data)
        except Exception as e:
            log.error(f"ERROR: {e}")
            time.sleep(i / 10)
    return None


def read_manual_hmi(m_data: dict):
    mb_client = initiate(m_data['ip_addr'], m_data['port'], m_data['unit_id'])
    result = {}
    tw_values = []
    temp_status = mb_client.read_coils(m_data['temp_width']['bit'])
    if temp_status is not None and temp_status == [1]:
        for i in range(5):
            try:

                tw_reg = mb_client.read_holding_registers(m_data['temp_width']['temp_reg'],
                                                          m_data['temp_width']['length'])
                # tw_values = f_list(tw_reg, False)
                tw_values.append(tw_reg[0] / 100)
                tw_values.append(tw_reg[2] / 100)
                reset_tw = mb_client.write_single_coil(m_data['temp_width']['bit'], False)
                result['tw_values'] = tw_values
            except Exception as e:
                log.error(f"ERROR: {e}")
                time.sleep(i / 10)

    trolley_status = mb_client.read_coils(m_data['trolley']['bit'])
    if trolley_status is not None and trolley_status == [1]:
        for i in range(5):
            try:

                tr_reg = mb_client.read_holding_registers(m_data['trolley']['reg'],
                                                          m_data['trolley']['length'])
                tr_reg[:] = list(filter(lambda x: x > 0, tr_reg))
                data = []
                for number in tr_reg:
                    data.append(number & 0xFF)
                    data.append((number >> 8) & 0xFF)
                trolley_name = bytearray(data).decode('utf-8')[:]
                regex = r'^\w+'
                trolley_name = re.findall(regex, trolley_name)
                if trolley_name:
                    trolley_name = trolley_name[0]
                else:
                    trolley_name = ''
                reset_tr = mb_client.write_single_coil(m_data['trolley']['bit'], False)
                if trolley_name:
                    result['trolley'] = trolley_name
            except Exception as e:
                log.error(f"ERROR: {e}")
                time.sleep(i / 10)
    log.info(f"Manual HMI DATA: {result}")
    return result


def reset_production_btn(m_data: dict):
    mb_client = initiate(m_data['ip_addr'], m_data['port'], m_data['unit_id'])
    memory_coil_1 = mb_client.read_coils(9147)
    print(f"{9147} : {memory_coil_1}")
    # start = 9140
    # for i in range(11):
    #     memory_coil_1 = mb_client.read_coils(start)
    #     start = start + 1
    #     print(f"{start} : {memory_coil_1}")
    # print("#"*15)
    if memory_coil_1[0]:
        mb_client.write_single_coil(9149, True)
        print(f"written true on bit 9149")
        time.sleep(0.5)
        mb_client.write_single_coil(9149, False)

# 9148 button
# 9150 reset
