import serial
import minimalmodbus
import logging

log = logging.getLogger()


def initiate(slaveId):
    try:
        com_port = '/dev/serial/by-id/usb-FTDI_FT232R_USB_UART_A10N310A-if00-port0'
        i = int(slaveId)
        instrument = minimalmodbus.Instrument(com_port, i)
        instrument.serial.baudrate = 9600
        instrument.serial.bytesize = 8
        instrument.serial.parity = serial.PARITY_NONE
        instrument.serial.stopbits = 1
        instrument.serial.timeout = 1
        instrument.serial.close_after_each_call = True
        # print('Modbus ID Initialized: ' + str(i))
        return instrument
    except Exception as e:
        log.error(f"Error while connecting with fluid: {e}")
        return None


def get_Fluid(unitId):
    mb_client = initiate(unitId)
    if mb_client:
        try:
            register_data = mb_client.read_registers(1, 20, 3)
            nh_rate = register_data[2]
            mass_flow = register_data[14]
            val1, val2 = mb_client.read_registers(12, 2, 3)
            nh_total = val1 * 65536 + val2
            # print()
            return nh_rate, mass_flow, nh_total
        except Exception as e:
            log.error(f"ERROR: {e}")
    return None, None, None


def initiate_meter(slaveId):
    try:
        com_port = '/dev/serial/by-id/usb-FTDI_FT232R_USB_UART_A10N310A-if00-port0'
        i = int(slaveId)
        instrument = minimalmodbus.Instrument(com_port, i)
        instrument.serial.baudrate = 9600
        instrument.serial.bytesize = 8
        instrument.serial.parity = serial.PARITY_NONE
        instrument.serial.stopbits = 1
        instrument.serial.timeout = 0.5
        instrument.serial.close_after_each_call = True
        # print('Modbus ID Initialized: ' + str(i))
        return instrument
    except Exception as e:
        log.error(f"Error while connecting with meter: {e}")
        return None


def get_kwh(unitId):
    mb_client = initiate_meter(unitId)
    if mb_client:
        try:
            register_data = mb_client.read_registers(199, 2, 3)
            return register_data
        except Exception as e:
            log.error(f"ERROR: {e}")
    return None
