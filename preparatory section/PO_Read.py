# -*- coding: utf-8 -*-
import serial
import os
import logging
from datetime import datetime, date

log = logging.getLogger()

dirname = os.path.dirname(os.path.abspath(__file__))

PORT = '/dev/serial/by-id/usb-Symbol_Technologies__Inc__2008_Symbol_Bar_Code_Scanner::EA_USB_CDC_Symbol_Scanner-if00'
POLL_RATE = 1

try:
    ser = serial.Serial(
        port=PORT,
        baudrate=9600,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        bytesize=serial.EIGHTBITS,
        xonxoff=True,
        timeout=0.5,
        write_timeout=1
    )
except Exception as e:
    log.error(f'ERROR: {e} Error in opening serial port')


def read_po_number():
    global ser
    try:
        po_data = ser.read_until("\r\n")  # read_all()
        po_data = po_data.decode('ascii').strip()
        # log.info(po_data)
        po_data = po_data.split("\r\n")[0]
        # log.info(po_data)
        po_data = po_data.split("$")
        if po_data[0] != "":
            log.info(po_data)
        if len(po_data) > 1:
            po_number, article, greige_glm, finish_glm, construction = po_data[:5]
        else:
            po_number = po_data[0]
            article, greige_glm, finish_glm, construction = None, None, None, None
        if po_number:
            log.info(f'PO Data: {po_data}')
            with open(os.path.join(dirname, f'logs/po_log_{date.today()}.txt'), 'a') as f:
                pname = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}---##{po_data}##\n'
                f.write(pname)
            ser.read_all()
            ser.flushInput()
            ser.flushOutput()
            ser.flush()
            if greige_glm:
                greige_glm = float(greige_glm)
            if finish_glm:
                finish_glm = float(finish_glm)
            po_number = po_number.replace("\x00", "")
            if len(po_number) < 10:
                return None, None, None, None, None
            return po_number[0:18], article, greige_glm, finish_glm, construction
        return None, None, None, None, None
    except Exception as e:
        log.error(f'Error: {e} Error in reading po number')
        try:
            ser.close()
        except:
            pass

        try:
            ser = serial.Serial(
                port=PORT,
                baudrate=9600,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                bytesize=serial.EIGHTBITS,
                xonxoff=True,
                timeout=0.5,
                write_timeout=1
            )
        except Exception as e:
            log.error(f'ERROR: {e} Error in opening serial port')

    return None, None, None, None, None

# log.info('*' * 10 + " Reading PO Number " + '*' * 10)

# try:
#     while True:
#         read_po_number()
#         sleep(POLL_RATE)
# except Exception as e:
#     print(e)
