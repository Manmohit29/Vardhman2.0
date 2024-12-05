import struct
import datetime
import json
import re
import requests
import logging

log = logging.getLogger()

shift_a_start = datetime.time(7, 00, 0, 0)
shift_b_start = datetime.time(15, 00, 0, 0)
shift_c_start = datetime.time(23, 00, 00, 0)
shift_a_end = datetime.time(15, 00, 00, 0)
shift_b_end = datetime.time(23, 00, 00, 0)
shift_c_end = datetime.time(7, 00, 00, 0)


def convertTime(time_):
    return datetime.datetime.strptime(time_.strftime("%F %T"), "%Y-%m-%d %H:%M:%S")


def getShift():
    global shift_a_start, shift_b_start, shift_c_start, shift_a_end, shift_b_end, shift_c_end
    now = datetime.datetime.now().time()
    new_day = datetime.time(23, 59, 59, 999)
    new_one = datetime.time(0, 0, 0, 0)
    if shift_b_start <= now < shift_b_end:
        return 'B'
    elif shift_a_start <= now < shift_a_end:
        return 'A'
    elif shift_c_start <= now <= new_day or new_one <= now < shift_c_end:
        return 'C'
    else:
        return 'C'


def decimalToBinary(n):
    return bin(n).replace("0b", "")


def decode_ieee(val_int):
    return struct.unpack("f", struct.pack("I", val_int))[0]


def long_list_to_word(val_list, big_endian=True):
    # allocate list for long int
    word_list = [None] * int(len(val_list) * 2)
    # fill registers list with register items
    for i, item in enumerate(val_list):
        if big_endian:
            word_list[2 * i + 1] = item & 0xFFFF
            word_list[2 * i] = (item >> 16) & 0xFFFF
        else:
            word_list[2 * i] = item & 0xFFFF
            word_list[2 * i + 1] = (item >> 16) & 0xFFFF
    # return long list
    return word_list


def word_list_to_long(val_list, big_endian=True):
    # allocate list for long int
    long_list = [None] * int(len(val_list) / 2)
    # fill registers list with register items
    for i, item in enumerate(long_list):
        if big_endian:
            long_list[i] = (val_list[i * 2] << 16) + val_list[(i * 2) + 1]
        else:
            long_list[i] = (val_list[(i * 2) + 1] << 16) + val_list[i * 2]
    # return long list
    #log.info(f"long_list from word_list_to_long func = {long_list}")
    return long_list


def byte_list_to_word(val_list, big_endian=True):
    # allocate list for long int
    word_list = [None] * int(len(val_list) / 2)
    # fill registers list with register items
    for i, item in enumerate(word_list):
        if big_endian:
            word_list[i] = (val_list[i * 2] << 8) + val_list[(i * 2) + 1]
        else:
            word_list[i] = (val_list[(i * 2) + 1] << 8) + val_list[i * 2]
    # return long list
    return word_list


def f_list(values, bit=False):
    fist = []
    for f in word_list_to_long(values, bit):
        fist.append(round(decode_ieee(f), 3))
    # print(len(f_list),f_list)
    #log.info(f"from f_list = {fist}")
    return fist
