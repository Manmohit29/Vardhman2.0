import json
import sqlite3
from datetime import datetime, timedelta
import logging
from Conversions import getShift, convertTime, shift_a_start, shift_b_start, shift_c_start
from Default_Data import master_data
from api import create_run_data, create_stop_data, post_stop_data, post_run_data, getEmailList, send_po_data
from config import machine_info

log = logging.getLogger()

machine_code = machine_info["machine_code"]


class DBHelper():
    def __init__(self):
        self.conn = sqlite3.connect("vardhman.db")
        self.c = self.conn.cursor()

        self.c.execute('''CREATE TABLE IF NOT EXISTS run_category(run_id INTEGER NOT NULL,
                        name VARCHAR(30) NOT NULL,code VARCHAR(30), PRIMARY KEY(run_id), UNIQUE(name))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS stop_category(stop_id INTEGER NOT NULL,
                        name VARCHAR(30) NOT NULL,code VARCHAR(30), PRIMARY KEY(stop_id), UNIQUE(name))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS operator_list(operator_id INTEGER NOT NULL,
                        name VARCHAR(30) NOT NULL, PRIMARY KEY(operator_id), UNIQUE(name))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS operation_list(operation_id INTEGER NOT NULL,
                        name VARCHAR(30) NOT NULL, PRIMARY KEY(operation_id), UNIQUE(name))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS po_data(po_id INTEGER NOT NULL,
                        po_number VARCHAR(30) NOT NULL, article TEXT, greige_glm DOUBLE, finish_glm DOUBLE,
                        construction TEXT, hmi_data TEXT DEFAULT('{}'),
                        PRIMARY KEY(po_id AUTOINCREMENT), UNIQUE(po_number))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS run_data(run_data_id INTEGER NOT NULL, date_ DATE NOT NULL,
                        shift VARCHAR(1) NOT NULL, time_ DATETIME, start_time DATETIME NOT NULL, stop_time DATETIME,
                        duration INTEGER,meters DOUBLE NOT NULL DEFAULT(0),energy_start DOUBLE NOT NULL DEFAULT(0),
                        energy_stop DOUBLE NOT NULL DEFAULT(0),fluid_total_start DOUBLE NOT NULL DEFAULT(0),
                        fluid_total_stop DOUBLE NOT NULL DEFAULT(0),
                        air_total DOUBLE NOT NULL DEFAULT(0),water_total DOUBLE NOT NULL DEFAULT(0), run_id INTEGER,
                        operator_id INTEGER, po_id INTEGER, operation_id INTEGER,
                        PRIMARY KEY (run_data_id AUTOINCREMENT),
                        FOREIGN KEY (run_id) REFERENCES run_category(run_id),
                        FOREIGN KEY (po_id) REFERENCES po_data(po_id))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS stop_data(stop_data_id INTEGER NOT NULL, date_ DATE NOT NULL,
                        shift VARCHAR(1) NOT NULL, time_ DATETIME, start_time DATETIME NOT NULL, stop_time DATETIME,
                        duration INTEGER,energy_start DOUBLE NOT NULL DEFAULT(0),energy_stop DOUBLE NOT NULL DEFAULT(0),
                        fluid_total_start DOUBLE NOT NULL DEFAULT(0),
                        fluid_total_stop DOUBLE NOT NULL DEFAULT(0), air_total DOUBLE NOT NULL DEFAULT(0),
                        water_total DOUBLE NOT NULL DEFAULT(0), stop_id INTEGER, operator_id INTEGER, po_id INTEGER,
                        operation_id INTEGER,
                        PRIMARY KEY (stop_data_id AUTOINCREMENT),
                        FOREIGN KEY (stop_id) REFERENCES stop_category(stop_id),
                        FOREIGN KEY (po_id) REFERENCES po_data(po_id))''')

        self.c.execute('''CREATE TABLE IF NOT EXISTS misc(id INTEGER NOT NULL DEFAULT "1",
                        current_date_ DATE NOT NULL DEFAULT (date('now','localtime')),
                        current_shift VARCHAR(1) NOT NULL, current_po_id INTEGER, last_run_id INTEGER,
                        last_stop_id INTEGER,current_operator_id INTEGER, status BOOLEAN, energy DOUBLE DEFAULT 0,
                        nh_total DOUBLE DEFAULT 0)''')
        self.c.execute('''CREATE TABLE IF NOT EXISTS email_list(email)''')
        self.add_default_data(master_data)

    def disconnect(self):
        self.c.close()
        self.conn.close()

    def add_default_data(self, master_data):
        try:
            self.c.execute('''SELECT * FROM operation_list LIMIT 1''')
            check = self.c.fetchone()
            if check is None:
                for operations in master_data['operation_list']:
                    self.c.execute('''INSERT into operation_list(operation_id,name)
                                                       VALUES(?,?)''',
                                   (operations[0], operations[1]))
                self.conn.commit()
                log.info('Successful:' + 'Run data inserted in the database.')
            self.c.execute('''SELECT * FROM run_category LIMIT 1''')
            check = self.c.fetchone()
            if check is None:
                for run_name in master_data['run_category']:
                    self.c.execute('''INSERT into run_category(name,code)
                                           VALUES(?,?)''',
                                   (run_name[0], run_name[1]))
                self.conn.commit()
                log.info('Successful:' + 'Run data inserted in the database.')

            self.c.execute('''SELECT * FROM stop_category LIMIT 1''')
            check = self.c.fetchone()
            if check is None:
                for stop_name in master_data['stop_category']:
                    self.c.execute('''INSERT into stop_category(name,code)
                                           VALUES(?,?)''',
                                   (stop_name[0], stop_name[1]))
                self.conn.commit()
                self.c.execute('''UPDATE stop_category SET stop_id=100
                                       WHERE name='NO REASON SELECTED' ''')
                self.conn.commit()
                log.info('Successful:' + 'Stop data inserted in the database.')

            self.c.execute('''SELECT * FROM operator_list LIMIT 1''')
            check = self.c.fetchone()
            if check is None:
                for operator_name in master_data['operators']:
                    self.c.execute('''INSERT into operator_list(name)
                                           VALUES(?)''',
                                   (operator_name,))
                self.conn.commit()
                log.info('Successful:' + 'Operator data inserted in the database.')
            email_list = getEmailList()
            if email_list is not None:
                self.c.execute('''DELETE FROM email_list;''')
                for email in email_list:
                    self.c.execute('''INSERT into email_list(email)
                                    VALUES(?)''', (email,))
                self.conn.commit()
        except Exception as e:
            log.error("ERROR:" + str(e) + " Error getting shift data")

    def get_email_list(self):
        self.c.execute('''SELECT * FROM email_list''')
        try:
            emails = self.c.fetchall()
            emails = [item[0] for item in emails]
            return emails
        except:
            return None

    def update_email_list(self):
        try:
            email_list = getEmailList()
            if email_list is not None:
                self.c.execute('''DELETE FROM email_list;''')
                for email in email_list:
                    self.c.execute('''INSERT into email_list(email) VALUES(?)''', (email,))
                self.conn.commit()
        except Exception as e:
            log.error(f"Error updating email list {e}")

    def add_stoppage_data(self, prevD, prevS, prev_po_id, last_stop_id, stoppages):
        try:
            water_total = 0
            if prev_po_id is not None:
                self.c.execute(f'''SELECT start_time FROM stop_data WHERE date_=? AND shift=? AND stop_data_id=?
                                AND po_id=? LIMIT 1''',
                               (prevD, prevS, last_stop_id, prev_po_id))
                data = self.c.fetchone()
            else:
                self.c.execute(f'''SELECT start_time FROM stop_data WHERE date_=? AND shift=? AND stop_data_id=?
                                LIMIT 1''',
                               (prevD, prevS, last_stop_id))
                data = self.c.fetchone()
            if data is not None:
                self.c.execute('''UPDATE stop_data SET time_=datetime(CURRENT_TIMESTAMP, 'localtime'),
                                energy_stop=?,fluid_total_stop=?,air_total=?,water_total=? WHERE date_=? AND shift=? AND
                                stop_data_id=?''',
                               (stoppages['kWh'], stoppages['fluid_total'], stoppages['air_total'], water_total, prevD,
                                prevS, last_stop_id))
                self.conn.commit()
                log.info('Successful:' + 'Stoppage data updated in the database.')
                self.add_stoppage_duration(prevD, prevS, last_stop_id)
            else:
                self.close_stoppage()
                operator_id = self.get_operator_id()
                self.c.execute('''INSERT into stop_data(date_, shift, time_, start_time, energy_start,energy_stop,
                                fluid_total_start,fluid_total_stop,air_total, water_total, stop_id, operator_id,
                                po_id, operation_id)
                                VALUES(?, ?,datetime(CURRENT_TIMESTAMP, 'localtime'),
                                datetime(CURRENT_TIMESTAMP, 'localtime'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                               (prevD, prevS, stoppages['kWh'], stoppages['kWh'], stoppages['fluid_total'],
                                stoppages['fluid_total'], stoppages['air_total'], water_total,
                                stoppages['stop_category'], operator_id,
                                prev_po_id, stoppages['operation']))
                self.conn.commit()
                log.info('Successful:' + 'Stoppage Data inserted in the database.')
                self.c.execute('''SELECT stop_data_id from stop_data WHERE rowid=last_insert_rowid() LIMIT 1''')
                stop_data_id = self.c.fetchone()[0]
                self.update_last_stop(stop_data_id)
                self.c.execute(f'''SELECT * FROM stop_data WHERE date_=? AND shift=? AND stop_data_id=? LIMIT 1''',
                               (prevD, prevS, stop_data_id))
                stop_data = list(self.c.fetchone())
                log.info(stop_data)
                self.c.execute('''SELECT name FROM stop_category WHERE stop_id=? LIMIT 1''', (stop_data[13],))
                try:
                    stop_name = self.c.fetchone()[0]
                except Exception as e:
                    stop_name = 'No StopType'
                log.info(stop_name)
                stop_data[13] = stop_name
                self.c.execute('''SELECT name FROM operation_list WHERE operation_id = ? LIMIT 1''', (stop_data[16],))
                try:
                    operation_name = self.c.fetchone()[0]
                except Exception as e:
                    operation_name = 'NA'
                stop_data[16] = operation_name
                self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (stop_data[15],))
                try:
                    po_number = self.c.fetchone()[0]
                except Exception as e:
                    po_number = 'No PO Number'
                stop_data[15] = po_number
                create_stop_data(stop_data)
        except Exception as e:
            log.error("ERROR:" + str(e) + " Error adding stop data")

    def add_stoppage_duration(self, prevD, prevS, last_stop_id):
        try:
            time_ = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute('''SELECT start_time,stop_data_id FROM stop_data WHERE stop_time IS NULL''')
            start_list = self.c.fetchall()
            log.debug(start_list)
            for items in start_list:
                start_time = items[0]
                stop_data_id = items[1]
                duration = (datetime.now() - datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")).seconds
                self.c.execute('''UPDATE stop_data SET time_=?,duration=?
                                WHERE stop_time is NULL AND stop_data_id=?''', (time_, duration, stop_data_id))
            self.conn.commit()
            # self.c.execute('''DELETE FROM stop_data WHERE duration<15 AND NOT stop_time is NULL''')
            # self.conn.commit()
            log.debug('Successful: ' + 'Stoppage duration updated successfully in the database.')
            self.c.execute(f'''SELECT * FROM stop_data WHERE date_=? AND shift=? AND stop_data_id=? LIMIT 1''',
                           (prevD, prevS, last_stop_id))
            stop_data = list(self.c.fetchone())
            res = post_stop_data(stop_data_id=stop_data[0], time_=stop_data[3], stop_time=time_,
                                 duration=stop_data[6],
                                 energy_stop=stop_data[8], fluid_total=stop_data[10] - stop_data[9],
                                 air_total=stop_data[11],
                                 water_total=stop_data[12])
            if res == 404:
                log.info(stop_data)
                self.c.execute('''SELECT name FROM stop_category WHERE stop_id=? LIMIT 1''', (stop_data[13],))
                try:
                    stop_name = self.c.fetchone()[0]
                except Exception as e:
                    stop_name = 'No StopType'
                log.info(stop_name)
                stop_data[13] = stop_name
                # self.c.execute('''SELECT name FROM operator_list WHERE operator_id = ? LIMIT 1''', (stop_data[13],))
                # try:
                #     operator_name = self.c.fetchone()[0]
                # except Exception as e:
                #     operator_name = 'No Operator'
                # log.info(operator_name)
                # stop_data[13] = operator_name
                self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (stop_data[14],))
                try:
                    po_number = self.c.fetchone()[0]
                except Exception as e:
                    po_number = 'No PO Number'
                log.info(po_number)
                stop_data[14] = po_number
                create_stop_data(stop_data)
        except Exception as e:
            log.error('Error:' + str(e) + ' Could not update stoppage duration in the database.')

    def close_stoppage(self):
        try:
            time_ = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute('''SELECT start_time,stop_data_id FROM stop_data WHERE stop_time IS NULL''')
            start_list = self.c.fetchall()
            log.debug(start_list)
            for items in start_list:
                start_time = items[0]
                stop_data_id = items[1]
                if datetime.now() > datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S"):
                    duration = (datetime.now() - datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")).seconds
                    self.c.execute('''UPDATE stop_data SET time_=?, stop_time=?,duration=?
                                    WHERE stop_time is NULL AND stop_data_id=?''',
                                   (time_, time_, duration, stop_data_id))
                    post_stop_data(stop_data_id=stop_data_id, time_=time_, stop_time=time_,
                                   duration=duration)
                else:
                    stop_time = (datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=10)).strftime(
                        "%Y-%m-%d %H:%M:%S")
                    self.c.execute('''UPDATE stop_data SET time_=?, stop_time=?,duration=?
                                    WHERE stop_time is NULL AND stop_data_id=?''',
                                   (time_, stop_time, 10, stop_data_id))
                    post_stop_data(stop_data_id=stop_data_id, time_=time_, stop_time=stop_time,
                                   duration=10)
            self.conn.commit()
            self.update_last_stop(0)
            log.info('Successful: ' + 'Stoppage is closed successfully in the database.')
        except Exception as e:
            log.error('ERROR' + str(e) + 'Could not close Stoppage to the database.')

    def add_run_data(self, prevD, prevS, prev_po_id, last_run_id, production):
        try:
            water_total = 0
            if prev_po_id is not None:
                self.c.execute(f'''SELECT start_time FROM run_data WHERE date_=? AND shift=? AND run_data_id=? AND po_id=?
                                LIMIT 1''',
                               (prevD, prevS, last_run_id, prev_po_id))
                data = self.c.fetchone()
            else:
                self.c.execute(f'''SELECT start_time FROM run_data WHERE date_=? AND shift=? AND run_data_id=?
                                LIMIT 1''',
                               (prevD, prevS, last_run_id))
                data = self.c.fetchone()
            if data is not None:
                self.c.execute('''UPDATE run_data SET time_=datetime(CURRENT_TIMESTAMP, 'localtime'),meters=?,
                                energy_stop=?,fluid_total_stop=?,air_total=?,water_total=? WHERE date_=? AND shift=? AND
                                run_data_id=?''',
                               (production['meter'], production['kWh'], production['fluid_total'],
                                production['air_total'], water_total, prevD, prevS, last_run_id))
                self.conn.commit()
                log.info('Successful:' + 'Running data updated in the database.')
                self.add_run_duration(prevD, prevS, last_run_id)  # also post data to FAPI server
            else:
                self.close_run()
                operator_id = self.get_operator_id()
                self.c.execute('''INSERT into run_data(date_,shift,time_,start_time,meters,energy_start,energy_stop,
                                fluid_total_start,fluid_total_stop,air_total, water_total, run_id, operator_id, po_id,
                                operation_id)
                                VALUES(?,?,datetime(CURRENT_TIMESTAMP, 'localtime'),
                                datetime(CURRENT_TIMESTAMP, 'localtime'),?,?,?,?,?,?,?,?,?,?,?)''',
                               (prevD, prevS, production['meter'], production['kWh'], production['kWh'],
                                production['fluid_total'], production['fluid_total'],
                                production['air_total'], water_total, production['run_category'], operator_id,
                                prev_po_id, production['operation']))
                self.conn.commit()
                log.info('Successful: ' + 'Running Data inserted in the database.')
                self.c.execute('''SELECT run_data_id from run_data WHERE rowid=last_insert_rowid() LIMIT 1''')
                run_data_id = self.c.fetchone()[0]
                self.update_last_run(run_data_id)
                self.c.execute(f'''SELECT * FROM run_data WHERE date_=? AND shift=? AND run_data_id=? LIMIT 1''',
                               (prevD, prevS, run_data_id))
                run_data = list(self.c.fetchone())
                log.info(run_data)
                self.c.execute('''SELECT name FROM run_category WHERE run_id=? LIMIT 1''', (run_data[14],))
                try:
                    run_name = self.c.fetchone()[0]
                    log.info(run_name)
                except:
                    run_name = 'No RunType'
                run_data[14] = run_name
                self.c.execute('''SELECT name FROM operation_list WHERE operation_id = ? LIMIT 1''', (run_data[17],))
                try:
                    operation_name = self.c.fetchone()[0]
                    log.info(operation_name)
                except:
                    operation_name = 'NA'
                run_data[17] = operation_name
                self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (run_data[16],))
                try:
                    po_number = self.c.fetchone()[0]
                    log.info(po_number)
                except:
                    po_number = 'No PO Number'
                run_data[16] = po_number
                create_run_data(run_data)
        except Exception as e:
            log.error("ERROR:" + str(e) + " Error adding run data")

    def add_run_duration(self, prevD, prevS, last_run_id):
        try:
            time_ = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute('''SELECT start_time,run_data_id FROM run_data WHERE stop_time IS NULL''')
            start_list = self.c.fetchall()
            log.debug(start_list)
            for items in start_list:
                start_time = items[0]
                run_data_id = items[1]
                duration = (datetime.now() - datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")).seconds
                self.c.execute('''UPDATE run_data SET time_=?,duration=?
                                WHERE stop_time is NULL AND run_data_id=?''', (time_, duration, run_data_id))
            self.conn.commit()
            # self.c.execute('''DELETE FROM stop_data WHERE duration<15 AND NOT stop_time is NULL''')
            # self.conn.commit()
            log.debug('Successful: ' + 'Running duration updated successfully in the database.')
            self.c.execute(f'''SELECT * FROM run_data WHERE date_=? AND shift=? AND run_data_id=? LIMIT 1''',
                           (prevD, prevS, last_run_id))
            run_data = list(self.c.fetchone())
            res = post_run_data(run_data_id=run_data[0], time_=run_data[3], stop_time=time_, duration=run_data[6],
                                meters=run_data[7], energy_stop=run_data[9],
                                fluid_total=run_data[11] - run_data[10], air_total=run_data[12],
                                water_total=run_data[13])
            if res == 404:
                self.c.execute('''SELECT name FROM run_category WHERE run_id=? LIMIT 1''', (run_data[14],))
                try:
                    run_name = self.c.fetchone()[0]
                except Exception as e:
                    run_name = 'No RunType'
                log.info(run_data)
                run_data[14] = run_name
                # self.c.execute('''SELECT name FROM operator_list WHERE operator_id = ? LIMIT 1''', (run_data[14],))
                # try:
                #     operator_name = self.c.fetchone()[0]
                # except Exception as e:
                #     operator_name = 'No Operator'
                # log.info(operator_name)
                # run_data[14] = operator_name
                self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (run_data[16],))
                try:
                    po_number = self.c.fetchone()[0]
                except Exception as e:
                    po_number = 'No PO Number'
                log.info(po_number)
                run_data[16] = po_number

                self.c.execute('''SELECT name FROM operation_list WHERE operation_id = ? LIMIT 1''',
                               (run_data[17],))
                try:
                    operation_name = self.c.fetchone()[0]
                    log.info(operation_name)
                except Exception as e:
                    log.error(f"Error while fetching operation name {e}")
                    operation_name = 'NA'
                run_data[17] = operation_name

                create_run_data(run_data)
        except Exception as e:
            log.error('Error:' + str(e) + ' Could not update running duration in the database.')

    def close_run(self):
        try:
            time_ = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute('''SELECT start_time,run_data_id,duration FROM run_data WHERE stop_time IS NULL''')
            start_list = self.c.fetchall()
            log.debug(start_list)
            for items in start_list:
                start_time = items[0]
                run_data_id = items[1]
                duration = items[2]
                if duration is not None:
                    if duration > 0:
                        stop_time = (datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + timedelta(
                            seconds=duration)).strftime("%Y-%m-%d %H:%M:%S")
                        self.c.execute('''UPDATE run_data SET time_=?, stop_time=?,duration=?
                                            WHERE stop_time is NULL AND run_data_id=?''',
                                       (time_, stop_time, duration, run_data_id))
                        post_run_data(run_data_id=run_data_id, time_=time_, stop_time=stop_time, duration=duration)
                else:
                    stop_time = (datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=10)).strftime(
                        "%Y-%m-%d %H:%M:%S")
                    self.c.execute('''UPDATE run_data SET time_=?, stop_time=?,duration=?
                                    WHERE stop_time is NULL AND run_data_id=?''',
                                   (time_, stop_time, 10, run_data_id))
                    post_run_data(run_data_id=run_data_id, time_=time_, stop_time=stop_time, duration=10)
            self.conn.commit()
            self.update_last_run(0)
            log.info('Successful: ' + 'Running is closed successfully in the database.')
        except Exception as e:
            log.error('ERROR' + str(e) + 'Could not close running to the database.')

    def add_po_data(self, po_number, article, greige_glm, finish_glm, construction):
        try:
            log.info(f"{po_number}, {article}, {greige_glm}, {finish_glm}, {construction}")
            po_number = po_number.replace("\x00", '')
            article = article.replace("\x00", '') if article is not None else None
            # greige_glm = greige_glm.replace("\x00", '') if greige_glm is not None else None
            # finish_glm = finish_glm.replace("\x00", '') if finish_glm is not None else None
            construction = construction.replace("\x00", '') if construction is not None else None

            self.c.execute('''INSERT OR IGNORE INTO po_data(po_number, article, greige_glm, finish_glm, construction)
                           VALUES(?,?,?,?,?)''', (po_number, article, greige_glm, finish_glm, construction))
            self.conn.commit()
            self.c.execute('''UPDATE po_data SET article=?, greige_glm=?, finish_glm=?, construction=?
                            WHERE po_number=?;''', (article, greige_glm, finish_glm, construction, po_number))
            self.conn.commit()
            log.info('Successful: PO Data added to the database.')
            self.c.execute("SELECT * FROM po_data WHERE po_number=?", (po_number,))
            res = self.c.fetchone()
            po_id, po_number, article, greige_glm, finish_glm, construction, hmi = res
            log.info(f"HMI DATA : {hmi}")
            payload = {
                "po_id": po_id, "po_number": po_number, "article": article, "greige_glm": greige_glm,
                "finish_glm": finish_glm, "construction": construction,
                "machine": machine_info["name"], "plant_name": machine_info["plant"]
            }
            send_po_data(payload)
            # self.update_po_id(po_number)
        except Exception as e:
            log.error('ERROR: ' + str(e) + ' Could not add PO DATA to the database.')

    def add_manual_data(self, manual_data: dict):
        try:
            log.info(f"{manual_data}")
            curr_po_id = self.get_po_id()
            if curr_po_id == 0:
                return
            self.c.execute('''SELECT hmi_data FROM po_data WHERE po_id = ? LIMIT 1''', (curr_po_id,))
            try:
                hmi_data = self.c.fetchone()[0]
            except Exception as e:
                hmi_data = None
            if hmi_data is None:
                return
            log.info(hmi_data)
            hmi_data = json.loads(hmi_data)
            if hmi_data.get('trolley') is not None:
                trolley_set = set(hmi_data['trolley'])
                if manual_data.get('trolley') is not None:
                    trolley_set.add(manual_data['trolley'])
                    hmi_data['trolley'] = list(trolley_set)
            else:
                if manual_data.get('trolley') is not None:
                    hmi_data['trolley'] = [manual_data['trolley']]

            if hmi_data.get('tw_values') is not None:
                if manual_data.get('tw_values') is not None:
                    hmi_data['tw_values']['temperature'] = manual_data['tw_values'][0]
                    hmi_data['tw_values']['width'] = manual_data['tw_values'][1]
            else:
                if manual_data.get('tw_values') is not None:
                    hmi_data['tw_values'] = {}
                    hmi_data['tw_values']['temperature'] = manual_data['tw_values'][0]
                    hmi_data['tw_values']['width'] = manual_data['tw_values'][1]
            log.info(hmi_data)

            self.c.execute('''UPDATE po_data SET hmi_data=? 
                             WHERE po_id=?;''', (json.dumps(hmi_data), curr_po_id))

            log.info('Successful: Manual HMI Entry added to the database.')
            res = self.c.execute("SELECT * FROM po_data WHERE po_id = ?", (curr_po_id,))
            res = res.fetchone()
            po_id, po_number, article, greige_glm, finish_glm, construction, hmi = res
            hmi = json.loads(hmi)
            payload = {
                "po_id": po_id, "po_number": po_number, "article": article, "greige_glm": greige_glm,
                "finish_glm": finish_glm, "construction": construction, "hmi_data": hmi,
                "machine": machine_info["name"], "plant_name": machine_info["plant"]
            }
            for i, v in payload.items():
                if v == None or v == "":
                    payload[i] = 0
            self.conn.commit()
            log.info(f"Payload : {payload}")
            send_po_data(payload)
        except Exception as e:
            log.error('ERROR: ' + str(e) + ' Could not add Manual HMI Entry to the database.')

    def get_run_data(self, date, shift):
        self.c.execute('''SELECT meters,(energy_stop - energy_start) as KWh,
                        (fluid_total_stop - fluid_total_start) as fluid_total,
                        air_total,water_total,run_id,operator_id,po_id FROM run_data
                        WHERE date_=? AND shift=? LIMIT 1''', (date, shift))
        try:
            data = list(self.c.fetchone())
            log.info(data)
            self.c.execute('''SELECT name FROM run_category WHERE run_id=? LIMIT 1''', (data[6],))
            try:
                run_name = self.c.fetchone()[0]
            except:
                run_name = 'No RunType'
            log.info(run_name)
            data[6] = run_name
            # self.c.execute('''SELECT name FROM operator_list WHERE operator_id = ? LIMIT 1''', (data[7],))
            # try:
            #     operator_name = self.c.fetchone()[0]
            # except:
            #     operator_name = 'No Operator Name'
            # log.info(operator_name)
            # data[7] = operator_name
            self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (data[8],))
            try:
                po_number = self.c.fetchone()[0]
            except:
                po_number = 'No po_number'
            log.info(po_number)
            data[8] = po_number
            return data
        except Exception as e:
            log.error(e)
            return None

    def get_stop_data(self, date, shift):
        self.c.execute('''SELECT duration,(energy_stop - energy_start) as KWh, 
                                 (fluid_total_stop - fluid_total_start) as fluid_total,
                                air_total,water_total,stop_id,operator_id,po_id FROM stop_data
                                WHERE date_=? AND shift=? LIMIT 1''', (date, shift))
        try:
            data = list(self.c.fetchone())
            log.info(data)
            self.c.execute('''SELECT name FROM stop_category WHERE stop_id=? LIMIT 1''', (data[6],))
            try:
                stop_name = self.c.fetchone()[0]
            except:
                stop_name = 'No StopType'
            log.info(stop_name)
            data[6] = stop_name
            # self.c.execute('''SELECT name FROM operator_list WHERE operator_id = ? LIMIT 1''', (data[7],))
            # try:
            #     operator_name = self.c.fetchone()[0]
            # except:
            #     operator_name = 'No operator_name'
            # log.info(operator_name)
            # data[7] = operator_name
            self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (data[8],))
            try:
                po_number = self.c.fetchone()[0]
            except:
                po_number = 'No po_number'
            log.info(po_number)
            data[8] = po_number
            return data
        except Exception as e:
            log.error(e)
            return None

    def get_stop_min(self, date, shift):
        self.c.execute(''' SELECT shift,SUM(duration) FROM stop_data  WHERE date_=? AND shift=?
                        GROUP BY shift''', (date, shift))
        try:
            data = self.c.fetchone()
            return data
        except Exception as e:
            log.error(e)
            return None

    def get_daily_calculations(self, today):
        try:
            stop_data, stop_duration = self.get_daily_stop_data(today)
            run_data, run_duration, meters = self.get_daily_run_data(today)
            totalTime = (convertTime(datetime.now().time()) - convertTime(shift_a_start)).seconds
            utilization = run_duration * 100 / totalTime
            daily_data = [item + stop_data[idx] for idx, item in enumerate(run_data)]
            # log.info(f'{daily_data} {totalTime} {utilization} {meters}')
            return daily_data, utilization, meters
        except Exception as e:
            log.error("ERROR:" + str(e) + " Error getting daily run data")
            return [0] * 4, 0, 0

    def get_daily_run_data(self, today):
        try:
            self.c.execute('''SELECT SUM(duration),SUM(meters),SUM(energy_stop - energy_start) as KWh,
                            SUM(fluid_total_stop-fluid_total_start),
                            SUM(air_total),SUM(water_total) FROM run_data
                            WHERE date_=? AND energy_start<=energy_stop AND 
                            NOT run_id=(SELECT run_id FROM run_category WHERE name='Lead cloth' LIMIT 1)
                            GROUP BY date_ ORDER BY run_data_id DESC''', (today,))
            data = self.c.fetchone()
            # log.info(data)
            if data is not None:
                if len(data) != 0:
                    run_data = list(data[2:])
                    run_duration = data[0]
                    meters = data[1]
                    # log.info(f'{run_data} {run_duration} {meters}')
                else:
                    run_data = [0] * 4
                    run_duration = 0
                    meters = 0
                return run_data, run_duration, meters
            return [0] * 4, 0, 0
        except Exception as e:
            log.error("ERROR:" + str(e) + " Error getting daily run data")
            return [0] * 4, 0, 0

    def get_daily_stop_data(self, today):
        try:
            self.c.execute('''SELECT SUM(duration),SUM(energy_stop - energy_start) as KWh,
                            SUM(fluid_total_stop-fluid_total_start),
                            SUM(air_total),SUM(water_total) FROM stop_data
                            WHERE date_=? AND energy_start<=energy_stop
                            GROUP BY date_ ORDER BY stop_data_id DESC''', (today,))
            data = self.c.fetchone()
            if data is not None:
                if len(data) != 0:
                    stop_data = list(data[1:])
                    stop_duration = data[0]
                    # log.info(f'{stop_data} {stop_duration}')
                else:
                    stop_data = [0] * 4
                    stop_duration = 0
                return stop_data, stop_duration
            return [0] * 4, 0
        except Exception as e:
            log.error("ERROR:" + str(e) + " Error getting daily stop data")
            return [0] * 4, 0

    def get_daily_hmi_length(self, today):
        try:
            self.c.execute('''SELECT SUM(meters) FROM run_data
                            WHERE date_=? AND
                            NOT run_id=(SELECT run_id FROM run_category WHERE name='Lead cloth' LIMIT 1)
                            GROUP BY date_ ORDER BY run_data_id DESC''', (today,))
            data = self.c.fetchone()
            # log.info(data)
            if data is not None:
                if len(data) != 0:
                    meters = int(data[0])
                    # log.info(f'HMI Length: {meters}')
                else:
                    meters = 0
                return meters
            return 0
        except Exception as e:
            log.error("ERROR:" + str(e) + " Error getting hmi run data")
            return 0

    def get_daily_po_data(self, today):
        try:
            self.c.execute('''SELECT run_data.po_id,po_data.po_number, SUM(run_data.meters) FROM run_data
                            LEFT OUTER JOIN po_data ON run_data.po_id = po_data.po_id
                            WHERE run_data.date_=? AND NOT run_data.po_id is NULL AND
                            NOT run_data.run_id=(SELECT run_category.run_id FROM run_category
                            WHERE run_category.name='Lead cloth' LIMIT 1)
                            GROUP BY run_data.po_id ORDER BY run_data.run_data_id DESC''', (today,))
            data = self.c.fetchall()
            log.debug(data)
            po_list = []
            meters_list = []
            if data is not None:
                if len(data) != 0 and data[0] != ():
                    for po_id, po, length in data:
                        if po is not None:
                            po_list.append(po)
                            meters_list.append(length)
                    log.debug(f'PO and Length List: {po_list} ## {meters_list}')
                else:
                    po_list = [""]
                    meters_list = [0]
            else:
                po_list = [""]
                meters_list = [0]
            return po_list, meters_list
        except Exception as e:
            log.error("ERROR:" + str(e) + " Error getting hmi run data")
            po_list = [""]
            meters_list = [0]
            return po_list, meters_list

    def get_shift_hmi_mins(self, today, shift):
        try:
            self.c.execute('''SELECT SUM(duration) FROM stop_data
                            WHERE date_=? AND shift=?
                            GROUP BY date_ ORDER BY stop_data_id DESC''', (today, shift,))
            data = self.c.fetchone()
            # log.info(data)
            if data is not None:
                if len(data) != 0:
                    duration = int(data[0] / 60)
                    # log.info(f'HMI Stop mins: {duration}')
                else:
                    duration = 0
                return duration
            return 0
        except Exception as e:
            log.error("ERROR:" + str(e) + " Error getting hmi stop mins data")
            return 0

    def deleteMachines(self):
        try:
            self.c.execute('''DELETE FROM machineList''')
            self.conn.commit()
            log.info("Machines removed from the database")
        except Exception as e:
            log.error("Error deleting Machines from database" + str(e))

    # function for  values from database return for setting the value

    def getReportData(self, shiftId, day):
        # get shift count

        shift_count = []
        for s in ['A', 'B', 'C']:
            self.c.execute('''SELECT COUNT(scanText) FROM scanData
                            WHERE machineId = ? AND date_ = ? AND shift = ? 
                            GROUP BY shift''',
                           (shiftId, day, s))
            try:
                shift_count.append(self.c.fetchone()[0])
            except:
                shift_count.append(0)

        # get day count
        self.c.execute('''SELECT SUM(prodCount) FROM hourData WHERE machineId = ? AND date_ = ?
                                GROUP BY date_''', (shiftId, day))
        try:
            day_count = self.c.fetchone()[0]
        except:
            day_count = 0

        # get hourly production
        self.c.execute('''SELECT hour_, prodCount, strftime("%H:%M:%S",time_) FROM hourData WHERE machineId = ? 
                        AND date_ = ? ORDER BY hourId''',
                       (shiftId, day))
        try:
            hour_count = self.c.fetchall()
        except:
            hour_count = [0, 0, 0]

        # get Scan Data
        self.c.execute('''SELECT strftime("%H:%M:%S",time_), scanText FROM scanData WHERE date_ = ? AND machineId = ?
                        ORDER BY time_''', (day, shiftId))
        try:
            scan_data = self.c.fetchall()
        except:
            scan_data = [0, 0]

        return shift_count, day_count, hour_count, scan_data

    def getCurrShift(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT current_shift FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()[0]
        except:
            data = 'N'
        return data

    def getCurrDate(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT current_date_ FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()[0]
        except:
            data = 'N'
        return data

    def getMiscData(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT current_shift, current_date_, status, current_po_id FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()
            prevS = data[0]
            prevD = data[1]
            prev_status = data[2]
            prev_po_id = data[3]
            return prevS, prevD, prev_status, prev_po_id
        except Exception as e:
            log.error(f'ERROR: fetching misc data {e}')
            return 'N', 'N', 0, 'N'

    def get_last_stop_id(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT last_stop_id FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()
            stop_id = data[0]
            return stop_id
        except:
            return None

    def get_status(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT status FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()
            int_status = data[0]
            if int_status == 1:
                status = True
            else:
                status = False
            return status
        except:
            return None

    def get_po_number(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT po_number FROM po_data 
                        WHERE po_id=(SELECT current_po_id FROM misc WHERE id=1 LIMIT 1)''')
        try:
            data = self.c.fetchone()
            po_number = data[0]
            return po_number
        except:
            return 0

    def get_po_id_number(self, po_number):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT po_id FROM po_data 
                        WHERE po_number=?''', (po_number,))
        try:
            data = self.c.fetchone()
            po_id = data[0]
            return po_id
        except Exception as e:
            print(e)
            return 0

    def get_po_id(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT current_po_id FROM misc WHERE id=1 LIMIT 1''')
        try:
            data = self.c.fetchone()
            po_id = data[0]
            if po_id is None:
                return 0
            return po_id
        except:
            return 0

    def get_po_details(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT po_number,article,greige_glm,finish_glm,construction FROM po_data 
                        WHERE po_id=(SELECT current_po_id FROM misc WHERE id=1 LIMIT 1)''')
        try:
            data = self.c.fetchone()
            po_number, article, greige_glm, finish_glm, construction = data
            return po_number, article, greige_glm, finish_glm, construction
        except:
            return None, None, None, None, None

    def get_operator_name(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT current_operator_id FROM misc WHERE id=1 LIMIT 1''')
        try:
            data = self.c.fetchone()
            operator_name = data[0]
            return operator_name
        except:
            return None

    def get_last_run_id(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT last_run_id FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()
            stop_id = data[0]
            return stop_id
        except:
            return None

    def get_operator_id(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT current_operator_id FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()
            stop_id = data[0]
            return stop_id
        except:
            return None

    def get_last_energy(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT energy FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()
            energy = data[0]
            return energy
        except:
            return None

    def get_last_nh(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT nh_total FROM misc WHERE id=1''')
        try:
            data = self.c.fetchone()
            nh_total = data[0]
            return nh_total
        except:
            return None

    def get_current_run_category(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT run_id FROM run_data  
                        WHERE run_data_id=(SELECT last_run_id FROM misc WHERE id=1 LIMIT 1)''')
        try:
            data = self.c.fetchone()
            run_category_id = data[0]
            return run_category_id
        except:
            return None

    def get_current_run_name(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT name FROM run_category  
                        WHERE run_id=(SELECT run_id FROM run_data  
                        WHERE run_data_id=(SELECT last_run_id FROM misc WHERE id=1 LIMIT 1))''')
        try:
            data = self.c.fetchone()
            run_category = data[0]
            return run_category
        except:
            return None

    def get_current_stop_category(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT stop_id FROM stop_data  
                        WHERE stop_data_id=(SELECT last_stop_id FROM misc WHERE id=1 LIMIT 1)''')
        try:
            data = self.c.fetchone()
            stop_category_id = data[0]
            return stop_category_id
        except:
            return None

    def get_current_stop_name(self):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute('''SELECT name FROM stop_category 
                        WHERE stop_id=(SELECT stop_id FROM stop_data  
                        WHERE stop_data_id=(SELECT last_stop_id FROM misc WHERE id=1 LIMIT 1))''')
        try:
            data = self.c.fetchone()
            stop_category_id = data[0]
            return stop_category_id
        except:
            return None

    def get_operation_name(self, operation_id):
        self.c.execute('''SELECT name FROM operation_list WHERE operation_id = ? LIMIT 1''', (operation_id,))
        try:
            operation_name = self.c.fetchone()[0]
        except Exception as e:
            operation_name = 'NA'
        return operation_name

    def updateCurrDate(self, today):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute("UPDATE misc SET current_date_=?", (today,))
        self.conn.commit()
        log.info('Successful:' + 'Date updated successfully in database.')

    def updateCurrShift(self, shift):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute("UPDATE misc SET current_shift=?", (shift,))
        self.conn.commit()
        log.info('Successful:' + 'Shift updated successfully in database.')

    def update_last_stop(self, stop_data_id):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute("UPDATE misc SET last_stop_id=?", (stop_data_id,))
        self.conn.commit()
        log.info('Successful:' + ' Stop ID updated successfully in database.')

    def update_last_run(self, run_data_id):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute("UPDATE misc SET last_run_id=?", (run_data_id,))
        self.conn.commit()
        log.info('Successful:' + ' Run ID updated successfully in database.')

    def update_po_id(self, po_number):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        log.info(po_number)
        if po_number == 0:
            self.c.execute('''UPDATE misc SET current_po_id = 0''')
            self.conn.commit()
        else:
            self.c.execute('''SELECT po_id FROM po_data WHERE po_number=? LIMIT 1''', (po_number,))
            try:
                po_id = self.c.fetchone()[0]
                print(po_id)
            except Exception as e:
                log.error("PO Number not in database")
                po_id = 0
            self.c.execute('''UPDATE misc SET current_po_id=?''',
                           (po_id,))
            self.conn.commit()

        log.info('Successful:' + ' PO ID updated successfully in database.')

    def update_status(self, status):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute("UPDATE misc SET status=?", (status,))
        self.conn.commit()
        log.info('Successful:' + 'Machine Status updated successfully in database.')

    def update_operator_id(self, operator_id):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute("UPDATE misc SET current_operator_id=?", (operator_id,))
        self.conn.commit()
        log.info('Successful:' + 'Operator ID updated successfully in database.')

    def updateLastEnergy(self, energy):
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute("UPDATE misc SET energy=?", (energy,))
        self.conn.commit()
        # log.info('Successful:' + 'Energy updated successfully in database.')

    def updateLastHeat(self, nh_total):
        if nh_total is not None:
            nh_total = nh_total / 100
        self.c.execute('''SELECT * FROM misc''')
        check = self.c.fetchone()
        if check is None:
            self.addMiscData()
        self.c.execute("UPDATE misc SET nh_total=?", (nh_total,))
        self.conn.commit()
        # log.info('Successful:' + 'Energy updated successfully in database.')

    def addMiscData(self):
        try:
            shift = getShift()
            self.c.execute('''INSERT INTO misc(id, current_shift, current_date_, current_po_id, last_stop_id, status)
                           VALUES (?,?,?,?,?,?)''', (1, shift, datetime.now().strftime("%F"), 0, 0, 0))
            self.conn.commit()
            log.info('Successful: Misc Data added to the database.')
        except Exception as e:
            log.error('ERROR: ' + str(e) + ' Could not add Misc DATA to the database.')

    def get_run_report(self, date, shift):
        self.c.execute('''SELECT po_id, po_id, po_id, po_id, operation_id, run_id,
                        strftime('%H:%M:%S',start_time), strftime('%H:%M:%S',stop_time),
                        (duration) as duration, meters,air_total,
                         (fluid_total_stop - fluid_total_start) as fluid_total, 
                         water_total,(energy_stop - energy_start) as energy,operator_id
                        FROM run_data WHERE date_=? AND shift=? 
                        ORDER BY date_,start_time''', (date, shift))
        try:
            data = self.c.fetchall()

            # log.info(data)
            report_data = []
            for datapoint in data:
                datapoint = list(datapoint)
                if datapoint[11] < 0:
                    datapoint[11] = 0
                else:
                    datapoint[11] = datapoint[11]
                self.c.execute('''SELECT article FROM po_data  WHERE po_id=? LIMIT 1''', (datapoint[0],))
                try:
                    article = self.c.fetchone()[0]
                except:
                    article = ''
                # log.info(article)
                datapoint[0] = article

                self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (datapoint[1],))
                try:
                    po_number = self.c.fetchone()[0]
                except:
                    po_number = ''
                # log.info(po_number)
                datapoint[1] = po_number

                self.c.execute('''SELECT greige_glm FROM po_data WHERE po_id = ? LIMIT 1''', (datapoint[2],))
                try:
                    greige_glm = self.c.fetchone()[0]
                except:
                    greige_glm = ''
                # log.info(po_number)
                datapoint[2] = greige_glm

                self.c.execute('''SELECT finish_glm FROM po_data WHERE po_id = ? LIMIT 1''', (datapoint[3],))
                try:
                    finish_glm = self.c.fetchone()[0]
                except:
                    finish_glm = ''
                # log.info(finish_glm)
                datapoint[3] = finish_glm

                self.c.execute('''SELECT name FROM operation_list WHERE operation_id = ? LIMIT 1''', (datapoint[4],))
                try:
                    operation_type = self.c.fetchone()[0]
                except:
                    operation_type = 'No Operation Type'
                # log.info( operation_type)
                datapoint[4] = operation_type

                self.c.execute('''SELECT name FROM run_category WHERE run_id=? LIMIT 1''', (datapoint[5],))
                try:
                    run_name = self.c.fetchone()[0]
                except:
                    run_name = 'No RunType'
                # log.info(run_name)
                datapoint[5] = run_name

                # self.c.execute('''SELECT name FROM operator_list WHERE operator_id = ? LIMIT 1''', (datapoint[14],))
                # try:
                #     operator_name = self.c.fetchone()[0]
                # except:
                #     operator_name = 'No Operator'
                # # log.info(operator_name)
                # datapoint[14] = operator_name

                report_data.append(datapoint)
            return report_data
        except Exception as e:
            log.error(e)
            return None

    def get_stop_report(self, date, shift):
        self.c.execute('''SELECT po_id, po_id, po_id, po_id, stop_id, strftime('%H:%M:%S',start_time),
                        strftime('%H:%M:%S',stop_time), (duration) as duration,
                         (fluid_total_stop - fluid_total_start) as fluid_total,
                         (energy_stop - energy_start) as energy,
                        water_total, air_total, operator_id                     
                        FROM stop_data WHERE date_=? AND shift=? 
                        ORDER BY date_,start_time''', (date, shift))
        try:
            data = self.c.fetchall()
            # log.info(data)
            report_data = []
            for datapoint in data:
                datapoint = list(datapoint)
                if datapoint[8] < 0:
                    datapoint[8] = 0
                else:
                    datapoint[8] = datapoint[8]

                self.c.execute('''SELECT article FROM po_data WHERE po_id = ? LIMIT 1''', (datapoint[0],))
                try:
                    article = self.c.fetchone()[0]
                except:
                    article = ''
                # log.info(article)
                datapoint[0] = article

                self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (datapoint[1],))
                try:
                    po_number = self.c.fetchone()[0]
                except:
                    po_number = ''
                # log.info(po_number)
                datapoint[1] = po_number

                self.c.execute('''SELECT greige_glm FROM po_data WHERE po_id = ? LIMIT 1''', (datapoint[2],))
                try:
                    greige_glm = self.c.fetchone()[0]
                except:
                    greige_glm = ''
                # log.info(greige_glm)
                datapoint[2] = greige_glm

                self.c.execute('''SELECT finish_glm FROM po_data WHERE po_id = ? LIMIT 1''', (datapoint[3],))
                try:
                    finish_glm = self.c.fetchone()[0]
                except:
                    finish_glm = ''
                # log.info(finish_glm)
                datapoint[3] = finish_glm

                self.c.execute('''SELECT name FROM stop_category WHERE stop_id=? LIMIT 1''', (datapoint[4],))
                try:
                    stop_name = self.c.fetchone()[0]
                except:
                    stop_name = 'No StopType'
                # log.info(stop_name)
                datapoint[4] = stop_name

                # self.c.execute('''SELECT name FROM operator_list WHERE operator_id = ? LIMIT 1''', (datapoint[12],))
                # try:
                #     operator_name = self.c.fetchone()[0]
                # except:
                #     operator_name = 'No Operator'
                # # log.info(operator_name)
                # datapoint[12] = operator_name

                report_data.append(datapoint)
            return report_data
        except Exception as e:
            log.error(e)
            return None

    def get_downtown_report(self, today):
        self.c.execute('''SELECT stop_id,SUM(duration)/60  FROM stop_data
                        WHERE date_=? GROUP BY stop_id ORDER by SUM(duration) DESC''', (today,))
        try:
            data = self.c.fetchall()
            report_data = []
            for datapoint in data:
                datapoint = list(datapoint)
                self.c.execute('''SELECT name FROM stop_category WHERE stop_id=? LIMIT 1''', (datapoint[0],))
                try:
                    stop_name = self.c.fetchone()[0]
                except:
                    stop_name = 'No StopType'
                # log.info(stop_name)
                datapoint[0] = stop_name
                report_data.append(datapoint)
            return report_data
        except Exception as e:
            log.error(e)
            return None

    def get_changeover_report(self, today):
        self.c.execute('''SELECT SUM(duration/60), count(duration)  FROM stop_data
                        WHERE date_=?  AND stop_id = 3 GROUP BY stop_id LIMIT 1''', (today,))
        try:
            data = self.c.fetchone()
            log.info(data)
            return data
        except Exception as e:
            log.error(e)
            return None

    def get_meters_report(self):
        self.c.execute('''SELECT SUM(meters),strftime('%m', date_) FROM run_data 
                        GROUP BY strftime('%m', date_)''')
        try:
            data = self.c.fetchall()
            log.info(data)
            return data
        except Exception as e:
            log.error(e)
            return None

    def get_duration_report(self):
        self.c.execute('''SELECT SUM(duration),strftime('%m', date_) FROM run_data WHERE strftime('%m', date_)  
                        GROUP BY strftime('%m', date_)''')
        try:
            data = self.c.fetchall()
            log.info(data)
            return data
        except Exception as e:
            log.error(e)
            return None

    def get_duration1_report(self):
        self.c.execute('''SELECT SUM(duration),strftime('%m', date_) FROM stop_data WHERE strftime('%m', date_) 
                           GROUP BY stop_id''')
        try:
            data = self.c.fetchall()
            log.info(data)
            return data
        except Exception as e:
            log.error(e)
            return None

    def get_MTTR_report(self, today):
        self.c.execute('''SELECT SUM(duration)/(COUNT(duration)*60) as MTTR FROM stop_data
                        WHERE date_=? AND NOT stop_id=3 AND NOT stop_id=100  GROUP BY date_ LIMIT 1''', (today,))
        try:
            data = self.c.fetchone()[0]
            log.info(data)
            return data
        except Exception as e:
            log.error(e)
            return 0

    def get_MTBF_report(self, today):
        self.c.execute('''SELECT SUM(duration) as run_min FROM run_data
                        WHERE date_=?  GROUP BY date_ LIMIT 1''', (today,))
        try:
            run_min = self.c.fetchone()[0]
        except Exception as e:
            run_min = 0
            log.error(e)

        self.c.execute('''SELECT (COUNT(duration)*60) as stop_count FROM stop_data
                                WHERE date_=? AND NOT stop_id=3 AND NOT stop_id=100 GROUP BY date_ LIMIT 1''', (today,))
        try:
            stop_count = self.c.fetchone()[0]
        except Exception as e:
            stop_count = 0
            log.error(e)
        try:
            MTBF = round(run_min / stop_count, 2)
        except ArithmeticError:
            MTBF = 'inf'
        return MTBF

    def get_utilization_report(self, prevD):
        # print(prevD)
        self.c.execute('''SELECT ((SUM(duration)*100)/(1440*60)) FROM run_data
                        WHERE date_=? AND NOT shift = '0' GROUP BY date_ LIMIT 1''', (prevD,))
        try:
            utilization = self.c.fetchone()[0]
            # log.info(utilization)
        except Exception as e:
            utilization = 0
            log.error(e)

        self.c.execute('''SELECT SUM(meters)  FROM run_data
                         WHERE date_=? AND NOT shift = '0' GROUP BY date_ LIMIT 1''', (prevD,))
        try:
            production = self.c.fetchone()[0]
            # log.info(production)
        except Exception as e:
            production = 0
            log.error(e)

        self.c.execute('''SELECT po_id  FROM run_data
                        WHERE date_=? AND NOT shift = '0'   ''', (prevD,))
        try:
            po_run = self.c.fetchall()
            if po_run:
                try:
                    po_run = [po_data[0] for po_data in po_run]
                except:
                    po_run = []
            # log.info(po_run)
        except Exception as e:
            po_run = []
            log.error(e)

        # self.c.execute('''SELECT po_id  FROM stop_data
        #                 WHERE date_=?  ''', (prevD,))
        # try:
        #     po_stop = self.c.fetchall()
        #     if po_stop:
        #         try:
        #             po_stop = [po_data[0] for po_data in po_stop]
        #         except:
        #             po_stop = []
        # except Exception as e:
        #     po_stop = []
        #     log.error(e)

        # try:
        #
        #     total_po_num = po_run + po_stop
        #     lot_number = len(total_po_num)
        #
        #     # log.info(lot_number)
        #
        # except ArithmeticError:
        #     lot_number = 0
        try:
            lot_number = len(po_run)
        except Exception as e:
            lot_number = 0

        self.c.execute('''SELECT SUM(duration)/60  FROM stop_data
                        WHERE date_=? AND stop_id=14 AND NOT shift = '0' GROUP BY date_ LIMIT 1''', (prevD,))
        try:
            no_program = self.c.fetchone()[0]
            if no_program is None:
                no_program = 0
            # log.info(no_program)
        except Exception as e:
            no_program = 0
            log.error(e)

        self.c.execute('''SELECT SUM(duration)/60  FROM stop_data
                            WHERE date_=? AND NOT shift = '0' AND stop_id in (4,5,6) 
                            GROUP BY date_ LIMIT 1''', (prevD,))
        try:
            breakdown = self.c.fetchone()[0]
            # log.info(breakdown)
            # Electrical Breakdown
            # Mechanical Breakdown
            # Utility Breakdown
            # Corrective Maintenance
            # Low Air Pressure
            # No Water
            # Chemical issue
            # Pin Bar change
            # Nip Test
            # Quality fail due to mc
            # Lead cloth burst
            # Teflon finish cleaning
            # tank not ok
        except Exception as e:
            breakdown = 0
            log.error(e)

        self.c.execute('''SELECT SUM( (energy_stop - energy_start))
                                       FROM run_data
                                        WHERE date_=? AND NOT shift = '0'
                                        AND (energy_stop - energy_start) > 0
                                        GROUP BY date_ LIMIT 1 ''', (prevD,))
        try:
            energy_run = self.c.fetchone()[0]
            # for i in fluid_run:
            #     if i < 0:
            #         i = 0
            #     else:
            #         i = i

            # log.info(fluid_run )
        except Exception as e:
            energy_run = 0
            log.error(e)

        self.c.execute('''SELECT SUM( (energy_stop - energy_start)) FROM stop_data
                                         WHERE date_=? AND NOT shift = '0'
                                        AND (energy_stop - energy_start) > 0
                                        GROUP BY date_ LIMIT 1''', (prevD,))
        try:
            energy_stop = self.c.fetchone()[0]
            # for i in fluid_stop:
            #     if i < 0:
            #         i = 0
            #     else:
            #         i = i
            # log.info(fluid_stop )
        except Exception as e:
            energy_stop = 0
            log.error(e)

        try:
            total_energy = energy_run + energy_stop

            print(total_energy)

        except ArithmeticError:
            total_energy = 0

        self.c.execute('''SELECT SUM( (fluid_total_stop - fluid_total_start))
                                        FROM run_data
                                         WHERE date_=? AND NOT shift = '0'
                                         AND (fluid_total_stop - fluid_total_start) > 0
                                          GROUP BY date_ LIMIT 1 ''', (prevD,))
        try:
            fluid_run = self.c.fetchone()[0]
            # for i in fluid_run:
            #     if i < 0:
            #         i = 0
            #     else:
            #         i = i

            # log.info(fluid_run )
        except Exception as e:
            fluid_run = 0
            log.error(e)

        self.c.execute('''SELECT SUM( (fluid_total_stop - fluid_total_start)) FROM stop_data
                                      WHERE date_=? AND NOT shift = '0'
                                       AND (fluid_total_stop - fluid_total_start) > 0
                                       GROUP BY date_ LIMIT 1''', (prevD,))
        try:
            fluid_stop = self.c.fetchone()[0]
            # for i in fluid_stop:
            #     if i < 0:
            #         i = 0
            #     else:
            #         i = i
            # log.info(fluid_stop )
        except Exception as e:
            fluid_stop = 0
            log.error(e)

        try:
            total_fluid = fluid_run + fluid_stop

            print(total_fluid)

        except ArithmeticError:
            total_fluid = 0

        self.c.execute('''SELECT SUM(air_total) FROM run_data
                                         WHERE date_=? AND NOT shift = '0' GROUP BY date_ LIMIT 1 ''', (prevD,))
        try:
            air_run = self.c.fetchone()[0]
            # log.info( air_run)
        except Exception as e:
            air_run = 0
            log.error(e)

        self.c.execute('''SELECT SUM(air_total) FROM stop_data
                                  WHERE date_=? AND NOT shift = '0' GROUP BY date_ LIMIT 1''', (prevD,))
        try:
            air_stop = self.c.fetchone()[0]
            # log.info(air_stop )
        except Exception as e:
            air_stop = 0
            log.error(e)

        try:
            total_air = air_run + air_stop
            print(total_air)

        except ArithmeticError:
            total_air = 0
        log.info(f"{utilization}, {production}, {lot_number}, {no_program}, {breakdown},{total_energy},"
                 f" {total_fluid}, {total_air}")
        return utilization, production, lot_number, no_program, breakdown, total_energy, total_fluid, total_air

    def get_now_production(self, date_, shift):
        self.c.execute('''SELECT po_id FROM run_data WHERE date_=? AND shift=?
                          AND NOT po_id is NULL GROUP BY po_id ORDER BY stop_time''',
                       (date_, shift))
        try:
            data1 = self.c.fetchall()
            po_data = [i[0] for i in data1]
            log.info(po_data)
            report_data = []
            for po_id in po_data:
                # datapoint = list(datapoint)
                self.c.execute('''SELECT date_, strftime('%H:%M:%S',max(stop_time)), po_id,?,
                            operation_id, run_id, shift, SUM(meters), operator_id,
                            (SUM(meters) / (SUM(duration)/60)) as speed, SUM(duration)/60,
                            SUM(energy_stop - energy_start) as energy,'0',
                            SUM((fluid_total_stop - fluid_total_start)) as heat, SUM(water_total),
                            SUM(air_total) FROM run_data WHERE date_=? AND shift=? AND po_id=? 
                            GROUP BY run_id and po_id  ORDER BY stop_time''', (machine_code, date_, shift, po_id))

                try:
                    data = self.c.fetchall()
                    # log.info(data)
                    for datapoint in data:
                        datapoint = list(datapoint)
                        if datapoint[6] == 'A':
                            datapoint[6] = 1
                        elif datapoint[6] == 'B':
                            datapoint[6] = 2
                        elif datapoint[6] == 'C':
                            datapoint[6] = 3

                        if datapoint[1] == None:
                            datapoint[1] = datetime.now().strftime('%H:%M:%S')
                            print(datapoint)

                        self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (datapoint[2],))
                        try:
                            po_number = self.c.fetchone()[0].replace("\x00", "")
                        except:
                            po_number = 'No PO Number'
                        # log.info(po_number)
                        datapoint[2] = po_number
                        self.c.execute('''SELECT name FROM operation_list WHERE operation_id = ? LIMIT 1''',
                                       (datapoint[4],))
                        try:
                            operation_name = self.c.fetchone()[0]
                        except:
                            operation_name = 'NA'
                        # log.info(operation_name)
                        datapoint[4] = operation_name
                        self.c.execute('''SELECT code FROM run_category WHERE run_id = ? LIMIT 1''', (datapoint[5],))
                        try:
                            run_code = self.c.fetchone()[0]
                        except:
                            run_code = '0'
                            # log.info(stop_code)
                        datapoint[5] = run_code
                        # self.c.execute('''SELECT name FROM operator_list WHERE operator_id = ? LIMIT 1''',
                        #                (datapoint[8],))
                        # try:
                        #     operator_name = self.c.fetchone()[0]
                        # except:
                        #     operator_name = 'No Operator'
                        #     # log.info(operator_name)
                        # datapoint[8] = operator_name
                        report_data.append(datapoint)
                except Exception as e:
                    # log.error(e)
                    return None

            return report_data
        except Exception as e:
            log.error(e)
            return None

    def get_now_stoppage(self, date_, shift):
        self.c.execute('''SELECT po_id FROM stop_data WHERE date_=? AND shift=?
                              AND NOT po_id is NULL GROUP BY po_id ORDER BY stop_time''',
                       (date_, shift))
        try:
            data1 = self.c.fetchall()
            po_data = [i[0] for i in data1]
            log.info(po_data)
            report_data = []
            for po_id in po_data:
                # datapoint = list(datapoint)
                self.c.execute('''SELECT date_, strftime('%H:%M:%S',max(stop_time)), po_id, ?,
                             operation_id, shift, stop_id, SUM(duration)/60, operator_id, 
                             SUM(energy_stop - energy_start) as energy,'0',
                             SUM( (fluid_total_stop - fluid_total_start)) as heat, SUM(water_total),
                             SUM(air_total) FROM stop_data WHERE date_=? AND shift=? AND po_id=? 
                             GROUP BY stop_id  
                             ORDER BY stop_time''', (machine_code, date_, shift, po_id))
                try:
                    data = self.c.fetchall()
                    # log.info(data)
                    for datapoint in data:
                        datapoint = list(datapoint)
                        if datapoint[5] == 'A':
                            datapoint[5] = 1
                        elif datapoint[5] == 'B':
                            datapoint[5] = 2
                        elif datapoint[5] == 'C':
                            datapoint[5] = 3

                        if datapoint[1] == None:
                            datapoint[1] = datetime.now().strftime('%H:%M:%S')
                            print(datapoint)

                        self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (datapoint[2],))
                        try:
                            po_number = self.c.fetchone()[0].replace("\x00", "")
                        except Exception as e:
                            print(e)
                            po_number = 'No PO Number'
                        # log.info(po_number)
                        datapoint[2] = po_number
                        self.c.execute('''SELECT name FROM operation_list WHERE operation_id = ? LIMIT 1''',
                                       (datapoint[4],))
                        try:
                            operation_name = self.c.fetchone()[0]
                        except:
                            operation_name = 'NA'
                        # log.info(operation_name)
                        datapoint[4] = operation_name
                        self.c.execute('''SELECT code FROM stop_category WHERE stop_id = ? LIMIT 1''', (datapoint[6],))
                        try:
                            stop_code = self.c.fetchone()[0]
                        except:
                            stop_code = '0'
                            # log.info(stop_code)
                        datapoint[6] = stop_code
                        # self.c.execute('''SELECT name FROM operator_list WHERE operator_id = ? LIMIT 1''',
                        #                (datapoint[8],))
                        # try:
                        #     operator_name = self.c.fetchone()[0]
                        # except:
                        #     operator_name = 'No Operator'
                        #     # log.info(operator_name)
                        # datapoint[8] = operator_name
                        report_data.append(datapoint)
                except Exception as e:
                    # log.error(e)
                    return None

            return report_data
        except Exception as e:
            log.error(e)
            return None

    def get_po_with_x(self):
        self.c.execute('''SELECT po_id, po_number FROM po_data WHERE instr(po_number, ?) > 0''', ['\x00'])
        try:
            po_number = self.c.fetchall()
        except:
            po_number = 'No PO Number'
        return po_number

    def get_dates_shift_with_x(self, po_list):
        po_ids = [str(po[0]) for po in po_list]
        po_str = "(" + ",".join(po_ids) + ")"
        self.c.execute(f'''SELECT date_, shift FROM run_data WHERE po_id in {po_str}''')  # , [po_str])
        try:
            po_number = self.c.fetchall()
            my_set = set(po_number)
            # convert the set back to a list
            my_list_without_duplicates = list(my_set)
        except:
            my_list_without_duplicates = None
        return my_list_without_duplicates

    def get_po_prod_summary(self, date):
        try:
            self.c.execute('''SELECT po_id,                                    
                                     SUM(meters) AS meters,
                                     SUM(duration) AS duration,
                                     SUM(fluid_total_stop - fluid_total_start) AS fluid_total,
                                     SUM(energy_stop - energy_start) AS energy
                              FROM run_data
                              WHERE date_ = ? AND NOT shift = '0' 
                              GROUP BY po_id
                              ORDER BY po_id''', (date,))
            data = self.c.fetchall()
            log.info(data)
            report_data = []
            for datapoint in data:
                datapoint = list(datapoint)
                self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (datapoint[0],))
                try:
                    po_number = self.c.fetchone()[0]
                except:
                    po_number = ''
                datapoint[0] = po_number
                report_data.append(datapoint)
            return report_data
        except Exception as e:
            log.error(e)
            return None

    def get_co_downtime_summary(self, date):
        try:
            self.c.execute('''SELECT po_id,
                                     SUM(duration) AS duration,                                     
                                     SUM(fluid_total_stop - fluid_total_start) AS fluid_total,
                                     SUM(energy_stop - energy_start) AS energy
                              FROM stop_data
                              WHERE date_ = ? AND NOT shift = '0'
                              GROUP BY po_id
                              ORDER BY po_id''', (date,))
            data = self.c.fetchall()
            log.info(data)

            report_data = []
            for datapoint in data:
                datapoint = list(datapoint)
                self.c.execute('''SELECT po_number FROM po_data WHERE po_id = ? LIMIT 1''', (datapoint[0],))
                try:
                    po_number = self.c.fetchone()[0]
                except:
                    po_number = ''
                datapoint[0] = po_number
                report_data.append(datapoint)
            return report_data
        except Exception as e:
            log.error(e)
            return None

    # FROM & JOINs determine & filter rows
    # WHERE more filters on the rows
    # GROUP BY combines those rows into groups
    # HAVING filters groups
    # ORDER BY arranges the remaining rows/groups
    # LIMIT filters on the remaining rows/groups
