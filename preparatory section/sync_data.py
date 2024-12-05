import json
import sqlite3
import logging

# from api import create_run_data, create_stop_data, post_stop_data, post_run_data, getEmailList, send_po_data
log = logging.getLogger()


class SyncDBHelper:
    def __init__(self):
        self.conn = sqlite3.connect("vardhman.db")
        self.c = self.conn.cursor()
        self.c.execute("""CREATE TABLE IF NOT EXISTS Sync_Table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                payload TEXT NOT NULL)""")

    def add_sync_data(self, payload_url, payload):
        try:
            payload_json = json.dumps(payload)
            self.c.execute('''
                INSERT INTO Sync_Table (url,payload)
                VALUES (?,?)
                ''', (payload_url, payload_json,))
            self.conn.commit()
        except Exception as e:
            log.error(f"Error while adding data : {e}")

    def get_sync_data(self):
        try:
            self.c.execute('SELECT * FROM Sync_Table')
            rows = self.c.fetchall()
            return rows
        except Exception as e:
            log.error(f"Error while getting data: {e}")
            return []

    def delete_sync_data(self, payload_id):
        try:
            # deleting the payload where ts is less than or equal to ts
            self.c.execute("""DELETE FROM Sync_Table where id = ?""", (payload_id,))
            self.conn.commit()
            log.info(f"Successful, Deleted data from Sync_Table")
        except Exception as e:
            log.error(f'Error: while deleting sync data {e}')
