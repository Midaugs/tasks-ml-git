import sqlite3
import datetime

class Record:
    def __init__(self, place_name):
        self.place_name = place_name

 
class Paklausimas:
    def __init__(self):
        self.conn = sqlite3.connect('dbaze_re_placename.db')     
        self.cursor = self.conn.cursor()
        self.cursor.execute(""" CREATE TABLE IF NOT EXISTS record(
                  record_id INTEGER PRIMARY KEY,            
                  place_name VARCHAR(50) NOT NULL)""")
        self.conn.commit()

    def add_entered_request(self, place_name):
        userio_irasas = Record(place_name)
        self.cursor.execute("INSERT INTO record (place_name) VALUES (?)", (place_name,))
        self.conn.commit()
        return userio_irasas  
      