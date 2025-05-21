import sqlite3
import datetime

class Databaserecord:
    def __init__(self, gyv_kodas, tipas, tipo_santrumpa, pavadinimas_k, pavadinimas):
            self.gyv_kodas  = gyv_kodas
            self.tipas = tipas
            self.tipo_santrumpa = tipo_santrumpa
            self.pavadinimas_k  = pavadinimas_k
            self.pavadinimas = pavadinimas
            

class Databaseentity:
    def __init__(self):
        self.conn = sqlite3.connect('placenames.db')
        self.cursor = self.conn.cursor()
        self.cursor.execute("""
               CREATE TABLE IF NOT EXISTS fullrecord(
               fullrecord_id INTEGER PRIMARY KEY,
               gyv_kodas INT,
               tipas VARCHAR(20),
               tipo_santrumpa VARCHAR(20),
               pavadinimas_k VARCHAR(20),                          
               pavadinimas VARCHAR(20))""")
        self.conn.commit()


        