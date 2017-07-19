import sqlite3
import DataStructures

def createdb(db):
    c = db.cursor()
    tables = ["sites", "inversions", "site_offset_join", "faults"]
    for table in tables:
        c.execute("DROP TABLE IF EXISTS {}".format(table))
    db.commit()

def populate_sites(db, sites_file):
    c = db.cursor()
    c.execute('''CREATE TABLE sites
                  (site_name TEXT PRIMARY KEY, lat REAL, lon REAL, ele REAL)''')
    with open(sites_file, "r") as f:
        sites = f.readlines()
        for site in sites:
            try:
                name, lat, lon, ele = site.split()
                c.execute('''INSERT INTO sites(name, lat, lon, ele) 
                              VALUES ("{}", {}, {}, {})'''.format(name, lat, lon, ele))
            except:
                pass
    db.commit()
    #for row in c.execute('SELECT * FROM sites'):
    #    print (row)

def populate_inversions(db):
    c = db.cursor()
    c.execute('''CREATE TABLE inversions
                      (id INT PRIMARY KEY,
                      model TEXT UNIQUE,
                      label TEXT,
                      tag TEXT,
                      min_offset REAL,
                      convergence REAL,
                      eq_pause INT,
                      eq_thresh REAL,
                      strike_slip INT,
                      wait INT,
                      max_offset REAL,
                      min_r REAL,
                      fault_len INT,
                      fault_wid INT)''')

    c.execute('''CREATE TABLE site_offset_join
                    (id INTEGER PRIMARY KEY,
                    offset REAL,
                    site_name TEXT,
                    inversion_id INT,
                    FOREIGN KEY(site_name) REFERENCES sites(site_name),
                    FOREIGN KEY(inversion_id) REFERENCES inversions(id))''')

    c.execute('''CREATE TABLE faults
                (id INT PRIMARY KEY,
                sequence_order INT,
                inversion_id INT,
                lat REAL,
                lon REAL,
                depth REAL,
                strike REAL,
                dip REAL,
                rake REAL,
                fault_length REAL,
                fault_width REAL,
                unknown REAL,
                FOREIGN KEY(inversion_id) REFERENCES inversions(id))''')

    db.commit()
    for run in DataStructures.inversion_runs:
        with open(run['faults_file']) as file:
            fault_data = [x.split() for x in file.readlines() if x[0] != '#']

        faults = {
            'length': float(fault_data[0][0]),
            'width': float(fault_data[0][1]),
            'subfault_list': [[float(x) for x in fault_data_line] for fault_data_line in fault_data[1:]]
        }


        c.execute('''INSERT INTO inversions(model, label, tag, min_offset, convergence, eq_pause,
                      eq_thresh, strike_slip, wait, max_offset, min_r, fault_len, fault_wid)
                      VALUES("{}","{}","{}",{},{},{},{},{},{},{},{},{},{}).'''.format(
                      run['model', run['label'], run['tag'], run['minimum_offset'],
                      run['convergence'], run['eq_pause'], run['eq_threshold'], 
                      1 if run['strike_slip'] else 0, run['mes_wait'], run['max_offset'],
                      run['min_r'], faults['length'], faults['width']]))
        db.commit()

        inv_db_rec = c.execute('SELECT id FROM inversions WHEWRE model="{}"'.format(run['model']))
        print ("...")
        print (inv_db_rec)
        pass

db = sqlite3.connect('LiveFilter.db')
createdb(db)
populate_sites(db, "site_lat_lon_ele.txt")
populate_inversions(db)
db.close()