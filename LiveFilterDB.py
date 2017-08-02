import sqlite3
import Config
import dist_filt
import traceback
import time

inversion_runs = [
    {
        'sites_file': './SA_offset.d',
        'faults_file': './SA_faults.d',
        'sites': None,
        'faults': None,
        'filters': None,
        'model': 'SanAndreas-20x4',
        'label': 'Refactor Version',
        'tag': 'current',
        'minimum_offset': 0.001,  # inverter config/validator/readonceconfig
        'convergence': 320.,  # read once config
        'eq_pause': 10.,
        'eq_threshold': 0.01,
        'strike_slip': False,
        'mes_wait': 2,
        'max_offset': 4000,
        'offset': False,
        'min_r': 0.0001,
        'float_equality': 1e-9,
        'inverter_configuration': {
            'strike_slip': None,
            'short_smoothing': True,
            'smoothing': True,
            'corner_fix': False,
            'offsets_per_site': 3,
            'subfault_len': 60.,
            'subfault_wid': 30.,
            'offset': None,
            'sub_inputs': None,
            'smooth_mat': None,
            'mask': None
        }
    },
    {
        'sites_file': './CAS_offset.d',
        'faults_file': './CAS_faults.d',
        'sites': None,
        'faults': None,
        'filters': None,
        'model': 'Cascadia-20x10',
        'label': 'Refactor Version',
        'tag': 'current',
        'minimum_offset': 0.001,  # inverter config/validator/readonceconfig
        'convergence': 45.,  # read once config
        'eq_pause': 120.,
        'eq_threshold': 1.,
        'strike_slip': False,
        'mes_wait': 2,
        'max_offset': 100,
        'offset': False,
        'min_r': 0.0001,
        'float_equality': 1e-9,
        'inverter_configuration': {
            'strike_slip': None,
            'short_smoothing': True,
            'smoothing': True,
            'corner_fix': False,
            'offsets_per_site': 3,
            'subfault_len': 60.,
            'subfault_wid': 30.,
            'offset': None,
            'sub_inputs': None,
            'smooth_mat': None,
            'mask': None
        }
    },
    {
        'sites_file': './MAT_offset.d',
        'faults_file': './MAT_faults.d',
        'sites': None,
        'faults': None,
        'filters': None,
        'model': 'MAT-57x4',
        'label': 'Refactor Version',
        'tag': 'current',
        'minimum_offset': 0.001,  # inverter config/validator/readonceconfig
        'convergence': 320.,  # read once config
        'eq_pause': 120.,
        'eq_threshold': 1.,
        'strike_slip': False,
        'mes_wait': 2,
        'max_offset': 100,
        'offset': False,
        'min_r': 0.0001,
        'float_equality': 1e-9,
        'inverter_configuration': {
            'strike_slip': None,
            'short_smoothing': True,
            'smoothing': True,
            'corner_fix': False,
            'offsets_per_site': 3,
            'subfault_len': 60.,
            'subfault_wid': 30.,
            'offset': None,
            'sub_inputs': None,
            'smooth_mat': None,
            'mask': None
        }
    },
    {
        'sites_file': './NAZ_offset.d',
        'faults_file': './NAZ_faults.d',
        'sites': None,
        'faults': None,
        'filters': None,
        'model': 'Nazca-40x4',
        'label': 'Refactor Version',
        'tag': 'current',
        'minimum_offset': 0.001,  # inverter config/validator/readonceconfig
        'convergence': 320.,  # read once config
        'eq_pause': 120.,
        'eq_threshold': .1,
        'strike_slip': False,
        'mes_wait': 2,
        'max_offset': 100,
        'offset': False,
        'min_r': 0.0001,
        'float_equality': 1e-9,
        'inverter_configuration': {
            'strike_slip': None,
            'short_smoothing': True,
            'smoothing': True,
            'corner_fix': False,
            'offsets_per_site': 3,
            'subfault_len': 60.,
            'subfault_wid': 30.,
            'offset': None,
            'sub_inputs': None,
            'smooth_mat': None,
            'mask': None
        }
    }
]

def createdb(db):
    c = db.cursor()
    tables = ["sites", "inversions", "site_offset_join", "faults"]
    for table in tables:
        c.execute("DROP TABLE IF EXISTS {}".format(table))
    db.commit()

def create_work_table(db):
    # types of work to be done:
    # process faults for an inversion
    # add new site to existing inversions

    c = db.cursor
    c.execute("DROP TABLE IF EXISTS pending_work")
    c.execute('''CREATE TABLE pending_work
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 site TEXT,
                 inversion_id INTEGER,                 
                 FOREIGN KEY(inversion_id) REFERENCES inversions(id))''')




def create_users_table(db, default_user='admin', default_password='admin'):
    c = db.cursor
    c.execute("DROP TABLE IF EXISTS users")
    c.execute('''CREATE TABLE users
              (user_name TEXT PRIMARY KEY, password TEXT)''')
    c.execute('''INSERT INTO users(user_name, password) VALUES ("{}", "{}")'''.format(
                default_user, default_password))
    db.commit()

def replace_fault_data(db, inversion_id, faults):
    c = db.cursor()
    Config.remove_inversion_run(inversion_id)
    c.execute('''DELETE FROM faults WHERE inversion_id = {}'''.format(int(inversion_id)))
    c.execute('''DELETE FROM site_offset_join WHERE inversion_id = {}'''.format(int(inversion_id)))
    c.execute('''UPDATE inversions fault_len = {}, fault_wid = {} WHERE inversion_id = {}'''.format(
        faults['length'], faults['width'], inversion_id))
    for idx, fault in enumerate(faults['subfault_list']):
        while len(fault) < 8:
            fault.append(0.0)
        c.execute('''INSERT INTO faults(sequence_order, inversion_id, lat, lon, depth,
                     strike, dip, fault_length, fault_width, unknown_field)
                     VALUES({},{},{},{},{},{},{},{},{},{})'''.format(
            idx, inversion_id, *fault))
    c.execute('''INSERT INTO pending_work(inversion_id) VALUES({})'''.format(inversion_id))
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
                c.execute('''INSERT INTO sites(site_name, lat, lon, ele) 
                              VALUES ("{}", {}, {}, {})'''.format(name, lat, lon, ele))
            except Exception as e:
                print("Error with sites file: {}".format(site))
                print("Error: ".format(traceback.format_exception(e)))
    db.commit()
    #for row in c.execute('SELECT * FROM sites'):
    #    print (row)

def populate_inversions(db):
    c = db.cursor()
    c.execute('''CREATE TABLE inversions
                      (id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    offset REAL,
                    site_name TEXT,
                    inversion_id INT,
                    FOREIGN KEY(site_name) REFERENCES sites(site_name),
                    FOREIGN KEY(inversion_id) REFERENCES inversions(id))''')

    c.execute('''CREATE TABLE faults
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                sequence_order INT,
                inversion_id INT,
                lat REAL,
                lon REAL,
                depth REAL,
                strike REAL,
                dip REAL,                
                fault_length REAL,
                fault_width REAL,
                unknown_field REAL,
                FOREIGN KEY(inversion_id) REFERENCES inversions(id))''')

    db.commit()
    for run in inversion_runs:
        with open(run['faults_file']) as file:
            fault_data = [x.split() for x in file.readlines() if x[0] != '#']

        faults = {
            'length': float(fault_data[0][0]),
            'width': float(fault_data[0][1]),
            'subfault_list': [[float(x) for x in fault_data_line] for fault_data_line in fault_data[1:]]
        }


        c.execute('''INSERT INTO inversions(model, label, tag, min_offset, convergence, eq_pause,
                      eq_thresh, strike_slip, wait, max_offset, min_r, fault_len, fault_wid)
                      VALUES("{}","{}","{}",{},{},{},{},{},{},{},{},{},{})'''.format(
                      run['model'], run['label'], run['tag'], run['minimum_offset'],
                      run['convergence'], run['eq_pause'], run['eq_threshold'], 
                      1 if run['strike_slip'] else 0, run['mes_wait'], run['max_offset'],
                      run['min_r'], int(faults['length']), int(faults['width'])))
        db.commit()
        inv_db_rec = c.execute('SELECT id FROM inversions WHERE model="{}"'.format(run['model'])).fetchone()[0]
        for idx, fault in enumerate(faults['subfault_list']):
            while len(fault) < 8:
                fault.append(0.0)
            c.execute('''INSERT INTO faults(sequence_order, inversion_id, lat, lon, depth,
                         strike, dip, fault_length, fault_width, unknown_field)
                         VALUES({},{},{},{},{},{},{},{},{},{})'''.format(
                         idx, inv_db_rec, *fault))
            db.commit()

def rebuild_faults_only(db):
    c = db.cursor()
    c.execute("DROP TABLE IF EXISTS faults")
    c.execute('''CREATE TABLE faults
                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sequence_order INT,
                    inversion_id INT,
                    lat REAL,
                    lon REAL,
                    depth REAL,
                    strike REAL,
                    dip REAL,                
                    fault_length REAL,
                    fault_width REAL,    
                    unknown_field REAL,
                    FOREIGN KEY(inversion_id) REFERENCES inversions(id))''')
    db.commit()
    for run in inversion_runs:
        with open(run['faults_file']) as file:
            fault_data = [x.split() for x in file.readlines() if x[0] != '#']

        faults = {
            'length': float(fault_data[0][0]),
            'width': float(fault_data[0][1]),
            'subfault_list': [[float(x) for x in fault_data_line] for fault_data_line in fault_data[1:]]
        }
        inv_db_rec = c.execute('SELECT id FROM inversions WHERE model="{}"'.format(run['model'])).fetchone()[0]
        for idx, fault in enumerate(faults['subfault_list']):
            while len(fault) < 8:
                fault.append(0.0)
            c.execute('''INSERT INTO faults(sequence_order, inversion_id, lat, lon, depth,
                         strike, dip, fault_length, fault_width, unknown_field)
                         VALUES({},{},{},{},{},{},{},{},{},{})'''.format(
                         idx, inv_db_rec, *fault))
    db.commit()

def populate_offsets(db, inversion_id = None):
    inv_cur = db.cursor()
    fault_cur = db.cursor()
    station_cur = db.cursor()
    join_cur = db.cursor()
    inversion_list = [inversion_id] if inversion_id is not None else \
                     inv_cur.execute('SELECT id FROM inversions').fetchall()
    for inversion in inv_cur.execute('SELECT id FROM inversions'):
        for station in station_cur.execute('SELECT site_name, lat, lon, ele FROM sites'):
            (site_name, site_lat, site_lon, site_ele) = station
            max_mag = 0
            for fault in fault_cur.execute('SELECT * FROM faults WHERE inversion_id = {}'.format(inversion[0])):
                (fault_id, fault_seq, inv_id, fault_lat, fault_lon, fault_depth, fault_strike, fault_dip,
                 fault_len, fault_wid) = fault
                slip = 1  # from example usage, not sure what this means
                rake = 180  # from example usage, not sure why real rake value isn't used
                mag = float(dist_filt.adp(fault_lat, fault_lon, fault_depth, fault_strike, fault_dip,
                                          rake, fault_len, fault_wid, slip, site_lat, site_lon))
                max_mag = max(mag, max_mag)
            print ("{} {}".format(site_name, max_mag))
            join_cur.execute('INSERT INTO site_offset_join(site_name, inversion_id, offset) VALUES("{}",{},{})'.format(
                site_name, inversion[0], max_mag))
            db.commit()

def get_sites(db, inversion_id, min_offset):
    cur = db.cursor()
    sites_dict = {}
    for site in cur.execute('SELECT sites.site_name, sites.lat, sites.lon, sites.ele, site_offset_join.offset ' \
                            'FROM sites ' \
                            'INNER JOIN site_offset_join ' \
                            'ON sites.site_name = site_offset_join.site_name ' \
                            'WHERE site_offset_join.inversion_id = {} ' \
                            'AND site_offset_join.offset >= {}'.format(inversion_id, min_offset)):
        sites_dict[site[0]] = {'name': site[0],
                           'lat': site[1],
                           'lon': site[2],
                           'height': site[3],
                           'offset': site[4],
                           'index': len(sites_dict)}
    return sites_dict

def get_faults(db, inversion_id):
    inv_cur = db.cursor()
    faults_cur = db.cursor()
    sizes = inv_cur.execute('SELECT fault_len, fault_wid FROM inversions WHERE id = {}'.format(inversion_id)).fetchone()
    faults_data = faults_cur.execute('SELECT lat, lon, depth, strike, dip, fault_length, fault_width, unknown_field ' \
                                     'FROM faults ' \
                                     'WHERE inversion_id = {} ' \
                                     'ORDER BY sequence_order'.format(inversion_id)).fetchall()
    if not len(faults_data):
        return None

    faults = {
        'length': float(sizes[0]),
        'width': float(sizes[1]),
        'subfault_list': [[float(x) for x in fault_data_line] for fault_data_line in faults_data]
    }
    return faults

def get_inversions(db, id=None, brief=False, runnable_only=False):
    inversions = []
    cur = db.cursor()
    query = 'SELECT id, model, label, tag, min_offset, convergence, eq_pause, ' \
            'eq_thresh, strike_slip, wait, max_offset, min_r, fault_len, fault_wid ' \
            'FROM inversions '
    if id is not None:
        query += 'WHERE id = {}'.format(id)
    for inversion in cur.execute(query):
        id, model, label, tag, min_offset, convergence, eq_pause, eq_thresh, \
                          strike_slip, wait, max_offset, min_r, fault_len, fault_wid = inversion
        inversions.append({
            'id': id,
            'sites': None if brief else get_sites(db, id, min_offset),
            'faults': None if brief else get_faults(db, id),
            'filters': None,
            'model': model,
            'label': label,
            'tag': tag,
            'minimum_offset': min_offset,
            'convergence': convergence,
            'eq_pause': eq_pause,
            'eq_threshold': eq_thresh,
            'strike_slip': False if strike_slip == 0 else True,
            'mes_wait': wait,
            'max_offset': max_offset,
            'offset': False,
            'min_r': min_r,
            'float_equality': 1e-9,
            'inverter_configuration': {
                'strike_slip': None,
                'short_smoothing': True,
                'smoothing': True,
                'corner_fix': False,
                'offsets_per_site': 3,
                'subfault_len': fault_len,
                'subfault_wid': fault_wid,
                'offset': None,
                'sub_inputs': None,
                'smooth_mat': None,
                'mask': None
            }
        })
    return inversions

def rebuild_database():
    db = sqlite3.connect('LiveFilter.db')
    createdb(db)
    populate_sites(db, "site_lat_lon_ele.txt")
    populate_inversions(db)
    populate_offsets(db)
    db.close()

def get_db():
    return sqlite3.connect('LiveFilter.db')

def work_table_watcher():
    db = get_db()
    c = db.cursor
    while True:
        c.execute('''SELECT (id, site, inversion_id) FROM pending_work''')
        work = c.fetchone()
        if work:
            if work[1]:
                pass
            if work[2]:
                populate_offsets(db, inversion_id=work[2])
        c.execute('''DELETE FROM pending_work WHERE id = {}'''.format(work[0]))
        db.commit()
        time.sleep(1)


#db = sqlite3.connect('LiveFilter.db')
#rebuild_faults_only(db)
#a = get_inversions(db)
#for b in a:
#    for c, d in b.items():
#        print ("{}:{}".format(c, d))