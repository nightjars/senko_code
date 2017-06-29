import logging
import DataStructures


def load_data_from_text_files(sites_data_file, faults_data_file):
    sites_dict = {}

    with open (sites_data_file) as file:
        sites = [x.split() for x in file.readlines() if x[0] != '#']

    for site in sites:
        station = {
            'name': site[0],
            'lat': float(site[1]),
            'lon': float(site[2]),
            'height': float(site[3]),
            'offset': float(site[4]),
            'index': len(sites_dict)                            # store index for array sequencing
        }

        if True or station['offset'] >= DataStructures.configuration['minimum_offset']:
            if station['name'] not in sites_dict:
                sites_dict[station['name']] = station

    # Allow appending new stations once programming is running without having to iterate through
    # full list of sites by storing last-used-index
    # sites_dict['max_index'] = len(sites)

    with open(faults_data_file) as file:
        fault_data = [x.split() for x in file.readlines() if x[0] != '#']

    try:
        faults = {
            'length': float(fault_data[0][0]),
            'width': float(fault_data[0][1]),
            'subfault_list': [[float(x) for x in fault_data_line] for fault_data_line in fault_data[1:]]
        }
    except Exception as e:
        logging.critical("Malformed faults file, unable to read {}".format(faults_data_file))
        raise e

    return {'sites': sites_dict, 'faults': faults}


