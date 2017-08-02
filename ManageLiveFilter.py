import curses
import requests
import time

host = '127.0.0.1'
port = '5002'

statusurl = '/status'

def get_status_api():
    status = requests.get("http://"+host+":"+port+statusurl)
    return status.json()

def toggle_state_api(inversion):
    change_to = not inversion['active']
    requests.put("http://"+host+":"+port+statusurl, json={
        'inversion': inversion['id'],
        'active': change_to
    })

def settings_screen(stdscr):
    pass

def edit_inversion(stdscr, inversion = None):
    edit = True if inversion is not None else False
    display = [('model', 'Inversion Model Name', 't'),
               ('label', 'Inversion Label', 't'),
               ('tag', 'Inversion Tag', 't'),
               ('minimum_offset', 'Minimum Offset', 'f'),
               ('convergence', 'Convergence', 'i'),
               ('eq_pause', 'EQ Pause', 'i'),
               ('eq_threshold', 'EQ Threshold', 'f'),
               ('mes_wait', 'Wait', 'i'),
               ('max_offset', 'Max Offset', 'f'),
               ('min_r', 'Minimum r Value', 'f'),
               ('faults', 'Faults Filename', 't')]
    if inversion is None:
        inversion = {
            'model': 'Model',
            'label': 'Label',
            'tag': 'Tag',
            'minimum_offset': '0.001',
            'convergence': '0',
            'eq_pause': '60',
            'eq_threshold': '1',
            'mes_wait': '2',
            'max_offset': '200',
            'min_r': '0.001',
            'faults': ''}
    else:
        for d in display:
            if d[3] in ('f', 'i'):
                inversion[d[0]] = str(inversion[d[0]])
    stdscr.nodelay(1)
    selected = 0
    while True:
        stdscr.addstr(0,0,"Edit Inversion" if edit else "Create New Inversion")
        for idx, entry in enumerate(display):
            stdscr(idx + 2, 0, entry[1])
            stdscr(idx + 2, 30, str(inversion[entry[0]]),
                   curses.A_STANDOUT if selected == idx else curses.A_NORMAL)
        stdscr(len(display) + 3, 0, "Save")
        stdscr(len(display) + 5, 0, "Cancel")
        input = stdscr.getch()
        if input == curses.KEY_UP:
            selected -= 1
            if selected < 0:
                selected = len(display) - 1
        elif input == curses.KEY_ENTER or input == 10 or input == 13:
            if selected > len(display) - 1:
                pass
            else:
                selected += 1
                if selected > len(display) - 1:
                    selected = 0
        elif input == curses.KEY_DOWN:
                selected += 1
                if selected > len(display) - 1:
                    selected = 0
        if input in range(ord('0'), ord('9')):
            if display[selected][2] in ('f', 'i'):
                inversion[selected[0]] *= 10
                inversion[selected[0]] += input - ord('0')
            elif display[selected[2]] == 't':
                inversion[selected[0]] += input




def status_screen(stdscr):
    selected = 0
    editcol = False
    lastpoll = None
    change = True
    stdscr.nodelay(1)
    while True:
        if lastpoll is None or (time.time() - lastpoll) > 1:
            status = get_status_api()
            lastpoll = time.time()
            change = True

        if change:
            stdscr.clear()
            stdscr.addstr(0, 0, "Current Inversion Status:")
            stdscr.addstr(1, 0, "Current Timestamp: {}".format(status['current_timestamp']))
            stdscr.addstr(2, 0, "Newest  Timestamp: {}".format(status['newest_timestamp']))
            stdscr.addstr(4, 0, "Currently Configured Inversions:")
            stdscr.addstr(5, 0, "Model")
            stdscr.addstr(5, 20, "Running")
            stdscr.addstr(5, 30, "Start/Stop")
            for idx, content in enumerate(status['inversions']):
                stdscr.addstr(idx + 6, 0, content['model'])
                stdscr.addstr(idx + 6, 20, "Yes" if content['active'] else "No")
                stdscr.addstr(idx + 6, 30, "Stop" if content['active'] else "Start",
                              curses.A_STANDOUT if selected == idx and not editcol else curses.A_NORMAL)
                stdscr.addstr(idx + 6, 45, "Edit",
                              curses.A_STANDOUT if selected == idx and editcol else curses.A_NORMAL)
            stdscr.addstr(len(status['inversions']) + 7, 0, "Edit General Settings",
                              curses.A_STANDOUT if selected == len(status['inversions']) else curses.A_NORMAL)
            stdscr.addstr(len(status['inversions']) + 8, 0, "Add New Inversion",
                              curses.A_STANDOUT if selected == len(status['inversions']) + 1 else curses.A_NORMAL)
            stdscr.addstr(len(status['inversions']) + 9, 0, "Exit",
                              curses.A_STANDOUT if selected == len(status['inversions']) + 2 else curses.A_NORMAL)
            stdscr.refresh()

        input = stdscr.getch()
        if input == curses.KEY_UP:
            change = True
            selected -= 1
            if selected < 0:
                selected = len(status['inversions']) + 2
        elif input == curses.KEY_DOWN:
            change = True
            selected += 1
            if selected > len(status['inversions']) + 2:
                selected = 0
        elif input == curses.KEY_RIGHT or input == curses.KEY_LEFT:
            editcol = not editcol
        elif input == curses.KEY_ENTER or input == 10 or input == 13:
            change = True
            if selected < len(status['inversions']) and not editcol:
                toggle_state_api(status['inversions'][selected])
            elif selected == len(status['inversions']) + 2:
                return
            else:
                pass

        time.sleep(.1)

def main(stdscr):
    status_screen(stdscr)

curses.wrapper(main)