import Queue
import logging
import os
import re
import time
import traceback
# noinspection PyUnresolvedReferences
import _strptime
from datetime import datetime, timedelta
from threading import Thread, Event
import operator

import numpy as np
import pandas as pd
from lxml import html
from pandas import DataFrame
from tinydb import *  # This should probably be transitioned to sqlite3 at some point

os.remove('db.json')

db = TinyDB('db.json')

settings = {
            'directory': 'data',
            'input_filename': 'parvo.txt',
            'concurrency': None,  # This script often runs faster w/o concurrency due the database and proc requirements
            'log_level': 'DEBUG'
           }
###########################
# Setup Logging
###########################
if 'log_level' in settings:
    level = settings['log_level'].strip()
else:
    level = 'WARNING'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s, %(levelname)s: %(message)s',
                    filename='{0}.log'.format(time.strftime("%Y_%m_%d-%I_%M_%S")))
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.getLevelName(level))
# tell the handler to use this format
console.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s: %(message)s'))
# add the handler to the root logger
logging.getLogger('').addHandler(console)

if not ('input_filename' in settings and os.path.exists(settings['input_filename'])):
    logging.error('input_filename not found.')
    exit()
# Read the input file
f = open(settings['input_filename'], 'r')
lines = f.readlines()
f.close()
a_nums = []
try:
    a_nums = [str(line[1:].strip()) for line in lines]
except Exception as e:
    logging.error('There was an error parsing the input file. '
                  'Ensure it is a proper format (newline separated A# list).')
    logging.debug(e)
if len(a_nums) == 0:
    logging.error('No ANums provided. Exiting.')
    exit()

pd.set_option("display.max_columns", 999)
pd.set_option("display.max_colwidth", 999)
pd.set_option('expand_frame_repr', False)
a_num_regex = re.compile('A\d\d\d\d\d\d\d\d')


def get_array_from_table(table, remove_empties=False):
    frame = []
    rows = table.cssselect("tr")
    for row in rows:
        frame.append(list())
        for td in row.cssselect("td"):
            contents = unicode(td.text_content().strip())
            if not remove_empties or contents:
                frame[-1].append(contents)
    return frame


def validate_shape(table, expected_size):
    if expected_size is None:
        return True
    if np.array(table).shape != expected_size:
        logging.debug('Table has unexpected shape (expected {0}).'.format(expected_size))
        logging.debug('\n'+str(DataFrame(table)))
        return False
    return True


# noinspection PyBroadException
def parse_element(data, specification, debug_label=''):
    # Validate input specification shape
    if (not isinstance(specification, type([]))) or len(specification) != 4:
        logging.debug('Error in {1}. Parse specification {0} is either of incorrect type (expected []) '
                      'or length (expected 4). Returning empty string pair.'.format(specification, debug_label))
        return {'': ''}
    # Extract specification parts
    label = specification[0]
    location = specification[1]
    regex = specification[2]
    postprocess = specification[3]
    # Validate label
    if not isinstance(label, type('')):
        logging.debug('Error in {1}. The provided label for specification {0} is not a valid string. '
                      'Returning empty string pair.'.format(specification, debug_label))
        return {'': ''}
    label = label.strip()
    # Validate regex and compile if necessary
    if isinstance(regex, type('')):
        try:
            regex = re.compile(regex)
        except:
            logging.debug('Error in {1}. The provided regular expression for specification {0} is not valid. '
                          'Returning labelled empty string.'.format(specification, debug_label))
            return {label: ''}
    # Locate the data
    try:
        result = reduce(operator.getitem, location, data)
    except IndexError:
        logging.debug('Error in {1}. The provided location does not exist for {0}. '
                      'Returning empty string.'.format(specification, debug_label))
        return {label: ''}
    except:
        logging.debug('Error in {1}. There was an unknown problem during location grabbing for {0}. '
                      'Returning labelled empty string.'.format(specification, debug_label))
        return {label: ''}
    # Match the regex and strip final string result
    try:
        regex_match = regex.search(result)
    except:
        logging.debug('Error in {1}. There was a problem matching regex for {0}. '
                      'Returning labelled empty string.'.format(specification, debug_label))
        return {label: ''}
    if regex_match is None:
        logging.debug('Error in {1}. No match found for {0} (string=\'{2}\'). '
                      'Returning labelled empty string.'.format(specification, debug_label, result))
        return {label: ''}
    result = regex_match.group(0).strip()
    # Postprocess the string
    try:
        result = postprocess(result)
    except:
        logging.debug('Error in {1}. There was a problem post-processing for specification {0}. '
                      'Returning raw regex match (string=\'{2}\').'.format(specification, debug_label, result))
    # Strip and return
    return {label: result}


def process_anum(a_number):
    global db
    file_path = os.path.join(settings['directory'], 'A' + a_number + '.htm')
    if not os.path.exists(file_path):
        return
    with open(file_path, 'r') as file_ptr:
        result = file_ptr.read()
    tree = html.fromstring(result)
    tables = tree.findall('.//table')
    frames = [get_array_from_table(table) for table in tables]
    # Validate Shapes
    expected_shapes = {None, (2L,), (4L, 3L)}
    for frame, shape in zip(frames, expected_shapes):
        validate_shape(frame, shape)
    parser_table = [  # key, frame_loc, regex
                        # Table 1
                        ['anum', [1, 0, 0], "(?!:\s*)[Aa]\d{8}", lambda x: x],
                        ['print_date', [1, 1, 0], "(?!:\s*)\d.*M", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
                        # Table 2
                        ['confirmation_anum', [2, 0, 0], "(?!:\s*)[Aa]\d{8}", lambda x: x],
                        ['alt_id', [2, 1, 0], ".*", lambda x: x],
                        ['gender', [2, 2, 1], "([Mm]ale)|([Ff]emale)|([Uu]nknown)", lambda x: x.lower()],
                        ['age_range', [2, 3, 1], ".*", lambda x: x.replace('no longer in use', '').strip()],
                        ['name', [2, 0, 1], ".*", lambda x: x.strip()],
                        ['declawed', [2, 2, 2], "(?=Declawed:).*", lambda x: x.split(':')[1].strip()],
                        ['bite_history', [2, 3, 2], "(?=Bitten:).*", lambda x: x.split(':')[1].strip()],
                        ['physical_attributes', [2, 0, 2], ".*", lambda x: [y.strip() for y in x.split(',')]],
                        ['species', [2, 1, 1], "([Dd]og)|([Cc]at)|([Uu]nknown)", lambda x: x.lower()],
                        ['age', [2, 1, 2], "^.*(?=\s+\,\s+)", lambda x: x],
                        ['dob', [2, 1, 2], "\d+\/\d+\/\d+", lambda x: datetime.strptime(x, '%m/%d/%Y')],
                        ['spay_neuter', [2, 1, 2], "(?=Spayed/Neutered:).*", lambda x: x.split(':')[1].strip()],

                        # TODO: Continue table parsing... at least get the other important tables.
                   ]

    # Parse Elements
    result = {}
    for pt in parser_table:
        test = parse_element(frames, pt, debug_label=a_number)
        result.update(test)

    # Send to DB
    if 'concurrency' in settings and settings['concurrency'] is not None:
        try:
            database_queue.put(result)
        except ValueError:
            logging.debug('Could not send {0} to DB.'.format(result))
    else:
        db.insert(result)


def process_next_anum():
    global q
    while True:
        next_a = None
        # noinspection PyBroadException
        try:
            next_a = q.get(True, 0)
            process_anum(next_a)
        except Queue.Empty:
            logging.debug('Closing thread gracefully after empty queue.')
            exit()
        except Exception:
            if next_a is not None:
                logging.error('There was an error processing the anum, {0}.'.format(next_a))
            else:
                logging.error('There was an error retreiving the next a number.')
            logging.error(traceback.print_exc())
            pass


def database_writer(database, queue, stop_signal):
    while not stop_signal.is_set():
        iters = queue.qsize()
        for index in range(iters):
            element = queue.get()
            database.insert(element)
    logging.info('Database writer received signal to end.')
    iters = queue.qsize()
    for index in range(iters):
        if index % 100 == 0:
            logging.info('{0} records left to write.'.format(iters-index))
        element = queue.get()
        database.insert(element)
    logging.info('Database writer exiting...')
    exit()

###########################
# Begin Parse
###########################
# Begin time logging
start_time = time.time()

logging.info('Initializing...')

if 'concurrency' in settings and settings['concurrency'] is not None:
    logging.info('Setting up concurrency with {0} thread(s).'.format(settings['concurrency']))
    database_writer_stop_signal = Event()
    database_queue = Queue.Queue()
    db_thread = Thread(target=database_writer, args=(db, database_queue, database_writer_stop_signal))
    db_thread.start()
    # Construct Queue
    q = Queue.Queue(len(a_nums))
    for anum in a_nums:
        q.put(anum)
    # Spawn Threads
    thread_pool = []
    for i in range(int(settings['concurrency'])):
        t = Thread(target=process_next_anum)
        thread_pool.append(t)
        t.start()
    # Wait for Queue to complete
    prev_print = ''
    while any([t.isAlive() for t in thread_pool]):
        time.sleep(1)
        next_print = '{0}/{1}'.format(q.qsize(), len(a_nums))
        if prev_print != next_print:
            logging.info(next_print)
            prev_print = next_print
        else:
            logging.debug('.')
    database_writer_stop_signal.set()
    logging.info('All threads completed.')
    logging.info('Signaling database to wrap up writing...')
    while db_thread.isAlive():
        time.sleep(1)
else:
    logging.info('Running in serial mode.')
    # Parse all anums sequentially
    for idx, anum in enumerate(a_nums):
        if idx % 100 == 0:
            logging.info("{0}/{1}".format(len(a_nums)-idx, len(a_nums)))
        process_anum(anum)

num_files = sum(os.path.isfile(os.path.join(settings['directory'], f)) for f in os.listdir(settings['directory']))
logging.info('Process completed in {0}. {1} files saved out of {2} requested.'.format(
    str(timedelta(seconds=time.time()-start_time)), num_files, len(a_nums)))
