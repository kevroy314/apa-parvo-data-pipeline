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
import copy
import numpy as np
import pandas as pd
from lxml import html
from pandas import DataFrame
from tinydb import *  # This should probably be transitioned to sqlite3 at some point

os.remove('db.json')

db = TinyDB('db.json')

settings = {
    'directory': 'data',
    'input_filename': 'whitelist.txt',
    'concurrency': None,  # This script often runs faster w/o concurrency due the database and proc requirements
    'log_level': 'INFO'
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
    result = np.array(table).shape == expected_size
    if None in expected_size:
        result = all([shape_element == expected_element for (shape_element, expected_element)
                      in zip(np.array(table).shape, expected_size) if expected_element is not None])
    if not result:
        logging.debug('Table has unexpected shape (expected {0}).'.format(expected_size))
        logging.debug('\n' + str(DataFrame(table)))
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


def generate_table_dictionary(data, table_specification, debug_label=''):
    # Parse Elements
    result = {}
    for pt in table_specification:
        test = parse_element(data, pt, debug_label=debug_label)
        result.update(test)
    return result


def generate_flexible_subtable(label, data, iter_table_number, subtable_specification, debug_label='', skip_first=True):
    if not data[iter_table_number]:
        return {label: ''}
    subtable_data = []
    for row_num, row in enumerate(data[iter_table_number]):
        row_spec = copy.deepcopy(subtable_specification)
        if skip_first and row_num == 0:
            continue
        for spec in row_spec:
            spec[1].insert(0, row_num)
            spec[1].insert(0, iter_table_number)
        element = generate_table_dictionary(data, row_spec, debug_label=debug_label)
        if not all(value == '' for value in element.values()):
            subtable_data.append(element)
    return {label: subtable_data}


def sort_and_associate_frames(frames):
    frame_listing = [
        ['^Animal Number:$', [0, 0]],
        ['^Animal: (?!:\s*)[Aa]\d{8}$', [0, 0]],
        ['^(?!:\s*)[Aa]\d{8}', [0, 0]],
        ['^DateSource$', [0, 0]],
        ['^PersonID$', [0, 0]],
        ['^Stage$', [0, 0]],
        ['^Location$', [0, 0]],
        ['^Microchip Number$', [0, 0]],
        ['^Medical Record #$', [0, 0]],
        ['^Conditions$', [0, 0]],
        ['^Tests$', [0, 0]],
        ['^Vaccinations$', [0, 0]],
        ['^Treatments$', [0, 0]],
        ['^Type$', [0, 0]],
        ['^Featured Pet$', [0, 0]],
        ['^Animal Type', [0, 1]],
        ['^Age Groups$', [0, 1]],
        ['Outcome Created Date:', [0, 2]],
        ['^Reason', [2, 2]]
    ]
    frame_assocation = []
    for _ in range(len(frame_listing)):
        frame_assocation.append([])
    for frame in frames:
        if np.array(frame).shape == (0L,):
            continue
        for listing_index, listing in enumerate(frame_listing):
            listing_label = listing[0]
            listing_location = listing[1]
            regex = re.compile(listing_label)
            try:
                if regex.search(reduce(operator.getitem, listing_location, frame)) is not None:
                    frame_assocation[listing_index].append(frame)
            except IndexError:
                pass
    # Flatten single value and empty value elements except intake and outcome (last two in list)
    for index in range(len(frame_assocation) - 2):
        if len(frame_assocation[index]) == 0:
            frame_assocation[index] = None
        elif len(frame_assocation[index]) == 1:
            frame_assocation[index] = frame_assocation[index][0]
    return frame_assocation


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
    expected_shapes = [None, (2L,), (4L, 3L), (None, 8L), None]
    for frame, shape in zip(frames, expected_shapes):
        validate_shape(frame, shape)
    frames = sort_and_associate_frames(frames)
    # print str(DataFrame(frames[3]))
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

        ['featured_pet', [14, 1, 0], ".*", lambda x: x.strip()],
        ['adoption_price', [14, 1, 1], ".*", lambda x: x.strip()],
        ['houstrained', [14, 1, 2], ".*", lambda x: x.strip()],
        ['houstraining_comments', [14, 1, 3], ".*", lambda x: x.strip()],
        ['special_needs', [14, 3, 0], ".*", lambda x: x.strip()],
        ['special_needs_comments', [14, 5, 0], ".*", lambda x: x.strip()],
        ['behavioral_special_needs', [14, 3, 1], ".*", lambda x: x.strip()],
        ['medical_special_needs', [14, 3, 2], ".*", lambda x: x.strip()],
        ['historical_environment', [14, 3, 3], ".*", lambda x: x.strip()],
        ['recommended_environment', [14, 3, 4], ".*", lambda x: x.strip()],
        ['service_animal', [14, 3, 5], ".*", lambda x: x.strip()],

        ['veterinarian', [14, 7, 0], ".*", lambda x: x.strip()],
        ['allergies', [14, 7, 1], ".*", lambda x: x.strip()],
        ['medications', [14, 7, 2], ".*", lambda x: x.strip()],

        ['i_enjoy', [14, 16, 0], ".*", lambda x: x.strip()],
        ['i_am_afraid_of', [14, 16, 1], ".*", lambda x: x.strip()],
        ['people_describe_me_as', [14, 16, 2], ".*", lambda x: x.strip()],

        ['activity_level', [14, 18, 0], ".*", lambda x: x.strip()],
        ['vocalization_level', [14, 18, 1], ".*", lambda x: x.strip()],
        ['off_leash', [14, 18, 2], ".*", lambda x: x.strip()],
        ['training_history', [14, 18, 3], ".*", lambda x: x.strip()],

        ['specific_known_commands', [14, 20, 0], ".*", lambda x: x.strip()],
        ['animal_profile_comments', [14, 22, 0], ".*", lambda x: x.strip()]
    ]

    result = {}

    result.update(generate_flexible_subtable('animal_point_in_time', frames, 3, [
        ['event_date', [0], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['data_source', [0], "(?!\n).*$", lambda x: x.strip()],
        ['size_bcs', [1], ".*", lambda x: x.strip()],
        ['animal_condition_asilomar', [2], ".*", lambda x: x.strip()],
        ['medical_status_age_group', [3], ".*", lambda x: x.replace('no longer in use', '').strip()],
        ['temp_status_weight', [4], ".*", lambda x: x.strip()],
        ['bitten_danger', [5], ".*", lambda x: x.strip()],
        ['s_n_pulse', [6], ".*", lambda x: x.strip()],
        ['temp_resp', [7], ".*", lambda x: x.strip()]
    ], debug_label=a_number))

    # Ownership/guardian
    result.update(generate_flexible_subtable('ownership', frames, 4, [
        ['person_id', [0], ".*", lambda x: x.strip()],
        ['date_from', [1], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y')],
        ['person_name', [2], ".*", lambda x: x.strip()],
        ['phone', [3], ".*", lambda x: x.strip()],
        ['address', [4], ".*", lambda x: x.strip()],
        ['city', [5], ".*", lambda x: x.strip()],
        ['completed_by', [6], ".*", lambda x: x.strip()]
    ], debug_label=a_number))

    # stage
    result.update(generate_flexible_subtable('stage', frames, 5, [
        ['stage', [0], ".*", lambda x: x.strip()],
        ['from', [1], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['review_date', [2], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['by', [3], ".*", lambda x: x.strip()],
        ['stage_change_reason', [4], ".*", lambda x: x.strip()]
    ], debug_label=a_number))

    # location
    result.update(generate_flexible_subtable('location', frames, 6, [
        ['location', [0], ".*", lambda x: x.strip()],
        ['sublocation', [1], ".*", lambda x: x.strip()],
        ['from', [2], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['by', [3], ".*", lambda x: x.strip()]
    ], debug_label=a_number))

    # microchip number
    result.update(generate_flexible_subtable('microchip', frames, 7, [
        ['number', [0], ".*", lambda x: x.strip()],
        ['provider', [1], ".*", lambda x: x.strip()],
        ['issue_date', [2], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')]
    ], debug_label=a_number))

    # medical record
    result.update(generate_flexible_subtable('medical_record', frames, 8, [
        ['record_number', [0], ".*", lambda x: x.strip()],
        ['type', [1], ".*", lambda x: x.strip()],
        ['subtype', [2], ".*", lambda x: x.strip()],
        ['medical_status', [3], ".*", lambda x: x.strip()],
        ['temperament_status', [4], ".*", lambda x: x.strip()],
        ['date', [5], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['review_date', [6], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')]
    ], debug_label=a_number))

    # conditions
    result.update(generate_flexible_subtable('conditions', frames, 9, [
        ['condition', [0], ".*", lambda x: x.strip()],
        ['type', [1], ".*", lambda x: x.strip()],
        ['date', [2], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['body_part', [3], ".*", lambda x: x.strip()],
        ['resolution_date', [4], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['review_date', [5], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['record_number', [6], ".*", lambda x: x.strip()]
    ], debug_label=a_number))

    # tests
    result.update(generate_flexible_subtable('tests', frames, 10, [
        ['type', [0], ".*", lambda x: x.strip()],
        ['for_condition', [1], ".*", lambda x: x.strip()],
        ['result', [2], ".*", lambda x: x.strip()],
        ['date', [3], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['result_date', [4], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['re-test_date', [5], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['record_number', [6], ".*", lambda x: x.strip()]
    ], debug_label=a_number))

    # vaccinations
    result.update(generate_flexible_subtable('vaccinations', frames, 11, [
        ['vaccination', [0], ".*", lambda x: x.strip()],
        ['type', [1], ".*", lambda x: x.strip()],
        ['date', [2], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['re-vacc_date', [3], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['pet_id', [4], ".*", lambda x: x.strip()],
        ['pet_id_type', [5], ".*", lambda x: x.strip()],
        ['record_number', [6], ".*", lambda x: x.strip()]
    ], debug_label=a_number))

    # treatments
    result.update(generate_flexible_subtable('treatments', frames, 12, [
        ['treatment', [0], ".*", lambda x: x.strip()],
        ['type', [1], ".*", lambda x: x.strip()],
        ['dose', [2], ".*", lambda x: x.strip()],
        ['for', [3], ".*", lambda x: x.strip()],
        ['date', [4], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['review_date', [5], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['record_number', [6], ".*", lambda x: x.strip()]
    ], debug_label=a_number))

    # memo
    result.update(generate_flexible_subtable('memo', frames, 13, [
        ['type', [0], ".*", lambda x: x.strip()],
        ['subtype', [1], ".*", lambda x: x.strip()],
        ['date', [2], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
        ['comment', [3], ".*", lambda x: x.strip()],
        ['by', [4], ".*", lambda x: x.strip()],
        ['review_date', [5], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')]
    ], debug_label=a_number))

    # animals
    result.update(generate_flexible_subtable('animals', frames, 15, [
        ['quantity', [0], ".*", lambda x: x.strip()],
        ['animal_type', [1], ".*", lambda x: x.strip()],
        ['lived_with', [2], ".*", lambda x: x.strip()],
        ['interacted_with', [3], ".*", lambda x: x.strip()],
        ['tested_with', [4], ".*", lambda x: x.strip()],
        ['do_not_place', [5], ".*", lambda x: x.strip()]
    ], debug_label=a_number))

    # people
    result.update(generate_flexible_subtable('people', frames, 16, [
        ['quantity', [0], ".*", lambda x: x.strip()],
        ['age_groups', [1], ".*", lambda x: x.strip()],
        ['lived_with', [2], ".*", lambda x: x.strip()],
        ['interacted_with', [3], ".*", lambda x: x.strip()],
        ['tested_with', [4], ".*", lambda x: x.strip()],
        ['do_not_place', [5], ".*", lambda x: x.strip()]
    ], debug_label=a_number))
    # Parse Elements

    # TODO: Update with correct field names/locations
    outcomes = []
    for outcome in frames[17]:
        # people
        outcome_elements = [
            ['date', [0, 0], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
            ['outcome_type', [0, 1], ".*", lambda x: x.strip()],
            ['record_owner', [0, 2], "Record Owner:.*\,", lambda x: x.replace('Record Owner:', '').strip()],
            ['released', [0, 2], "Released:\s*\S*", lambda x: x.replace('Released:', '').strip()],
            ['outcome_created_date', [0, 2], "Created Date:.*", lambda x: x.replace('Created Date:', '').strip()],
            ['status', [1, 0], ".*", lambda x: x.strip()],
            ['location', [1, 1], ".*", lambda x: x.strip()],
            ['p_num', [2, 1], ".*", lambda x: x.strip()],
            ['contact', [2, 2], ".*", lambda x: x.strip()],
            ['subtype', [5, 2], "^.*\,", lambda x: x.replace(',', '').strip()],
            ['issue_date', [5, 2], "Issue Date:.*[AaPp]M", lambda x: datetime.strptime(x.replace('Issue Date:',
                                                                                                 '').strip(),
                                                                                       '%m/%d/%Y %I:%M%p')]
        ]
        current_outcome = {}
        for pt in outcome_elements:
            element = parse_element(outcome, pt, debug_label=a_number)
            current_outcome.update(element)
        outcomes.append({"outcome": current_outcome})
    result.update({'outcomes': outcomes})

    # TODO: Update with correct field names/locations
    intakes = []
    for intake in frames[18]:
        # people
        intake_elements = [
            ['date', [1, 0], ".*", lambda x: datetime.strptime(x, '%m/%d/%Y %I:%M%p')],
            ['intake_type', [1, 1], ".*", lambda x: x.strip()],
            ['record_owner', [1, 2], "Record Owner:.*\,", lambda x: x.replace('Record Owner:', '').strip()],
            ['status', [2, 0], "Status:.*\,", lambda x: x.replace('Status:', '').replace(',', '').strip()],
            ['source_raw', [2, 1], ".*", lambda x: x.strip()],
            ['source', [2, 2], "Source:.*", lambda x: x.replace('Source:', '').strip()],
            ['reason', [2, 2], "Reason:.*", lambda x: x.replace('Reason:', '').strip()],
            ['p_num', [4, 1], ".*", lambda x: x.strip()],
            ['contact', [4, 2], ".*", lambda x: x.strip()],
            ['subcontact', [5, 2], ".*", lambda x: x.strip()]
        ]
        current_intake = {}
        for pt in intake_elements:
            element = parse_element(intake, pt, debug_label=a_number)
            current_intake.update(element)
        intakes.append({"intake": current_intake})
    result.update({'intakes': intakes})

    for pt in parser_table:
        element = parse_element(frames, pt, debug_label=a_number)
        result.update(element)

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
            logging.info('{0} records left to write.'.format(iters - index))
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
            logging.info("{0}/{1}".format(len(a_nums) - idx, len(a_nums)))
        process_anum(anum)

num_files = sum(os.path.isfile(os.path.join(settings['directory'], f)) for f in os.listdir(settings['directory']))
logging.info('Process completed in {0}. {1} files saved out of {2} requested.'.format(
    str(timedelta(seconds=time.time() - start_time)), num_files, len(a_nums)))
