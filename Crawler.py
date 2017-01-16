from Tkinter import *
from threading import Thread
from tkFileDialog import askopenfilename
from tkinter import messagebox

import Queue
import cookielib
import datetime
import logging
import mechanize
import os
import time
import traceback
from lxml import html
from mechanize import ControlNotFoundError

###########################
# Settings
###########################
# If defaults are set, some input steps are skipped
settings = {'path': 'C:\\Users\\Kevin\\PycharmProjects\\ParvoCrawler\\parvo.txt',
            'shelter': 'USTX95',
            'username': '',
            'password': '',
            'skip_login': False,
            'skip_downloaded_files': True,
            'filter_keywords': ['parvo'],
            'directory': 'data',  # time.strftime("%Y_%m_%d-%I_%M_%S")
            'url_template': 'http://sms.petpoint.com/sms3/embeddedreports/animalviewreport.aspx?AnimalID=',
            'cookies': ["rbCookie1rbBodysbAnimal", "rbCookie1rbBodysbAnimalDetails", "rbCookie1rbBodysbAnimalGroup",
                        "rbCookie1rbBodysbAnimalPIT", "rbCookie1rbBodysbIntake", "rbCookie1rbBodysbOutcome",
                        "rbCookie1rbBodysbOwnership", "rbCookie1rbBodysbLostFound", "rbCookie1rbBodysbCareActivity",
                        "rbCookie1rbBodysbStage", "rbCookie1rbBodysbLocation", "rbCookie1rbBodysbAnimalHold",
                        "rbCookie1rbBodysbMicrochip", "rbCookie1rbBodysbAnimalTag", "rbCookie1rbBodysbExam",
                        "rbCookie1rbBodysbBehaviorTestsCompleted", "rbCookie1rbBodysbBehaviorTestsScheduled",
                        "rbCookie1rbBodysbAnimalMemo", "rbCookie1rbBodysbVoucher", "rbCookie1rbBodysbWaiver",
                        "rbCookie1rbBodysbProfile", "rbCookie1rbBodysbFoster", "rbCookie1rbBodysbCase",
                        "rbCookie1rbBodysbLicense",
                        "rbCookie1rbBodysbContacts", "rbCookie1rbBodysbTransferNWRequest", "rbCookie1rbBodysbSchedule",
                        "rbCookie1rbBodysbHotline", "rbCookie1rbBodysbDocumentList"],
            'concurrency': 50,
            'log_level': 'WARNING'
            }
###########################
# Setup Logging
###########################
level = 'WARNING'
if 'log_level' in settings:
    level = settings['log_level'].strip()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s, %(levelname)s: %(message)s',
                    filename='{0}.log'.format(time.strftime("%Y_%m_%d-%I_%M_%S")))
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.getLevelName(level))
# tell the handler to use this format
console.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s: %(message)s'))
# add the handler to the root logger
logging.getLogger('').addHandler(console)
###########################
# File I/O Setup
###########################
# If no default is set, for the path, prompt user for newline separated file of A########
# Determine the input file path
if settings['path'] is None:
    input_filename = askopenfilename().strip()
else:
    input_filename = settings['path'].strip()

if input_filename is '' or not os.path.exists(input_filename):
    logging.error('Input file {0} not found. Ensure the file/path exists.'.format(input_filename))
    exit()

# Confirm output directory exists and create it if it does not
if 'directory' not in settings:
    settings['directory'] = ''
# If the directory is not an absolute path, make it one, assuming the root directory is the input file directory
if not os.path.isabs(settings['directory']):
    settings['directory'] = os.path.join(os.path.dirname(input_filename), settings['directory']).strip()
if not os.path.exists(settings['directory']):
    logging.warning('Output directory {0} doesn\'t exist. An attempt will be made to create '
                    'it automatically.'.format(settings['directory']))
    os.mkdir(settings['directory'])

# Read the input file
f = open(input_filename, 'r')
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
###########################
# Get Credentials From User
###########################

# Generate the root window for credentials
root = Tk()
root.withdraw()
root.resizable(0, 0)  # Remove the Maximize button and make non-resizable


# Callback function for the Begin button (just closes and quits the window loop)
# noinspection PyUnusedLocal
def callback(x=0):
    global root
    root.withdraw()
    root.quit()


def on_closing():
    if messagebox.askokcancel("Quit", "Do you want to quit?"):
        root.destroy()
        exit()

root.protocol("WM_DELETE_WINDOW", on_closing)

# Hide the window by default, and populate it
root.deiconify()
root.bind('<Return>', callback)  # Return button submits the form
# Populate the fields with defaults
shelter_id_label = Label(root, text="Shelter ID")
shelter_id_entry = Entry(root)
if 'shelter' in settings:
    shelter_id_entry.insert(END, settings['shelter'].strip())
username_label = Label(root, text="Username")
username_entry = Entry(root)
if 'username' in settings:
    username_entry.insert(END, settings['username'].strip())
password_label = Label(root, text="Password")
password_entry = Entry(root)
if 'password' in settings:
    password_entry.insert(END, settings['password'].strip())
submit_button = Button(root, text="Begin", command=callback)
shelter_id_label.pack()
shelter_id_entry.pack()
username_label.pack()
username_entry.pack()
password_label.pack()
password_entry.pack()
submit_button.pack()
# Set focus on the first field
shelter_id_entry.focus_set()

# If skip_login is true, just use whatever the defaults are for credentials
if 'skip_login' in settings and not settings['skip_login']:
    root.mainloop()

# All inputs have been acquired, parsing starts
payload = {
    "ctl00$cphSearchArea$txtShelterPetFinderId": shelter_id_entry.get(),
    "ctl00$cphSearchArea$txtUserName": username_entry.get(),
    "ctl00$cphSearchArea$txtPassword": password_entry.get()
}


###########################
# Setup Browser
###########################
def get_authenticated_browser_and_cookies(suppress_log=False):
    global settings
    if not suppress_log:
        logging.info("Setting Up Browser/Cookie Jar...")
    # Setup browser/cookies
    browser = mechanize.Browser()
    browser.addheaders = [("User-agent",
                           "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.2.13) Gecko/20101206 Ubuntu/10.10 ("
                           "maverick) Firefox/3.6.13")]
    cookie_jar = cookielib.LWPCookieJar()
    browser.set_cookiejar(cookie_jar)

    ###########################
    # Authenticate Session
    ###########################
    if not suppress_log:
        logging.info("Authenticating...")
    if 'url_template' not in settings:
        if not suppress_log:
            logging.error('Error: No url_template provided. No requests can be made.')
        exit()
    # Load a page to generate the login prompt
    browser.open(settings['url_template'].strip() + '000000000').read()
    browser.select_form(nr=0)
    # Login with the user provided credentials
    for key in payload:
        browser[key] = payload[key]
    tmp = browser.submit().read()  # submitting the login credentials
    if 'Please sign in to continue' in tmp:
        if not suppress_log:
            logging.error('Authentication failed due to bad credentials.')
        exit()
    return browser, cookie_jar


###########################
# ANum Processing
###########################
# This function does all the appropriate processing for each A number
def process_anum(a_number, browser, cookie_jar):
    global settings
    if 'skip_downloaded_files' in settings and settings['skip_downloaded_files'] and os.path.exists(
            os.path.join(settings['directory'], 'A' + a_number + '.htm')):
        logging.info("A{0} was skipped because it was already downloaded (set skip_downloaded_files to False to "
                     "redownload).".format(a_number))
        return

    # Open the URL for a given anum
    browser.open(settings['url_template'].strip() + a_number).read()

    # The only way to get the whole details expanded is to set these cookies to 'block'
    # Set the cookies so all fields are loaded
    if 'cookies' in settings:
        for c in settings['cookies']:
            cookie = cookielib.Cookie(version=0, name=c, value='block', expires=99999, port=None,
                                      port_specified=False, domain='sms.petpoint.com', domain_specified=True,
                                      domain_initial_dot=False, path='/sms3/embeddedreports', path_specified=True,
                                      secure=False, discard=False, comment=None, comment_url=None,
                                      rest={'HttpOnly': False}, rfc2109=False)
            cookie_jar.set_cookie(cookie)
    browser.set_cookiejar(cookie_jar)
    browser.select_form(nr=0)
    result = ''
    # Reload the page
    try:
        result = browser.submit("ctl00$cphSearchArea$btnPostBackButton").read()
    except ControlNotFoundError:
        logging.debug('ANum {0} did not load properly. Confirm it is a valid ANum and try again.'.format(a_number))

    # Filter based on the filter inputs
    found_key = ''
    if 'filter_keywords' in settings:
        case_insensitive_result = result.lower().strip()
        for filter_key in settings['filter_keywords']:
            if filter_key.strip() not in case_insensitive_result:
                found_key = filter_key.strip()
                break

    if found_key != '':
        logging.info("A{0} was skipped for not containing filter '{1}'.".format(a_number, found_key))
        return
    else:
        # At this point the page has passed the filter
        tree = html.fromstring(result)
        logging.info("{0} completed successfully.".format(
            tree.get_element_by_id("cphWorkArea_lblAnimalNumber").text_content().strip()))

    # Print .htm file
    out_file = open(os.path.join(settings['directory'], 'A' + a_number + '.htm'), 'w')
    out_file.write(result)
    out_file.close()


def process_next_anum():
    global q
    # noinspection PyBroadException
    try:
        b, c = get_authenticated_browser_and_cookies()
    except:
        logging.warning('There was an error authenticating which resulted in a thread not spinning up.')
        return
    while True:
        a = None
        # noinspection PyBroadException
        try:
            a = q.get(True, 0)
            process_anum(a, b, c)
        except Queue.Empty:
            logging.debug('Closing thread gracefully after empty queue.')
            exit()
        except Exception:
            if a is not None:
                logging.error('There was an error processing the anum, {0}.'.format(a))
            logging.error(traceback.print_exc())
            pass


###########################
# Begin Crawling
###########################
# Begin time logging
start_time = time.time()

logging.info('Initializing...')

if 'concurrency' in settings:
    logging.info('Setting up concurrency with {0} thread(s).'.format(settings['concurrency']))
    # Test Credentials
    get_authenticated_browser_and_cookies(suppress_log=True)
    # Construct Queue
    q = Queue.Queue(len(a_nums))
    for anum in a_nums:
        q.put(anum)
    # Spawn Threads
    thread_pool = []
    for i in range(settings['concurrency']):
        t = Thread(target=process_next_anum)
        thread_pool.append(t)
        t.start()
    # Wait for Queue to complete
    while any([t.isAlive() for t in thread_pool]):
        time.sleep(0.1)
    time.sleep(1)
else:
    logging.info('Running in serial mode.')
    # Parse all anums sequentially
    br, cj = get_authenticated_browser_and_cookies()
    for anum in a_nums:
        process_anum(anum, br, cj)

num_files = sum(os.path.isfile(os.path.join(settings['directory'], f)) for f in os.listdir(settings['directory']))
logging.info('Process completed in {0}. {1} files saved out of {2} requested.'.format(
    str(datetime.timedelta(seconds=time.time()-start_time)), num_files, len(a_nums)))
