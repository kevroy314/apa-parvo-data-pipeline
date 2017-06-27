from tinydb import *  # This should probably be transitioned to sqlite3 at some point
from datetime import datetime, timedelta
import operator
import itertools
import matplotlib.pyplot as plt
from matplotlib.dates import YearLocator, MonthLocator, DateFormatter

db = TinyDB('db.json')
animal = Query()


def get_unique_value_for_field(field, search=None):
    q = Query()
    if search is None:
        return list(set([reduce(operator.getitem, field, item) for item in db.search(q.anum != '')]))
    else:
        return list(set([reduce(operator.getitem, field, item) for item in db.search(search)]))


def length_test(val, gt, lt):
    return lt > len(val) > gt


def date_between(val, start_date, end_date):
    return start_date <= datetime.fromtimestamp(val) < end_date


def contains_parvo_test_positive(tests_val):
    for test in tests_val:
        if "Parvo Test (IDEXX)".lower() in test['type'].lower() and "Positive".lower() in test['result'].lower():
            return True
    return False


# print list(set([item['anum'] for item in db.search(animal.tests.test(contains_parvo_test_positive))]))

print len(db)
print db.count(animal.intakes[0].intake.intake_type == 'Transfer In')
# print list(set([item['intakes'][0]['intake']['intake_type'] for item in db.search(animal.intakes[0].intake.intake_type != 'Transfer In')]))
# print get_unique_value_for_field(['intakes', 0, 'intake', 'intake_type'])
# print get_unique_value_for_field(['intakes', 1, 'intake', 'intake_type'], search=animal.intakes.test(length_test, 1, sys.maxint))
# print get_unique_value_for_field(['outcomes', 1, 'outcome', 'outcome_type'], search=animal.intakes.test(length_test, 1, sys.maxint))
years = range(2007, 2018)
dates = ['01/{0}/{1}'.format(month, year) for (year, month) in list(itertools.product(years, range(1, 13)))]

for year in years:
    date0 = datetime.strptime('01/01/{0}'.format(year), '%d/%m/%Y')
    date1 = datetime.strptime('01/01/{0}'.format(year + 1), '%d/%m/%Y') - timedelta(seconds=1)
    print '{0} to {1} : {2}'.format(date0, date1,
                                    len(get_unique_value_for_field(['anum'],
                                                                   search=animal.intakes[0].intake.date.test(
                                                                       date_between, date0, date1))))

values = []
for idx in range(len(dates) - 1):
    date0 = datetime.strptime(dates[idx], '%d/%m/%Y')
    date1 = datetime.strptime(dates[idx + 1], '%d/%m/%Y') - timedelta(seconds=1)
    value = get_unique_value_for_field(['anum'], search=animal.intakes[0].intake.date.test(
        date_between,
        date0,
        date1
    ))
    values.append(len(value))
    print '{0} to {1} : {2}'.format(date0, date1, len(value))

'''
import numpy as np
from scipy.optimize import leastsq
import pylab as plt

data = [0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 10, 7, 6, 2, 11, 18, 34, 32, 15, 8, 4, 19, 28, 12, 18, 9, 19, 18, 37, 39, 15, 6, 3, 6, 14, 20, 20, 20, 13, 22, 27, 29, 14, 1, 8, 8, 33, 37, 39, 35, 50, 101, 62, 39, 45, 15, 8, 15, 36, 45, 19, 11, 15, 41, 34, 55, 43, 26, 20, 29, 41, 22, 46, 27, 33, 51, 77, 86, 57, 20, 14, 19, 18, 24, 19, 47, 22, 26, 50, 68, 35, 38, 22, 27, 23, 31, 46, 47, 56, 59, 77, 51, 44, 18, 28, 64, 40]
N = len(data)
t = np.linspace(0, len(years)*2*np.pi, N)
xdates = [datetime.strptime(d, '%d/%m/%Y') for d in dates][0:-1]
'''
'''values = [0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 10, 7, 6, 2, 11, 18, 34, 32, 15, 8, 4, 19, 28, 12, 18, 9, 19, 18, 37, 39, 15, 6, 3, 6, 14, 20, 20, 20, 13, 22, 27, 29, 14, 1, 8, 8, 33, 37, 39, 35, 50, 101, 62, 39, 45, 15, 8, 15, 36, 45, 19, 11, 15, 41, 34, 55, 43, 26, 20, 29, 41, 22, 46, 27, 33, 51, 77, 86, 57, 20, 14, 19, 18, 24, 19, 47, 22, 26, 50, 68, 35, 38, 22, 27, 23, 31, 46, 47, 56, 59, 77, 51, 44, 18, 28, 64, 40]
years = YearLocator()   # every year
months = MonthLocator([1, 7])  # every month
yearsFmt = DateFormatter('%m/%Y')



fig, ax = plt.subplots()
ax.bar(xdates, values, width=10)
ax.xaxis_date()

# format the ticks
ax.xaxis.set_major_locator(months)
ax.xaxis.set_major_formatter(yearsFmt)
ax.autoscale_view()

ax.fmt_xdata = DateFormatter('%m/%Y')

ax.grid(True)

fig.autofmt_xdate()
plt.show()
'''
'''
guess_mean = np.mean(data)*4
guess_std = 3*np.std(data)/(2**0.5)
guess_phase = 3.14/2

# we'll use this to plot our first estimate. This might already be good enough for you
data_first_guess = guess_std*np.sin(t+guess_phase) + guess_mean

# Define the function to optimize, in this case, we want to minimize the difference
# between the actual data and our "guessed" parameters
optimize_func = lambda x: x[0]*np.sin(t+x[1]) + x[2] - data
est_std, est_phase, est_mean = leastsq(optimize_func, [guess_std, guess_phase, guess_mean])[0]

# recreate the fitted curve using the optimized parameters
data_fit = est_std*np.sin(t+est_phase) + est_mean


years = YearLocator()   # every year
months = MonthLocator([1, 7])  # every month
yearsFmt = DateFormatter('%m/%Y')



fig, ax = plt.subplots()
ax.bar(xdates, data, width=10)
ax.plot(xdates, data_fit, '-')
ax.xaxis_date()

# format the ticks
ax.xaxis.set_major_locator(months)
ax.xaxis.set_major_formatter(yearsFmt)
ax.autoscale_view()

ax.fmt_xdata = DateFormatter('%m/%Y')

ax.grid(True)

fig.autofmt_xdate()
plt.show()
'''
'''for table in db.tables():
    contents = db.table(table).all()

    schema = Counter(frozenset(doc.keys()) for doc in contents)

    print('table %s (documents %d):' % (table, sum(schema.values())))
    for fields, count in schema.iteritems():
        print('  document (count %d):' % count)
        print('\n'.join('    %s' % field for field in fields))'''
