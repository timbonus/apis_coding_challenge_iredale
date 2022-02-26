import datetime
import os
import shlex
import subprocess
import unittest

from car_counter import util
from car_counter.daily_count import DailyCount
from car_counter.quietest_period import QuietestPeriod
from car_counter.max_counts import MaxCounts


class testCarCounter(unittest.TestCase):

    def test_process_row(self):
        space_separated_row = '2021-12-01T07:00:00 10'
        tab_separated_row = '2021-12-01T07:00:00\t10'

        processed_space_row = util.line_to_record(space_separated_row)
        processed_tab_row = util.line_to_record(tab_separated_row)

        expected_timestamp = datetime.datetime(year=2021,
                                               month=12,
                                               day=1,
                                               hour=7,
                                               minute=0,
                                               second=0)

        # Check correct parsing of space separated row.
        self.assertEqual(processed_space_row[0], expected_timestamp)
        self.assertEqual(processed_space_row[1], 10)

        # Check correct parsing of tabs separated row.
        self.assertEqual(processed_tab_row[0], expected_timestamp)
        self.assertEqual(processed_tab_row[1], 10)


class testMaxCollection(unittest.TestCase):

    def setUp(self):
        half_hours = [i * datetime.timedelta(minutes=30) for i in range(10)]

        start_time = datetime.datetime(year=2021,
                                       month=12,
                                       day=31,
                                       hour=23,
                                       minute=0,
                                       second=0)

        self.timestamps = [start_time + hh for hh in half_hours]

    def test_add_new_smallest_count_collection_not_full(self):
        mc = MaxCounts()

        first_timestamp, first_data = self.timestamps[0], 4
        second_timestamp, second_data = self.timestamps[1], 2

        mc.process_record((first_timestamp, first_data))
        mc.process_record((second_timestamp, second_data))

        self.assertEqual(len(mc), 2)
        # smallest count (second record) is first in heap
        self.assertEqual(mc._counts[0], (second_data, second_timestamp))

    def test_add_new_largest_count_collection_not_full(self):
        mc = MaxCounts()

        first_timestamp, first_data = self.timestamps[0], 2
        second_timestamp, second_data = self.timestamps[1], 4

        mc.process_record((first_timestamp, first_data))
        mc.process_record((second_timestamp, second_data))

        self.assertEqual(len(mc), 2)
        # largest count (second record) is in last position in heap
        self.assertEqual(mc._counts[0], (first_data, first_timestamp))

    def test_add_new_smallest_count_collection_full(self):
        mc = MaxCounts()

        first_timestamp, first_data = self.timestamps[0], 1
        second_timestamp, second_data = self.timestamps[1], 4
        third_timestamp, third_data = self.timestamps[2], 3
        fourth_timestamp, fourth_data = self.timestamps[3], 2

        mc.process_record((first_timestamp, first_data))
        mc.process_record((second_timestamp, second_data))
        mc.process_record((third_timestamp, third_data))
        mc.process_record((fourth_timestamp, fourth_data))

        self.assertEqual(len(mc), 3)
        # third largest count (fourth record) count is in first position in heap
        self.assertEqual(mc._counts[0], (fourth_data, fourth_timestamp))

    def test_small_new_record_not_added_to_heap(self):
        mc = MaxCounts()

        first_timestamp, first_data = self.timestamps[0], 2
        second_timestamp, second_data = self.timestamps[1], 4
        third_timestamp, third_data = self.timestamps[2], 3
        fourth_timestamp, fourth_data = self.timestamps[3], 1

        mc.process_record((first_timestamp, first_data))
        mc.process_record((second_timestamp, second_data))
        mc.process_record((third_timestamp, third_data))
        mc.process_record((fourth_timestamp, fourth_data))

        self.assertEqual(len(mc), 3)
        # third largest count (first record) is in first position in heap
        self.assertEqual(mc._counts[0], (first_data, first_timestamp))

    def test_serialise(self):
        mc = MaxCounts()

        first_timestamp, first_data = self.timestamps[0], 2
        second_timestamp, second_data = self.timestamps[1], 4

        mc.process_record((first_timestamp, first_data))
        mc.process_record((second_timestamp, second_data))

        serialised_counts = mc.serialise()
        self.assertEqual(serialised_counts[0], '2021-12-31T23:00:00 2')
        self.assertEqual(serialised_counts[1], '2021-12-31T23:30:00 4')


class testQuietestPeriod(unittest.TestCase):

    def setUp(self):
        half_hours = [i * datetime.timedelta(minutes=30) for i in range(10)]

        start_time = datetime.datetime(year=2021,
                                       month=12,
                                       day=31,
                                       hour=23,
                                       minute=0,
                                       second=0)

        self.timestamps = [start_time + hh for hh in half_hours]

    def test_add_new_record_small_queue(self):
        qp = QuietestPeriod(size=3)

        first_record = (self.timestamps[0], 4)
        second_record = (self.timestamps[1], 2)

        qp.process_record(first_record)
        qp.process_record(second_record)

        self.assertEqual(len(qp._current_period), 2)
        self.assertEqual(qp._current_period_count, 6)
        self.assertIsNone(qp._quietest_period)

    def test_add_new_record_no_quiet_period_update(self):
        qp = QuietestPeriod(size=3)

        first_record = (self.timestamps[0], 1)
        second_record = (self.timestamps[1], 2)
        third_record = (self.timestamps[2], 3)
        fourth_record = (self.timestamps[3], 4)

        qp.process_record(first_record)
        qp.process_record(second_record)
        qp.process_record(third_record)

        self.assertEqual(len(qp._current_period), 3)
        self.assertEqual(qp._current_period_count, 6)
        self.assertIsNotNone(qp._quietest_period)
        self.assertEqual(qp._quietest_period_count, 6)

        qp.process_record(fourth_record)

        # quietest period not updated by current period which has larger cumulative count
        self.assertEqual(qp._current_period_count, 9)
        self.assertEqual(qp._quietest_period_count, 9)

    def test_add_new_record_quiet_period_update(self):
        qp = QuietestPeriod(size=3)

        first_record = (self.timestamps[0], 4)
        second_record = (self.timestamps[1], 3)
        third_record = (self.timestamps[2], 2)
        fourth_record = (self.timestamps[3], 1)

        qp.process_record(first_record)
        qp.process_record(second_record)
        qp.process_record(third_record)

        self.assertEqual(len(qp._current_period), 3)
        self.assertEqual(qp._current_period_count, 9)
        self.assertIsNotNone(qp._quietest_period)
        self.assertEqual(qp._quietest_period_count, 9)

        qp.process_record(fourth_record)

        # quietest period is updated by current period which has smaller cumulative count
        self.assertEqual(qp._current_period_count, 6)
        self.assertEqual(qp._quietest_period_count, 6)

    def test_serialise(self):
        qp = QuietestPeriod(size=3)

        first_record = (self.timestamps[0], 4)
        second_record = (self.timestamps[1], 3)
        third_record = (self.timestamps[2], 2)

        qp.process_record(first_record)
        qp.process_record(second_record)
        qp.process_record(third_record)

        serialised_counts = qp.serialise()
        self.assertEqual(serialised_counts[0], '2021-12-31T23:00:00 4')
        self.assertEqual(serialised_counts[1], '2021-12-31T23:30:00 3')
        self.assertEqual(serialised_counts[2], '2022-01-01T00:00:00 2')


class testDailyCounts(unittest.TestCase):

    def setUp(self):

        half_hours = [i * datetime.timedelta(minutes=30) for i in range(10)]

        start_time = datetime.datetime(year=2021,
                                       month=12,
                                       day=31,
                                       hour=23,
                                       minute=0,
                                       second=0)

        self.timestamps = [start_time + hh for hh in half_hours]

        self.dc = DailyCount()

        for ts in self.timestamps:
            self.dc.process_record((ts, 5))

    def test_daily_counts(self):
        self.assertEqual(self.dc._daily_counts['2021-12-31'], 10)
        self.assertEqual(self.dc._daily_counts['2022-01-01'], 40)

    def test_serialise(self):
        serialised_counts = self.dc.serialise()
        self.assertEqual(serialised_counts[0], '2021-12-31 10')
        self.assertEqual(serialised_counts[1], '2022-01-01 40')

    def test_total_count(self):
        self.assertEqual(self.dc.total_count(), 50)


class testMain( unittest.TestCase ):

    def setUp(self):
        self.input = "2021-01-01T00:00:00 5\n" * 50
        with open('test_data', 'w') as f:
            f.write(self.input)

    def tearDown(self):
        if os.path.exists('test_data'):
            os.remove('test_data')
        if os.path.exists('test_out'):
            os.remove('test_out')

    def test_stdin_stdout(self):
        # run main.py with no input/output files.
        p = subprocess.run( shlex.split( 'python3 main.py'),
                            input=self.input.encode('utf-8'),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE )

        # Check something was sent to stdout as results.
        captured_output = p.stdout.decode('utf-8')
        lines = captured_output.split('\n')
        self.assertTrue(len(lines) > 10)

    def test_filein_fileout(self):
        # run main.py with input/output files.
        p = subprocess.run( shlex.split( 'python3 main.py --path-in test_data --path-out test_out'),
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE )

        self.assertEqual(p.returncode, 0)

        with open('test_out') as f:
            line = f.readline()
            line_count = 1
            while len(line) > 0:
                line_count += 1
                line = f.readline()

        # Check something was output to result file.
        self.assertTrue(line_count > 10)

