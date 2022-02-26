import argparse
import sys
from argparse import RawTextHelpFormatter

from car_counter import util
from car_counter.daily_count import DailyCount
from car_counter.quietest_period import QuietestPeriod
from car_counter.max_counts import MaxCounts


description = """
Process car counter data file and return the following results as text:
 - Total car count - cumulative total across entire file.
 - Daily counts - cumulative total of counts in the file grouped by date.
 - Lowest count period - observations from the lowest consecutive three half-hour observations in the file.
 - Largest observations - the top three observations in the file.

Usage:
    python3 main.py [<options>]

Optional arguments to specify source data and output file:
    --path-in <path_to_file>   process the file <path_to_file>. If not specified stdin is used.
    --path-out <path_to_file>  output the results to <file path_to_file>. If not specified stdout is used.
"""

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)
    parser.add_argument('--path-in', help='Path to file to process.')
    parser.add_argument('--path-out', help='Results output to this path.')
    args = parser.parse_args()

    if args.path_in is not None:
        input_buffer = open(args.path_in)
    else:
        input_buffer = sys.stdin

    if args.path_out is not None:
        output_buffer = open(args.path_out, 'w')
    else:
        output_buffer = sys.stdout

    daily_count = DailyCount()
    quietest_period = QuietestPeriod()
    max_counts = MaxCounts()

    line = input_buffer.readline()
    line_count = 1
    while len(line) > 0:
        try:
            record = util.line_to_record(line)
            daily_count.process_record(record)
            quietest_period.process_record(record)
            max_counts.process_record(record)
            line = input_buffer.readline()
            line_count += 1
        except ValueError:
            output_buffer.write("Line {} of input could not be parsed. Skipping.\n".format(line_count))
            line = input_buffer.readline()
            line_count += 1

    input_buffer.close()

    output_buffer.write("\n")
    output_buffer.write("Total car count: ")
    output_buffer.write(str(daily_count.total_count()))
    output_buffer.write("\n")
    output_buffer.write("\n")
    output_buffer.write("Daily counts:\n")
    output_buffer.write("\n".join(daily_count.serialise()))
    output_buffer.write("\n")
    output_buffer.write("\n")
    output_buffer.write("Consecutive three half-hourly measurements with lowest cumulative count:\n")
    output_buffer.write("\n".join(quietest_period.serialise()))
    output_buffer.write("\n")
    output_buffer.write("\n")
    output_buffer.write("Largest half-hourly counts in the file:\n")
    output_buffer.write("\n".join(max_counts.serialise()))
    output_buffer.write("\n")
    output_buffer.write("\n")

    output_buffer.close()









