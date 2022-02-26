class DailyCount(object):
    """
    Maintains a records of daily cumulative counts for any number of (timestamp, count) records.
    """
    def __init__(self):
        self._daily_counts = {}

    def process_record(self, record):
        """
        Process single record of (timestamp, count) and add to daily count.
        :param record: tuple of type (datetime.datetime, int)
        """
        timestamp, count = record
        date = str(timestamp.date())

        if date not in self._daily_counts:
            self._daily_counts[date] = count
        else:
            self._daily_counts[date] += count

    def serialise(self):
        """
        String formatting of current daily count.
        :return: list of str
        """
        ordered_dates = sorted(self._daily_counts.keys())
        return ['{} {}'.format(date, self._daily_counts[date]) for date in ordered_dates]

    def total_count(self):
        """
        Calculates cumulative count across all days.
        :return: int of cumulative count across all days.
        """
        return sum([count for count in self._daily_counts.values()])
