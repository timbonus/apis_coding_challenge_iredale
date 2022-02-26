class QuietestPeriod(object):
    """
    Maintains a record of period of consecutive half-hours with lowest cumulative count, for any number of
    (timestamp, count) records.
    """
    def __init__(self, size=3):
        """
        :param size: int, number of half hour periods defining length of lowest count period to track.
        """
        self._size = size
        self._current_period = []
        self._current_period_count = 0
        self._quietest_period = None
        self._quietest_period_count = 0

    def process_record(self, record):
        """
        Process single record of (timestamp, count) and determine if it and previous records should be tracked as the
        lowest count period.
        :param record: tuple of type (datetime.datetime, int)
        """
        self._current_period.append(record)
        self._current_period_count += record[1]

        if (self._quietest_period is None) and (len(self._current_period) == self._size):
            self._update_quietest_period()
        elif len(self._current_period) > self._size:
            unqueued_record = self._current_period.pop(0)
            self._current_period_count -= unqueued_record[1]
            self._update_quietest_period()

    def serialise(self):
        """
        String formatting of quietest period records.
        :return: list of str
        """
        if self._quietest_period is not None:
            return ['{} {}'.format(period[0].isoformat(), period[1]) for period in self._quietest_period]
        else:
            return ['There are less than {} records in the input file.'.format(self._size)]

    def _update_quietest_period(self):
        self._quietest_period_count = self._current_period_count
        self._quietest_period = self._current_period.copy()

    def __len__(self):
        return len(self._current_period)