import heapq

class MaxCounts( object ):
    """
    Maintains a record of largest counts for any number of processed (timestamp, count) records.
    """
    def __init__(self, size=3):
        """
        :param size: top N count values to track.
        """
        self._counts = []
        self._size = size

    def process_record(self, record):
        """
        Process single record of (timestamp, count) and determine if it should be added to the largest count collection.
        :param record: tuple of type (datetime.datetime, int)
        """
        new_timestamp, new_count = record
        heap_formatted_record = (new_count, new_timestamp)
        if len(self) < self._size:
            heapq.heappush(self._counts, heap_formatted_record)
        elif self._counts[0][0] < new_count:
            heapq.heappop(self._counts)
            heapq.heappush(self._counts, heap_formatted_record)

    def serialise(self):
        """
        String formatting of current largest count collection.
        :return: list of str
        """
        return ['{} {}'.format(max_record[1].isoformat(), max_record[0]) for max_record in self._counts]

    def __len__(self):
        return len(self._counts)