import math
import re


class Detector(object):
    """
    Resample given data to use only full lines (w/o truncated).
    Detect possible delimiter character based on it's frequency. Gracefully
    handles multiline columns.
    """

    frequency_table = None
    non_delimiter_re = re.compile('[\w]', re.UNICODE)
    buff_size = 1024 * 2048

    def __init__(self):
        self.frequency_table = {}

    def increment(self, char, lines):
        if char not in self.frequency_table:
            self.frequency_table[char] = {}
        if lines not in self.frequency_table[char]:
            self.frequency_table[char][lines] = 0

        self.frequency_table[char][lines] += 1

    def detect(self, sample, quotechar='"'):
        lines_analyzed, lines_content = self.get_sample(sample,
                                                        quotechar=quotechar)
        resampled = "".join(lines_content[:-1])
        return self.analyze(lines_analyzed), resampled

    def get_sample(self, sample, sample_lines=20, quotechar='"'):
        """
        Given actual sample method extracts complete lines and fills delimiter
        candidates table with frequencies of occurrences.
        """
        enclosed = False
        actual_lines = 1
        lines = []
        single_line = ""

        for idx, ch in enumerate(sample):
            if actual_lines >= sample_lines:
                break
            prev_ch = sample[idx-1] if idx else None
            next_ch = sample[idx+1] if (idx + 1) < len(sample) else None
            single_line += ch

            if ch == quotechar:
                if enclosed and next_ch != quotechar:
                    enclosed = False
                elif not enclosed:
                    enclosed = True
            elif not enclosed and \
                    (ch == '\n' and prev_ch != '\r' or ch == '\r'):
                actual_lines += 1
                lines.append(single_line)
                single_line = ""

            elif not enclosed and not self.non_delimiter_re.match(ch):
                self.increment(ch, actual_lines)

        return actual_lines, lines

    def analyze(self, lines_analyzed):
        candidates = []
        for delim, freq in self.frequency_table.items():
            deviation = self.deviation(freq, lines_analyzed)

            if float(0.0) == deviation:
                candidates.append(delim)
        return candidates

    def mean(self, line_freq, lines_analyzed):
        """ Calculates mean frequency of potential delimiters occurrences

        :param line_freq: dict[int]int
        :param lines_analyzed: int
        :return: int
            mean value of all characters appeared at sample
        """
        freqs = list(line_freq.values())[:lines_analyzed]
        return sum(freqs) / lines_analyzed

    def deviation(self, line_freq, lines_analyzed):
        lines_to_account = lines_analyzed - 1

        average = self.mean(line_freq, lines_to_account)
        frequencies = list(line_freq.values())[:lines_to_account]

        squares = map(lambda frequency: (float(average) - frequency) ** 2,
                      frequencies)

        try:
            deviation = math.sqrt(sum(squares) / (lines_to_account - 1))
        except ZeroDivisionError:
            deviation = -1

        return deviation
