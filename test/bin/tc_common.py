"""
Some handy objects that appear in multiple test scripts.
"""

def read_config(filename):
    config_file_obj = open(f'{filename}/config.txt', 'r')
    config_dict = {line.split(':')[0]: line.split(':')[1].rstrip('\n') for line in config_file_obj.readlines()}
    config_file_obj.close()
    return(config_dict)


class TestCase():
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def doi_record(self):
        return self.evaluate_file(f"{self.dirname}/doi_record.txt")
    def author_details(self):
        return self.evaluate_file(f"{self.dirname}/author_details.txt")
    def candidate_ids(self):
        return eval(self.initial_candidate_employee_ids)
    def id_result(self):
        id_results_from_file = []
        for id in eval(self.initial_candidate_employee_ids):
            id_results_from_file.append( self.evaluate_file(f'{self.dirname}/id_result_{id}.txt') )
        return id_results_from_file
    def parse_proposed_guesses(self):
        return [s.split('|') for s in self.proposed_guesses.split(';')]

    def evaluate_file(self, filename):
        """
        Read a file and return a data structure that is just what you would get from a particular doi_common or jrc_common command.
        """
        with open(filename, 'r') as inF:
            return eval(inF.readlines()[0].rstrip('\n'))


