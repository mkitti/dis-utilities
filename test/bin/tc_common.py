class TestCase():
    # def __init__(self, **kwargs):
    #     for key, value in kwargs.items():
    #         setattr(self, key, value)
    def read_config(self, filename):
        with open(f'{filename}/config.txt', 'r') as config_file_obj:
            config_dict = {line.split(':')[0]: line.split(':')[1].rstrip('\n') for line in config_file_obj.readlines()}
        for key, value in config_dict.items():
            setattr(self, key, value)

    # def doi_record(self):
    #     return self.evaluate_file(f"{self.dirname}/doi_record.txt")
    def doi_record(self):
        return self.read_file(f"{self.dirname}/doi_record.txt")
    # def author_details(self):
    #     return self.evaluate_file(f"{self.dirname}/author_details.txt")
    def author_details(self):
        return self.read_file(f"{self.dirname}/author_details.txt")
    def author_objects(self):
        return self.evaluate_file(f"{self.dirname}/author_objects.txt")
    # def candidate_ids(self):
    #     return eval(self.initial_candidate_employee_ids)
    def candidate_ids(self):
        return(self.initial_candidate_employee_ids.split(","))
    def janelians_bool(self):
        return(eval(self.janelians))
    def id_result(self):
        id_results_from_file = []
        for id in self.initial_candidate_employee_ids.split(","):
            id_results_from_file.append( self.read_file(f'{self.dirname}/id_result_{id}.txt') )
        return id_results_from_file
    # def id_result(self):
    #     id_results_from_file = []
    #     for id in eval(self.initial_candidate_employee_ids):
    #         id_results_from_file.append( self.evaluate_file(f'{self.dirname}/id_result_{id}.txt') )
    #     return id_results_from_file
    def parse_proposed_guesses(self):
        return [s.split('|') for s in self.proposed_guesses.split(';')]
    def guesses(self):
        return self.read_file(f"{self.dirname}/guesses.txt")

    def evaluate_file(self, filename):
        """
        Read a file and return a data structure that is just what you would get from a particular doi_common or jrc_common command.
        """
        with open(filename, 'r') as inF:
            return eval(inF.readlines()[0].rstrip('\n'))
    def read_file(self, filename):
        with open (filename, 'r') as inF:
            return inF.readlines()[0].rstrip('\n')

