"""
Some handy objects that appear in multiple test scripts.
"""

class TestCase():
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

def evaluate_file(filename):
    """
    Read a file and return a data structure that is just what you would get from a particular doi_common or jrc_common command.
    """
    with open(filename, 'r') as inF:
        return(eval(inF.readlines()[0].rstrip('\n')))

def read_config(testname):
    config_file_obj = open(f'{testname}/config.txt', 'r')
    config_dict = {line.split(':')[0]: line.split(':')[1].rstrip('\n') for line in config_file_obj.readlines()}
    config_file_obj.close()
    return(config_dict)