import numpy
import pandas

def run_all_districts(state):
    """
    Loops through all of the congressional districts of a state and calls gen_report(state, cd) on them

    Parameter state: str of desired state to examine
    """
    
    data = pandas.read_table('state'+'.txt', sep='|',header =None,names=['BLOCKID','DISTRICT'])
    dists = data['DISTRICT'].unique()
    for cd in dists:
        gen_report(state, cd)