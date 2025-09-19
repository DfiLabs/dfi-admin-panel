import pandas as pd 
import numpy as np 

def convert_equity_name(name):
    name = name.replace(' ','_')
    name = name.replace('/','_')
    return name 