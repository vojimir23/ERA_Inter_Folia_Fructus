from argparse import ArgumentParser

parser = ArgumentParser(prog="era", usage="Provide a path to the excel file to convert.")
parser.add_argument("path")
args = parser.parse_args()
