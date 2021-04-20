import csv
import re
from sys import argv

"""
We get files like this:

E:\GA010-1956\PDF\A-3111-ADD-1-F.pdf	
E:\GA033-1979\PDF\A-33-550-S.pdf	
E:\GA033-1979\PDF\A-33-551-ADD-1-S.pdf	
...
A-C-5-1004-R.pdf	GA018-1963
A-C-5-1197-R.pdf	GA023-1968
A-C-5-657-F.pdf	GA011-1956

And need to transform the contents into something like this:

A-C-5-658-E.pdf	GA013-1958	A/C.5/658	E
A-C-5-658-F.pdf	GA013-1958	A/C.5/658	F
A-C-5-659-ADD-1-E.pdf	GA013-1958	A/C.5/659/ADD.1	E
A-C-5-659-ADD-1-F.pdf	GA013-1958	A/C.5/659/ADD.1	F

Some of the - must be replaced by / and some replaced by . while the dash 
separating the symbol from language will be replaced by a tab character.

A cat and gawk chain that approaches this:
cat june2020.tsv |cut -d$'\t' -f1 |awk -F'\.pdf' '{print $1}' \
    |gawk '{print gensub(/-([ACEFRS]$)/, "\t\\1", "g", $1 );}' \
    |gawk '{print gensub(/^([AES])-/, "\\1/", "g", $1 "\t" $2);}' \
    |gawk '{print gensub(/[-|/](ADD|CORR|SC|SR|L|AC|W|REV|SUB|CN|C|WG|CRP)-/, "/\\1.", "g", $1 "\t" $2);}' \
    |gawk '{print gensub(/-/, "/", "g", $1 "\t" $2);}' 

These steps are reproduced below.
"""

def parse_symbol(input_string):
    symbol = None
    sym_lang = input_string.split('.')[0]
    sym = re.sub("-[ACEFRS]$", "", sym_lang)
    built_sym_1 = re.sub("^([AES])-", "\\1/", sym)
    built_sym_2 = re.sub("[-|/](ADD|CORR|SC|SR|L|AC|W|REV|SUB|CN|C|WG|CRP)-", "/\\1.", built_sym_1)
    built_sym_3 = re.sub("-", "/", built_sym_2)
    return built_sym_3

try:
    parse_file = argv[1]

    with open(parse_file) as csvfile:
        this_reader = csv.reader(csvfile,delimiter='\t')
        for row in this_reader:
            #print(row)
            if "E:\\" in row[0]:
                # We're dealing with file path
                filename = row[0].split('\\')[-1]
                subfolder = row[0].split('\\')[1]
                
            else:
                # Hopefully we have filename \t folder
                filename = row[0]
                subfolder = row[1]
                #pass

            
            lang = filename.split('-')[-1].split('.')[0]
            if lang in ['A','C','E','F','R','S']:
                pass
            else:
                # Should there be a default language when none is provided?
                lang = 'E'
            symbol = parse_symbol(filename)
            print(f"{filename}\t{subfolder}\t{symbol}\t{lang}")

except IndexError:
    print("Error: Tab separated file required as the first argument.")