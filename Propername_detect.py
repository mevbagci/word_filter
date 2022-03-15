import tqdm
import gzip
from SPARQLWrapper import SPARQLWrapper, JSON

endpoint = "https://query.wikidata.org/sparql"
VERSION = '0.1'
user_agent = 'TTLDDC/' + VERSION + ' (https://www.texttechnologylab.org/) SPARQLWrapper/1.8.4 (rdflib.github.io/sparqlwrapper)'

sparql = SPARQLWrapper(endpoint, agent=user_agent)
sparql.setReturnFormat(JSON)

def mark_proper_names(index_words: dict, lower_case=False):
    counter = 0
    proper_name_dir = f"/mnt/corpora2/projects/bagci/Arxiv/textbooks/Economy/en/words/json/personennamen.csv_json.gz"
    with gzip.open(proper_name_dir, "rt", encoding="UTF-8") as out_file:
        for i in tqdm.tqdm(out_file.readlines(), desc="Mark all proper_names"):
            name_proper = i.split("\t")[0]
            if lower_case:
                name_proper = name_proper.lower()
            if name_proper in index_words:
                counter += 1
                index_words[name_proper]["No_proper_name"] = False
    print(f"Number of proper names found {counter} of {len(index_words)}")
    return index_words


def mark_typonoyms(index_words: dict, lower_case=False):
    counter = 0
    proper_name_dir = f"/mnt/corpora2/projects/bagci/Arxiv/multilingual_data/Toponymelist.csv_json.gz"
    with gzip.open(proper_name_dir, "rt", encoding="UTF-8") as out_file:
        for i in tqdm.tqdm(out_file.readlines(), desc="Mark all Toponyms"):
            name_proper = i.replace("\n", "")
            if lower_case:
                name_proper = name_proper.lower()
            if name_proper in index_words:
                counter += 1
                index_words[name_proper]["No_toponym"] = False
    proper_name_dir = f"/mnt/corpora2/projects/bagci/Arxiv/multilingual_data/geonames.txt"
    with open(proper_name_dir, "r", encoding="UTF-8") as out_file:
        for i in tqdm.tqdm(out_file.readlines(), desc="Mark all Toponyms"):
            name_proper = i.split("\t")[0]
            if lower_case:
                name_proper = name_proper.lower()
            if name_proper in index_words:
                counter += 1
                index_words[name_proper]["No_toponym"] = False
    print(f"Number of proper names found {counter} of {len(index_words)}")
    return index_words


def mark_organization_names(index_words: dict, qid_dict: dict):
    counter = 0
    all_organization = []
    for c, word in enumerate(tqdm.tqdm(index_words, desc="Mark all Organization-Names")):
        word_article = word.replace(" ", "_")
        if word_article in qid_dict:
            qid = qid_dict[word_article]["en"]["qid"].split("/")[-1]
            query = """SELECT DISTINCT ?class
                    WHERE
                    {{
                      wd:{} wdt:P31/wdt:P279* ?class .
                    }}""".format(qid)
            proper_name = False
            try:
                sparql.setQuery(query)
                results = sparql.query().convert()
                bad_qid = ["Q43229"]
                for result in results["results"]["bindings"]:
                    output_qid = result["class"]["value"].split("/")[-1]
                    if output_qid in bad_qid:
                        proper_name = True
                        break
                if proper_name:
                    counter += 1
                    index_words[word]["No_proper_name"] = False
                    all_organization.append(word)
            except Exception as e:
                print(qid)
                print(e)
    print(f"Number of Organization names found {counter} of {len(index_words)}")
    return index_words, all_organization


