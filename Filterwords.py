import os

from Propername_detect import mark_proper_names, mark_typonoyms, mark_organization_names, mark_organization_names_with_list
from Dateparser import parsetime
import json
from tqdm import tqdm
import pandas as pd


def filter_words(word_dir_csv: str, json_article_dir: str, lower_case=False, only_sup_lang=True, out_dir: str = ""):
    words = {}
    words_time_out = []
    words_proper_out = []
    words_topo = []
    words_csv = pd.read_csv(word_dir_csv, sep="\t")
    with open(json_article_dir, "r", encoding="UTF-8") as list_file:
        qid_words = list_file.read().split("\n")
    for row in words_csv.iterrows():
        if only_sup_lang:
            if row[1]["sup_all_lang"]:
                words[str(row[1]["en"])] = {}
        else:
            words[str(row[1]["en"])] = {}

    for word in tqdm(words, desc="Mark all times"):
        time_detect = parsetime(word)[2]
        if time_detect:
            words[word]["No_time"] = False
        else:
            words[word]["No_time"] = True

    words_out = mark_organization_names_with_list(words, qid_words)
    words_out = mark_proper_names(words_out, lower_case)
    words_out = mark_typonoyms(words_out, lower_case)

    for word in words_out:
        if "No_proper_name" not in words_out[word]:
            words_out[word]["No_proper_name"] = True
        if "No_toponym" not in words_out[word]:
            words_out[word]["No_toponym"] = True

    for row in words_csv.iterrows():
        word_en = str(row[1]["en"])
        if word_en in words_out:
            words_time_out.append(words_out[word_en]["No_time"])
            words_proper_out.append(words_out[word_en]["No_proper_name"])
            words_topo.append(words_out[word_en]["No_toponym"])
        else:
            words_time_out.append("False Not controlled")
            words_proper_out.append("False Not controlled")
            words_topo.append("False Not controlled")
    words_csv["No_time"] = words_time_out
    words_csv["No_proper_name"] = words_proper_out
    words_csv["No_toponym"] = words_topo
    words_csv = words_csv.sort_values(by=["sup_all_lang", "No_time", "No_proper_name", "No_toponym", "links_combined", "ddc-combined", "Article"],
                                      ascending=[False, False, False, False, False, False, True])
    len_words = words_csv.shape[1]
    print(len_words)
    words_order = [0, 1, len_words - 3, len_words - 2, len_words - 1] + list(range(2, len_words - 3))
    words_csv = words_csv.iloc[:, words_order]
    if out_dir != "":
        os.makedirs(os.path.dirname(out_dir), exist_ok=True)
        words_csv.to_csv(out_dir, sep="\t", header=True, index=False)

    return words_csv


def take_best_ddc(word_csv: pd, ddc_limit: float, langs: list, out_dir):
    columns = word_csv.columns.values.tolist()
    dict_out_csv = {}
    for col in columns:
        dict_out_csv[col] = []
    for row in word_csv.iterrows():
        sup_all_lang = row[1]["sup_all_lang"]
        no_time = row[1]["No_time"]
        no_prop = row[1]["No_proper_name"]
        no_topo = row[1]["No_toponym"]
        count = 0
        ddc = 0
        if sup_all_lang and no_topo and no_time and no_prop:
            for lang in langs:
                ddc_i = row[1][f"ddc2_{lang}"]
                if ddc_i >= 0.0:
                    ddc += ddc_i
                    count += 1
            if ddc > 0.0:
                ddc_o = ddc / count
                if ddc_o >= ddc_limit:
                    for col in columns:
                        dict_out_csv[col].append(row[1][col])
    df = pd.DataFrame(dict_out_csv)
    df = df.sort_values(by=["sup_all_lang", "No_time", "No_proper_name", "No_toponym", "links_combined", "ddc-combined", "Article"], ascending=[False, False, False, False, False, False, True])
    if out_dir != "":
        os.makedirs(os.path.dirname(out_dir), exist_ok=True)
        df.to_csv(out_dir, sep="\t", header=True, index=False)


def get_all_organization_names(word_dir_csv: str, json_article_dir: str, out_dir: str, only_sup_lang=True):
    words = {}
    words_csv = pd.read_csv(word_dir_csv, sep="\t")
    with open(json_article_dir, "r", encoding="UTF-8") as json_file:
        qid_words = json.load(json_file)
    for row in words_csv.iterrows():
        if only_sup_lang:
            if row[1]["sup_all_lang"]:
                words[str(row[1]["en"])] = {}
        else:
            words[str(row[1]["en"])] = {}
    word_out, all_organization = mark_organization_names(words, qid_words)
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)
    with open(out_dir, "w", encoding="UTF-8") as txt_file:
        txt_file.write("\n".join(all_organization))


if __name__ == "__main__":
    field_oecds = ["Mathematics", "Computer_and_information_sciences", "Economics_and_business", "Physical_sciences", "Biological_sciences", "Languages", "Literature", "Law"]
    limit_ddc = 0.75
    for field_oecd in field_oecds:
        version = "v4"
        all_langs = ['en', 'de', 'fr', 'ru', 'es', 'it', 'arz', 'pl', 'ja', 'zh', 'ar', 'uk', "pt"]
        data_name = f"languagesAll_expanded_ddc2_incominglinks_{version}"
        base_dir = f"/mnt/corpora2/projects/bagci/Arxiv/multilingual_data/baumartz/OECD-wikipedia_new_{version}/{field_oecd}"
        csv_dir = f"{base_dir}/{data_name}.csv"
        list_dir = f"{base_dir}/{field_oecd}_Organization_Names.txt"
        csv_out = f"{base_dir}/{data_name}_filtered.csv"
        csv_out_over_fac = f"{base_dir}/{data_name}_filtered_over_limit.csv"
        csv_data = filter_words(csv_dir, list_dir, out_dir=csv_out, only_sup_lang=False)
        take_best_ddc(csv_data, limit_ddc, all_langs, csv_out_over_fac)
    # for field_oecd in ["Literature", "Law"]:
    #     all_langs = ['en', 'de', 'fr', 'ru', 'es', 'it', 'arz', 'pl', 'ja', 'zh', 'ar', 'uk', "pt"]
    #     version = "v4"
    #     data_name = f"languagesAll_expanded_ddc2_incominglinks_{version}"
    #     base_dir = f"/mnt/corpora2/projects/bagci/Arxiv/multilingual_data/baumartz/OECD-wikipedia_new_{version}/{field_oecd}"
    #     out_name_dir = f"{base_dir}/{field_oecd}_Organization_Names.txt"
    #     csv_dir = f"{base_dir}/{data_name}.csv"
    #     json_dir = f"{base_dir}/wanted_language/languageWanted_expanded_ddc2_incominglinks_{version}.json"
    #     get_all_organization_names(csv_dir, json_dir, out_name_dir)
