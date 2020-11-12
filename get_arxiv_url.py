import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)


def get_page_data(html_text):

    pattern = r"^\[[0-9.]+\](.*)"
    soup = BeautifulSoup(html_text, "html.parser")

    # Page Title
    title = soup.find("title")
    if title is not None and title:
        #print(title)
        if re.match(pattern, title.string):
            title = re.match(pattern, title.string).group(1).lstrip(" ")
        else:
            title = title.string
        #print(title)
    else:
        title = ""

    # Abstract
    abstract = soup.select_one(".abstract").get_text()
    #print(abstract)
    if abstract is not None and abstract:
        abstract = abstract.replace("\n", "").replace("Abstract:", "").lstrip()
    else:
        abstract = ""
    #print(abstract)

    # Subject
    subjects = soup.select_one(".metatable .subjects").get_text()
    #print(subjects)
    if subjects is not None and subjects:
        subjects = subjects.lstrip().split(";")
    else:
        subjects = ""
    #print(subjects)

    return [title, abstract, subjects]


def get_arxiv_url():

    # カウント用変数
    start_count = 150
    interval = 200
    end_count = start_count + interval

    # ファイル読み込み
    file_name = "twitter_timeline.tsv"
    pd_data = pd.read_table(file_name)

    # TweetからHTMLを抜き出す
    #str_tweets = pd_data["tweet"].values
    #str_tweets = pd_data["tweet"][start_count:].values
    str_tweets = pd_data["tweet"][start_count:end_count].values
    for tweet in str_tweets:

        str_url, str_title, str_abstract, str_p_sub, str_s_sub = "", "", "", "", ""
        # URLを取得
        res_match = re.findall(r"((?:http|https)://(?:[0-9a-zA-Z./])+)", tweet)
        #print(res_match)

        match_flg = False
        for arxiv_url in res_match:
            #print(arxiv_url)
            res = requests.get(arxiv_url.rstrip(" "), verify=False)

            #print(res.url)
            if re.match(r".*arxiv.org\/(abs|pdf).*", res.url):
                match_flg = True
                # リンクがPDFファイルの場合、URLを取得しなおす
                if re.match(r".*.pdf$", res.url):
                    url = res.url.replace(".pdf","").replace("pdf","abs")

                    # URLを変更して再度GETリクエスト
                    res2 = requests.get(url)
                    #print(res2.url)
                    str_url += res2.url + ", "
                    l_res = get_page_data(res2.text)

                # 通常URL
                else:
                    str_url += res.url + ", "
                    l_res = get_page_data(res.text)

                str_title += l_res[0] + ", "
                str_abstract += l_res[1] + ", "
                str_p_sub += l_res[2][0] + ", "
                for i, subject in enumerate(l_res[2]):
                    if i != 0:
                        str_s_sub += l_res[2][i].lstrip() + ";"
                str_s_sub = str_s_sub.rstrip(";") + ", "

        if match_flg:
            pd_data.at[start_count, "arxiv_url"] = str_url.rstrip(", ")
            pd_data.at[start_count, "arxiv_title"] = str_title.rstrip(", ")
            pd_data.at[start_count, "arxiv_abstract"] = str_abstract.rstrip(", ")
            pd_data.at[start_count, "arxiv_p_subject"] = str_p_sub.rstrip(", ")
            pd_data.at[start_count, "arxiv_s_subject"] = str_s_sub.rstrip(", ")

        start_count += 1
        if start_count % 10 == 0:
            print("Done :", start_count)
    
    #print(pd_data.head())
    pd_data.to_csv("twitter_timeline_add.tsv", sep="\t", index=False)


if __name__ == "__main__":
    get_arxiv_url()
