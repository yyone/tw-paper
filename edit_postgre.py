import pandas as pd
import datetime
import re
import psycopg2
import psycopg2.extras


################################
## シングルクォーテーションエスケープ
################################
def escape_singlequote(str):

    edit_str = ""
    for c in str:
        if c == "'":
            edit_str += "'" + c
        else:
            edit_str += c

    return edit_str


################################
## SQL（INSERT）作成
################################
def make_sql(paper):

    date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    sql =  "INSERT INTO papers "
    sql += "(url, title, abstract, p_subject, s_subject, create_at) "
    sql += "VALUES ("
    sql += "'" + escape_singlequote(str(paper['arxiv_url'])) + "',"
    sql += "'" + escape_singlequote(str(paper['arxiv_title'])) + "',"
    sql += "'" + escape_singlequote(str(paper['arxiv_abstract'])) + "',"
    sql += "'" + escape_singlequote(str(paper['arxiv_p_subject'])) + "',"
    sql += "'" + escape_singlequote(str(paper['arxiv_s_subject'])) + "',"
    sql += "'" + str(date) + "'"
    sql += ")"

    return sql


################################
## papersテーブル処理
################################
def update_papers_table():

    ################################
    ## DB接続
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        dbname="papers",
        user="postgres",
        password="secret"
    )
    cur = conn.cursor()


    ################################
    ## データ読み込み
    df_data = pd.read_table("data01.tsv")
    pattern = "arxiv_(url|title|abstract|subject|)"
    df_data = df_data.filter(regex=pattern)

    # 型変換
    column_name = [
        "arxiv_url",
        "arxiv_title",
        "arxiv_abstract",
        "arxiv_p_subject",
        "arxiv_s_subject"
    ]
    for column in column_name:
        df_data[column] = df_data[column].astype("str")

    print(df_data.head())

    # 複数urlのデータを分割
    df_data2 = pd.DataFrame(columns=column_name)
    for i, paper in df_data.iterrows():
        if paper["arxiv_url"].find(",") > 0:
            urls = paper["arxiv_url"].split(",")
            titles = paper["arxiv_title"].split(",")
            abstracts = paper["arxiv_abstract"].split(",")
            p_subjects = paper["arxiv_p_subject"].split(",")
            s_subjects = paper["arxiv_s_subject"].split(",")

            for url, title, abstract, p_sub, s_sub in zip(urls, titles, abstracts, p_subjects, s_subjects):
                df_data2 = df_data2.append(pd.Series([
                    url.lstrip(),
                    title.lstrip(),
                    abstract.lstrip(),
                    p_sub.lstrip(),
                    s_sub.lstrip()
                ], index=df_data2.columns), ignore_index=True)
        else:
            df_data2 = df_data2.append(pd.Series([
                paper["arxiv_url"],
                paper["arxiv_title"],
                paper["arxiv_abstract"],
                paper["arxiv_p_subject"],
                paper["arxiv_s_subject"],
            ], index=df_data2.columns), ignore_index=True)
    # print(df_data2.head())

    # 重複行を削除
    df_data2 = df_data2.drop_duplicates()


    ################################
    ## DB更新
    # 既存データ取得
    cur.execute("select url from papers")
    results = cur.fetchall()

    # データ数だけループ処理
    for i, paper in df_data2.iterrows():
        exec_flg = False

        # 論文URL以外は最初に除外する
        pattern = r".*arxiv.org\/(abs|pdf).*"
        if not re.match(pattern, paper["arxiv_url"]):
            continue

        # データが空の場合
        elif len(results) == 0:
            sql = make_sql(paper)
            exec_flg = True

        # データが存在する場合
        else:
            # 該当論文が存在しない場合のみINSERT
            match_flg = False
            for row in results:
                if row["url"] == paper["arxiv_url"]:
                    match_flg = True
                    break

            if not match_flg:
                sql = make_sql(paper)
                exec_flg = True
        
        # 登録
        if exec_flg:
            try:
                cur.execute(sql)
                conn.commit()
            except Exception as e:
                print("Exception : %s", e)
                conn.rollback()


    ## DBクローズ
    conn.close()


if __name__ == "__main__":
    update_papers_table()
