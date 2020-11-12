import tweepy
import datetime
import pandas as pd

def gettwitterdata(keyword,dfile):

    # 変数
    count = 0

    # Twitter APIを使用するためのConsumerキー、アクセストークン設定
    Consumer_key = '3opn4Ok00sMsQnf79OXauhKoF'
    Consumer_secret = '1KQH6nPHP28fHQckF097KCtOKdJ5wErD9U2PvSM10cZYNdnIRk'
    Access_token = '5309772-IsvjEcjFakWgPv3MZKjgbmShmfrb9DEOZpzIGucsRP'
    Access_secret = 'N104BRGYXmOr64HMhMPobWxtf8DVq1RT9Xe0wx6S7cCsl'

    # 認証
    auth = tweepy.OAuthHandler(Consumer_key, Consumer_secret)
    auth.set_access_token(Access_token, Access_secret)

    api = tweepy.API(auth, wait_on_rate_limit = True)

    # 検索キーワード設定 ※デフォルトでリツイートは除外
    q = keyword + " -RT"

    # つぶやきを格納するDataFrame
    column_name = [
        "user_id",
        "user_name",
        "follower",
        "following",
        "tweet",
        "date",
    ]
    df_tweets = pd.DataFrame([], columns=column_name)

    # カーソルを使用してデータ取得
    try:
        for tweet in tweepy.Cursor(api.search, q=q, count=15, lang='ja', tweet_mode='extended').items():

            # つぶやき時間がUTCのため、JSTに変換  ※デバッグ用のコード
            #jsttime = tweet.created_at + datetime.timedelta(hours=9)
            #print(jsttime)

            # つぶやきテキスト(FULL)を取得
            se_tweet = pd.Series([
                tweet.user.screen_name,
                tweet.user.name,
                tweet.user.followers_count,
                tweet.user.friends_count,
                tweet.full_text.replace("\n"," "),
                tweet.created_at + datetime.timedelta(hours=9)
            ], index=df_tweets.columns)
            df_tweets = df_tweets.append(se_tweet, ignore_index=True)

            count += 1
            if count % 500 == 0:
                print("count: {:,}".format(count))

    except tweepy.error.TweepError as e:
        print (e.reason)

    # ファイル出力
    df_tweets.to_csv(dfile + ".tsv", sep="\t", encoding="utf-8", index=False)


if __name__ == '__main__':

    # 検索キーワードを入力
    print ('====== Enter Serch KeyWord   =====')
    keyword = input('>  ')

    # 出力ファイル名を入力(相対パス or 絶対パス)
    print ('====== Enter Tweet Data file =====')
    dfile = input('>  ')

    gettwitterdata(keyword, dfile)
