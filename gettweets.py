import mysql.connector
import json
import config
import requests
import base64
import sys
import getopt
import configparser
from requests_oauthlib import OAuth1Session
import urllib



#tw = OAuth1Session(config.CONSUMER_KEY, config.CONSUMER_SECRET, \
#                   config.ACCESS_TOKEN, config.ACCESS_SECRET)
#
#EP_AUTH = "https://api.twitter.com/oauth2/token"
#idpw = config.CONSUMER_KEY + ":" + config.CONSUMER_SECRET
#print(idpw)
#auth_headers = {"Authorization" : "Basic ".encode() + base64.b64encode(idpw.encode()), \
#                "Content-Type" : "application/x-www-form-urlencoded;charset=UTF-8"}
#data = requests.post(EP_AUTH, data="grant_type=client_credentials", headers=auth_headers)
#print(data.text)
#request_token_url = "https://api.twitter.com/oauth/request_token"
#oauth = OAuth1Session(config.CONSUMER_KEY, client_secret=config.CONSUMER_SECRET)
#request_token_url = "https://api.twitter.com/oauth/request_token"
#fetch_response = oauth.fetch_request_token(request_token_url)
#resource_owner_key = fetch_response.get("oauth_token")
#resource_owner_secret = fetch_response.get("oauth_token_secret")
#base_authorization_url = "https://api.twitter.com/oauth/authorize"
#authorization_url = oauth.authorization_url(base_authorization_url)
#access_token_url = "https://api.twitter.com/oauth/access_token"
#oauth = OAuth1Session(
#    consumer_key,
#    client_secret=consumer_secret,
#    resource_owner_key=resource_owner_key,
##    resource_owner_secret=resource_owner_secret,
#    verifier=verifier,
#)
#oauth_tokens = oauth.fetch_access_token(access_token_url)
#access_token = oauth_tokens["oauth_token"]
#access_token_secret = oauth_tokens["oauth_token_secret"]

# Make the request
#oauth = OAuth1Session(
#    config.CONSUMER_KEY,
#    client_secret=config.CONSUMER_SECRET,
#    resource_owner_key=access_token,
#    resource_owner_secret=access_token_secret,
#)

conn = None
cur = None

# 設定を設定ファイルから読んで内部に保持するクラス
class Config:
    CONSUMER_KEY=""
    CONSUMER_SECRET=""
    BEARER_TOKEN=""
    ACCESS_TOKEN=""
    ACCESS_SECRET=""
    TARGET=""
    dbuser=""
    dbpass=""
    dbhost=""
    db=""
    toptweetonly = False
    recursive = False
    def __init__(self, section):
        config_ini = configparser.ConfigParser()
        config_ini.read("config.ini", encoding = "utf-8")
        self.CONSUMER_KEY = config_ini["general"]["CONSUMER_KEY"]
        self.CONSUMER_SECRET = config_ini["general"]["CONSUMER_SECRET"]
        self.BEARER_TOKEN = config_ini["general"]["BEARER_TOKEN"]
        self.ACCESS_TOKEN = config_ini["general"]["ACCESS_TOKEN"]
        self.ACCESS_SECRET = config_ini["general"]["ACCESS_SECRET"]
        self.TARGET = config_ini[section]["TARGET"]
        self.dbuser = config_ini[section]["dbuser"]
        self.dbpass = config_ini[section]["dbpass"]
        self.dbhost = config_ini[section]["dbhost"]
        self.db = config_ini[section]["db"]
        if config_ini[section]["toptweetonly"] == "True":
            self.toptweetonly = True
        if config_ini[section]["recursive"] == "True":
            self.recursive = True

# ツイートのJSONから主要なフィールドを抽出し，テキストは引用符の処置をする
class TweetData:
    id = None
    created_at = None
    text = None
    conversation_id = None
    replied_to_author = None
    replied_to_id = None
    author_id = None
    json_data = None
    def __init__(self, json_data):
        print(json_data)
        self.id = json_data["id"]
        self.created_at = json_data["created_at"]
        self.text = postedit_json(json_data["text"])
        self.conversation_id = json_data["conversation_id"]
        self.replied_to_author = None # ここがわからん
        if 'referenced_tweets' in json_data:
            self.replied_to_id = json_data["referenced_tweets"][0]['id']
        else:
            self.replied_to_id = None
        self.author_id = json_data["author_id"]
        self.json_data = json_data


insert_tweet_sql = '''insert into tweet (id, created_at, twtext, conversation_id, replied_to_author, replied_to_id, author_id, json_data)  values (?, str_to_date(?, "%Y-%m-%dT%T.%fZ"), ?, ?, ?, ?, ?, ?)'''
insert_metrics_sql = '''insert into tweetinfo (id, rt, fav, quote, reply)  values (?, ?, ?, ?, ?);'''
update_metrics_sql = '''update tweetinfo set rt=?, fav=?, quote=?, reply=? where id=?;'''

def dbsetup():
    conn = mysql.connector.connect(
        user=config.dbuser, passwd=config.dbpass, host=config.dbhost, db=config.db
    )
    cur = conn.cursor(prepared=True)
    return conn, cur

# 'を$single-quote$, \"を$double-quote$にする
def preedit_json(before):
    return before.replace("'", "$single-quote$"). replace('\\"', "$double-quote$")

# preedit_jsonの逆
def postedit_json(before):
    return before.replace("$single-quote$", "'"). replace("$double-quote$", '\\"')

# PythonのJSONの引用符は"だがMySQLが受け付けないので'に変換するのとpreedit_jsonの逆で戻す
def edit_json(before):
    repquote =  postedit_json(str(before). replace("'", '"'))

    # True, Falseの置換
    bvalue = repquote.replace(': False', ': "False"').replace(': True', ': "True"')
    print("edit_json=", bvalue)
    return bvalue

# DBにツイートとツイート関連情報を追加する
def dbinsert(td):
    # ツイート本体
    json = edit_json(td.json_data)
    cur.execute(insert_tweet_sql, (td.id, td.created_at, td.text, td.conversation_id, td.replied_to_id, \
                                   td.replied_to_author, td.author_id, edit_json(td.json_data)))
    conn.commit()

    # ツイート統計情報
    metrics = td.json_data['public_metrics']
    retweet_count = 0 if "retweet_count" not in metrics else metrics["retweet_count"]
    like_count = 0 if "like_count" not in metrics else metrics["like_count"]
    quote = 0 if "quote_count" not in metrics else metrics["quote_count"]
    # リプとかだったらここで-1でなくてなんか入れる
    cur.execute(insert_metrics_sql, (td.id, retweet_count, like_count, quote, -1))
    conn.commit()

    # ハッシュタグ
def metrics_update(td):
    metrics = td.json_data['public_metrics']
    retweet_count = 0 if "retweet_count" not in metrics else metrics["retweet_count"]
    like_count = 0 if "like_count" not in metrics else metrics["like_count"]
    quote = 0 if "quote_count" not in metrics else metrics["quote_count"]
    # リプとかだったらここで-1でなくてなんか入れる
    cur.execute(update_metrics_sql, (retweet_count, like_count, quote, -1, td.id))
    conn.commit()

def dbfinish(cur, conn):
    cur.close
    conn.close

def getQueryURL(filter, config, next_token):
    return "https://api.twitter.com/2/tweets/search/recent?query=" + filter + (" -is:Reply " if config.toptweetonly else " ") + " -is:retweet" + ("" if next_token == "" else "&next_token="+next_token) 

def getTweetURL(ids):
    return "https://api.twitter.com/2/tweets?ids=" + ids

def add_tweet(item, config):
    td = TweetData(item)
    if is_first_tweet(td.id) is None:
        dbinsert(td)
        # メンションがあればメンション先をテーブルに登録する
        mention_users = get_mention_target_user(item)
        if mention_users is not None:
            for u in mention_users:
                add_userid_entry(u, td.id)
        # ハッシュタグがあればハッシュタグをテーブルに登録する
        hashtags = get_hashtag(item)
        if hashtags is not None:
            for t in hashtags:
                add_hashtag_entry(t, td.id)
    else: # 統計情報更新
        metrics_update(td)

def getTweetTree(id, config):
    endpoint = getQueryURL("conversation_id:" + id, config, "")
    print("endpoint=", endpoint)
    while True:
        data = requests.get(endpoint, params=params, headers=headers)
        obj = json.loads(preedit_json(data.text))
        for item in obj['data']:
            add_tweet(item, config)
        if 'next_token' not in obj['meta']:
            break
        next_token = obj['meta']['next_token']
        endpoint = getQueryURL("conversation_id:"+id, config, next_token)
        print("next endpoint=", endpoint) 


def get_mention_target_user(obj):
    users = []
    if 'entities' not in obj:
        return None
    if 'mentions' not in obj['entities']:
        return None
    for it in obj['entities']['mentions']:
        users.append(it['username'])
    return users

def get_hashtag(obj):
    tags = []
    if 'entities' not in obj:
        return None
    if 'hashtags' not in obj['entities']:
        return None
    for it in obj['entities']['hashtags']:
        tags.append(it['tag'])
    return tags

def is_first_tweet(id):
    cur.execute("select id from tweet where id = ?", (id,))
    result = cur.fetchone()
    if result is not None:
        return result[0]
    else:
        return None

def is_first_userid(user):
    cur.execute("select id from userid where userid = ?", (user,))
    result = cur.fetchone()
    if result is not None:
        return result[0]
    else:
        return None

def add_userid_entry(user, tweetid):
    userid = is_first_userid(user)
    if userid is None:
        cur.execute("insert into userid(id,userid) values(?,?)", (0, user))
        conn.commit()
        userid = is_first_userid(user)
    cur.execute("insert into userid_table(id, tweetid, userid) values (?,?,?)", (0, tweetid, userid))

def is_first_hashtag(hashtag):
    cur.execute("select id from hashtag where tagtext = ?", (hashtag,))
    result = cur.fetchone()
    if result is not None:
        return result[0]
    else:
        return None

def add_hashtag_entry(hashtag, tweetid):
    tagid = is_first_hashtag(hashtag)
    if tagid is None:
        cur.execute("insert into hashtag(id,tagtext) values(?,?)", (0, hashtag))
        conn.commit()
        tagid = is_first_hashtag(hashtag)
    cur.execute("insert into hashtag_table(id, tweetid, hashtagid) values(?,?,?)", (0, tweetid, tagid))

params = { \
    "tweet.fields":"attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld", \
          "media.fields": "preview_image_url,type,url", \
          "expansions": "attachments.media_keys"}

#params = {"tweet.fields": "attachment"}
#params = {}

if __name__ == "__main__":
    iopt = False
    sopt = "neko"
    config = None


    optlist, args = getopt.getopt(sys.argv[1:], "i:s:")
    for opt, arg in optlist:
        # 指定ツイートの情報表示
        if opt == "-i": 
            iopt = True
        # ヘルプ
        elif opt == "-s":
            sopt = arg
        elif opt == "-r":
            recursive = True
        else:
            print("gettweets.py [-i id1,id2,…|-s <section>]")
            sys.exit(0)
    config = Config(sopt)
    conn, cur = dbsetup()
    headers = {"Authorization": "Bearer " + config.BEARER_TOKEN}

    if iopt :
        endpoint = getTweetURL(arg)
    else:
        endpoint = getQueryURL(urllib.parse.quote(config.TARGET.encode(encoding='utf-8')), \
                               config, "")

    while True:
        print(headers)
        print(endpoint)
        alldata = requests.get(endpoint, params=params, headers=headers)
        obj = json.loads(preedit_json(alldata.text))
        print(alldata.text)
        data = obj['data']
        meta = obj['meta']

        for item in data:
            if config.recursive:
                getTweetTree(item['conversation_id'], config, recursive)
            else:
                add_tweet(item, config)
        if 'next_token' not in obj['meta']:
            break
        next_token = obj['meta']['next_token']
        endpoint = getQueryURL(urllib.parse.quote(config.TARGET.encode(encoding='utf-8')), \
                               config, next_token)
    dbfinish(conn, cur)