-- 投稿本体 + 投稿後変わらない情報(jsonを除く)
create table tweet (
    id varchar(20) not null primary key,
    created_at datetime,
    twtext varchar(200),
    conversation_id varchar(20),
    -- どのツイートへのリプか
    replied_to_id varchar(20),
    replied_to_author varchar(20),
    -- 
    author_id varchar(20),
    json_data json
);

-- 投稿後変わる情報
create table tweetinfo (
    id varchar(20) not null primary key,
    rt int,
    fav int,
    quote int,
    reply int
);

-- ハッシュタグの定義
create table hashtag (
    id int auto_increment not null primary key,
    tagtext varchar(200)
);

-- ハッシュタグの利用状況
create table hashtag_table (
    id int auto_increment not null primary key,
    tweetid varchar(20),
    hashtagid int,
    foreign key fk_tweet(tweetid) references tweet(id),
    foreign key fk_hashtag(hashtagid) references hashtag(id)
);

-- ユーザIDの定義
create table userid (
    id int auto_increment not null primary key,
    userid varchar(16)
);

---- ユーザIDの利用状況
create table userid_table (
    id int auto_increment not null primary key ,
    tweetid varchar(20),
    userid int,
    foreign key fk_tweet(tweetid) references tweet(id),
    foreign key fk_userid(userid) references userid(id)
);
