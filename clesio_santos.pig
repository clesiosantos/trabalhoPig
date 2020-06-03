-- Script Pig Clesio Santos
-- limpar logs 

fs -rm -f -r -R qtd_user_rating.txt;
fs -rm -f -r -R user_max_rate_date.txt
fs -rm -f -r -R qtd_movie_by_gender.txt
fs -rm -f -r -R qtd_movie_by_tag.txt
fs -rm -f -r -R top_10_movie.txt
fs -rm -f -r -R qtd_movie_gender.txt
fs -rm -f -r -R bottom_10_movie.txt
fs -rm -f -r -R rank_rating.txt

-- CARREGA OS ARQUIVOS PARA MANIPULAÇÃO
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
ratings = LOAD 'ratings.csv' USING CSVExcelStorage() AS (userId:int,movieId:int,rating:float,timestamp:long);
movies = LOAD 'movies.csv' USING CSVExcelStorage() AS (movieId:int ,title: chararray,genres: chararray);
tags = LOAD 'tags.csv' USING CSVExcelStorage() AS (userId:int, movieId:int,tag: chararray,timestamp:long);
	

-- QUESTAO 1 - Quantidade de usuários distintos que fizeram avaliação de filmes (DUMP)
ratings_userid_1 = FOREACH ratings GENERATE $0;
d_ratings_userid_1 = DISTINCT ratings_userid_1;
g_ratings_userid_1 = GROUP d_ratings_userid_1 ALL;
count_distinct_userid_1 = FOREACH g_ratings_userid_1 GENERATE COUNT(d_ratings_userid_1);
DUMP count_distinct_userid_1; 

-- QUESTAO 2 - quantidade de avaliações por usuário (qtd_user_rating.txt)
g_ratings_userid_2 = GROUP ratings BY userId;
c_userid_total_2 = FOREACH g_ratings_userid_2 GENERATE group AS userId, COUNT($1) AS count;
STORE c_userid_total_2 INTO 'qtd_user_rating.txt' USING PigStorage(',');

-- QUESTAO 3 - O usuário e a data de sua última avaliação (user_max_rate_date.txt)
ratings_3 = FILTER ratings BY (userId>0);
ratings_time_3 = FOREACH ratings_3 GENERATE userId, ((long) timestamp*1000) as time;
ratings_date_3 = FOREACH ratings_time_3 GENERATE userId,ToDate(time) as time;
ratings_group_3 = GROUP ratings_date_3 BY userId;
ratings_max_3 = FOREACH ratings_group_3 GENERATE  group, MAX(ratings_date_3.time);
STORE ratings_max_3 INTO 'user_max_rate_date.txt' USING PigStorage(',');

-- QUESTAO 4 - Quantidade de filmes por gênero (qtd_movie_by_gender.txt)
movies_split_4 = FOREACH movies GENERATE movieId as movieId , FLATTEN(STRSPLITTOBAG(genres, '\\|')) AS genres;
g_movies_4 = GROUP movies_split_4 BY genres;
count_moveis_gender_4 = FOREACH g_movies_4 GENERATE group as genres, COUNT(movies_split_4.movieId);
STORE count_moveis_gender_4 INTO 'qtd_movie_by_gender.txt' USING PigStorage(',');

-- QUESTAO 05 - Lista distinta dos gêneros dos filmes (DUMP)
genres_split_5 = FOREACH movies GENERATE FLATTEN(STRSPLITTOBAG(genres, '\\|')) AS genres;
movie_gender_distinct_5 = DISTINCT genres_split_5;
DUMP movie_gender_distinct_5;

-- QUESTAO 06 - Quantidade de filmes pelas 10 maiores tags (qtd_movie_by_tag.txt)
g_tag_6 = GROUP tags BY tag;
count_tag_6 = FOREACH g_tag_6 GENERATE group as tag, COUNT(tags.tag) AS qtd_tag;
order_tags_6 = ORDER count_tag_6 BY qtd_tag DESC;
top10_tags_6 = LIMIT order_tags_6 10;
STORE top10_tags_6 INTO 'qtd_movie_by_tag.txt' USING PigStorage(',');

-- QUESTAO 07 - Lista de nomes dos 10 filmes mais bem avaliados, quantidade de avaliações e sua nota média (top_10_movie.txt)
g_ratings_7 = GROUP ratings BY movieId;
g_ratings_Media_7 = FOREACH g_ratings_7 GENERATE group as movieId, AVG(ratings.rating) as RatingMedia, COUNT(ratings) as countRatings;
f_moveis_7 = FOREACH movies GENERATE movieId, title;
j_movie_ratingstitle_7 = JOIN g_ratings_Media_7 BY movieId, f_moveis_7 BY movieId;
f_movie_title_ratings_7 = FOREACH j_movie_ratingstitle_7 GENERATE f_moveis_7::title AS title, g_ratings_Media_7::countRatings as countRatings, g_ratings_Media_7::RatingMedia as RatingMedia;
f_movie_title_ratings_order_7 = Order f_movie_title_ratings_7 BY RatingMedia DESC, countRatings DESC;
top10_7 = LIMIT f_movie_title_ratings_order_7 10;
STORE top10_7 INTO 'top_10_movie.txt' USING PigStorage(',');


-- Questão 8 - Lista dos filmes e a quantidade de gêneros de cada um, ordenando descrescente pela quantidade  (qtd_movie_gender.txt)
f_moviesTitle_8 = FOREACH movies GENERATE movieId, title;
f_moviegenres_8 = FOREACH movies GENERATE movieId as movieId , FLATTEN(STRSPLITTOBAG(genres, '\\|')) AS genres;
g_moviegenres_8 = GROUP f_moviegenres_8 BY movieId;
c_moviegenres_8 = FOREACH g_moviegenres_8 GENERATE group as movieId, COUNT(f_moviegenres_8.genres) as countGenres;
j_countitlegender_8 = JOIN c_moviegenres_8 BY movieId, f_moviesTitle_8 BY movieId;
c_movietitlegender_8 = FOREACH j_countitlegender_8 GENERATE f_moviesTitle_8::title AS title, c_moviegenres_8::countGenres as countGenres;
order_title_gender_8 = Order c_movietitlegender_8 BY countGenres DESC;
STORE order_title_gender_8 INTO 'qtd_movie_gender.txt' USING PigStorage(',');


-- QUESTAO 9 - Lista de nomes dos 10 piores filmes, a quantidade distinta de usuários que avaliaram e sua nota média (bottom_10_movie.txt)
f_moviesTitle_9 = FOREACH movies GENERATE movieId, title;
g_ratings_9 = GROUP ratings BY movieId;
f_ratings_avg_down_9 = foreach g_ratings_9 { 
    userId = DISTINCT ratings.userId;
    generate group as movieId, COUNT(userId) as countUserId, AVG(ratings.rating) as RatingMedia;
};

j_movies_title_9 = JOIN f_ratings_avg_down_9 BY movieId, f_moviesTitle_9 BY movieId;
f_down10_movie_9 = FOREACH j_movies_title_9 GENERATE f_moviesTitle_9::title AS title,f_ratings_avg_down_9::countUserId as countDistinctUser, f_ratings_avg_down_9::RatingMedia as RatingMedia;
f_down10_order_9 = ORDER f_down10_movie_9 BY RatingMedia ASC;
down10_9 = LIMIT f_down10_order_9 10;
STORE down10_9 INTO 'bottom_10_movie.txt' USING PigStorage(',');

-- Questão 10 - Ranking dos 10 gêneros mais bem avaliados, com quantidade de avaliações e nota média (rank_rating.txt)	
g_ratings_10 = GROUP ratings BY movieId;
f_movies_gender_10 = FOREACH movies GENERATE movieId as movieId , FLATTEN(STRSPLITTOBAG(genres, '\\|')) AS genres;
g_ratings_AVG_10 = FOREACH g_ratings_10 GENERATE group as movieId, AVG(ratings.rating) as RatingMedia, COUNT(ratings.rating) AS ratingCount;
f_movies_gender_RatingsAVG_10 = JOIN f_movies_gender_10 BY movieId, g_ratings_AVG_10 BY movieId;
j_f_movies_gender_10Ratings_10 = GROUP f_movies_gender_RatingsAVG_10 BY f_movies_gender_10::genres;
f_gender_ratings_AVG_10 = FOREACH j_f_movies_gender_10Ratings_10 GENERATE group as gender, COUNT(f_movies_gender_RatingsAVG_10.g_ratings_AVG_10::ratingCount) as ratingCount, AVG(f_movies_gender_RatingsAVG_10.g_ratings_AVG_10::RatingMedia) AS f_gender_ratings_AVG_10;
f_gender_ratings_AVG_10 = FOREACH j_f_movies_gender_10Ratings_10 GENERATE group as gender, COUNT(f_movies_gender_RatingsAVG_10.g_ratings_AVG_10::ratingCount) as ratingCount;
order_top10_ratings = ORDER f_gender_ratings_AVG_10 by ratingCount DESC;
top10_rank_10 = LIMIT order_top10_ratings 10;
STORE top10_rank_10 INTO 'rank_rating.txt' USING PigStorage(',');

