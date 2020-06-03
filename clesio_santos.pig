-- Script Pig Clesio Santos
-- limpar logs 


fs -rm -f -r -R qtd_user_rating.txt;
fs -rm -f -r -R user_max_rate_date.txt
fs -rm -f -r -R qtd_movie_by_gender.txt

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



