-- Script Pig Clesio Santos
-- limpar logs 
sh rm -rf ./*.logs

-- CARREGA OS ARQUIVOS PARA MANIPULAÇÃO
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
ratings = LOAD 'ratings.csv' USING CSVExcelStorage() AS (userId:int,movieId:int,rating:float,timestamp:long);
movies = LOAD 'movies.csv' USING CSVExcelStorage() AS (movieId:int ,title: chararray,genres: chararray);
tags = LOAD 'tags.csv' USING CSVExcelStorage() AS (userId:int, movieId:int,tag: chararray,timestamp:long);
	

-- QUESTAO 1 - Quantidade de usuários distintos que fizeram avaliação de filmes (DUMP)
ratings_userid_1 = FOREACH ratings GENERATE $0;
d_ratings_userid_1 = DISTINCT ratings_userid_1;
g_ratings_userid = GROUP d_ratings_userid_1 ALL;
count_distinct_userid = FOREACH g_ratings_userid_1 GENERATE COUNT(d_ratings_userid_1);
DUMP count_distinct_userid; 

-- QUESTAO 2 - quantidade de avaliações por usuário (qtd_user_rating.txt)
