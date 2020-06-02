-- Script Pig Clesio Santos

-- limpar logs 
rm -rf ./*.logs

-- CARREGA OS ARQUIVOS PARA MANIPULAÇÃO
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
ratings = LOAD 'ratings.csv' USING CSVExcelStorage() AS (userId:int,movieId:int,rating:float,timestamp:long);
movies = LOAD 'movies.csv' USING CSVExcelStorage() AS (movieId:int ,title: chararray,genres: chararray);
tags = LOAD 'tags.csv' USING CSVExcelStorage() AS (userId:int, movieId:int,tag: chararray,timestamp:long);


-- QUESTAO 1 - Quantidade de usuários distintos que fizeram avaliação de filmes (DUMP)
1_ratings_userid = FOREACH ratings GENERATE $0;
1_d_ratings_userid = DISTINCT 1_ratings_userid;
1_g_ratings_userid = GROUP 1_d_ratings_userid ALL;
1_count_distinct_userid = FOREACH 1_g_ratings_userid GENERATE COUNT(1_d_ratings_userid);
DUMP count_distinct_userid; 

-- QUESTAO 2 - quantidade de avaliações por usuário (qtd_user_rating.txt)
