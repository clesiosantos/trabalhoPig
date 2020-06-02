-- Script Pig Clesio Santos

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
ratings = LOAD 'ratings.csv' USING CSVExcelStorage() AS (userId:int,movieId:int,rating:float,timestamp:long);
movies = LOAD 'movies.csv' USING CSVExcelStorage() AS (movieId:int ,title: chararray,genres: chararray);
tags = LOAD 'tags.csv' USING CSVExcelStorage() AS (userId:int, movieId:int,tag: chararray,timestamp:long);


count_usuario_distinto = FOREACH ratings generate COUNT(userId) ;  
DUMP count_usuario_distinto;
