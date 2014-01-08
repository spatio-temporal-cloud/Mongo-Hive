drop database if exists testdb3 cascade;
create database if not exists testdb3;
use testdb3;
create table testta1(id string, line1 string)
row format delimited
fields terminated by ' ';
create table testta2(id string, line2 string)
row format delimited
fields terminated by ' ';
create table testjoin(id string, line1 string, line2 string);
load data local inpath 'path of file testta1.txt' into table testta1;
load data local inpath 'path of file testta2.txt' into table testta2;
insert overwrite table testjoin select testta1.id as id, testta1.line1 as line1, testta2.line2 as line2 from testta1 left outer join testta2 on testta1.id = testta2.id;
select * from testta1;
select * from testta2;
select * from testjoin;
select count(*) from testta1;
select count(*) from testta2;
select count(*) from testjoin;

