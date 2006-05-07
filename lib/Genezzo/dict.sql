REM  $Header: /Users/claude/fuzz/lib/Genezzo/RCS/dict.sql,v 1.9 2006/05/07 06:47:09 claude Exp claude $
REM
REM $Revision: 1.9 $
REM
REM copyright (c) 2005, 2006 Jeffrey I Cohen, all rights reserved, worldwide
REM
REM dict.sql - additional dictionary objects
REM 
REM   Contains recursive SQL to construct additional dictionary objects in 
REM   "phase three" during database creation.
REM
REM   Note: end all commands 
REM   (even Feeble commands (with the exception of _REMarks_)) 
REM   with semicolon
REM

REM ct dict_test_1 a=c b=c     ;
REM i  dict_test_1 a 1 b 2 c 3 ;

alter table _tspace add constraint tspace_tsname_uk unique (tsname);

create table dual (dummy varchar(1));
insert into dual values ('X');

select HavokUse('Genezzo::Havok') from dual;
select HavokUse('Genezzo::Havok::SQLScalar') from dual;



REM always commit changes!!
commit ;
