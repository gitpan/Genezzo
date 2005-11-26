REM  $Header: /Users/claude/fuzz/lib/Genezzo/RCS/dict.sql,v 1.6 2005/10/02 07:29:06 claude Exp claude $
REM
REM $Revision: 1.6 $
REM
REM dict.sql - addition dictionary objects
REM
REM   Note: end all commands 
REM   (even Feeble commands (with the exception of _REMarks_)) 
REM   with semicolon

REM ct dict_test_1 a=c b=c     ;
REM i  dict_test_1 a 1 b 2 c 3 ;

alter table _tspace add constraint tspace_tsname_uk unique (tsname);

REM always commit changes!!
commit ;
