REM Generated by Genezzo::Havok version 7.11 on 2006-08-11T00:53:10
REM
REM Copyright (c) 2004-2006 Jeffrey I Cohen.  All rights reserved.
REM
REM 
select HavokUse('Genezzo::Havok') from dual

REM HAVOK_EXAMPLE
REM select * from tab1 where Genezzo::Havok::Examples::isRedGreen(col1)
select HavokUse('Genezzo::Havok::UserExtend') from dual
i user_extend 1 require Genezzo::Havok::Examples isRedGreen SYSTEM 2006-08-11T00:53:10 0
i user_extend 2 require Text::Soundex soundex SYSTEM 2006-08-11T00:53:10 0



commit
shutdown
startup
