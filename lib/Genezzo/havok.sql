REM Generated by Genezzo::Havok version 7.01 on 2005-07-19T00:54:24
REM
REM Copyright (c) 2004, 2005 Jeffrey I Cohen.  All rights reserved.
REM
REM 
ct havok hid=n modname=c owner=c creationdate=c flag=c version=c
ct user_extend xid=n xtype=c xname=c args=c owner=c creationdate=c version=c
i havok 1 Genezzo::Havok SYSTEM 2005-07-19T00:54:24 0 7.01
i havok 2 Genezzo::Havok::UserExtend SYSTEM 2005-07-19T00:54:24 0 0

REM HAVOK_EXAMPLE
REM select * from tab1 where Genezzo::Havok::Examples::isRedGreen(col1)
i user_extend 1 require Genezzo::Havok::Examples isRedGreen SYSTEM 2005-07-19T00:54:24 0
i user_extend 2 require Text::Soundex soundex SYSTEM 2005-07-19T00:54:24 0



commit
shutdown
startup
