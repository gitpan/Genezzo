REM Generated by Genezzo::Havok version 1.06 on 2004-12-07T23:01:42
REM
REM Copyright (c) 2004 Jeffrey I Cohen.  All rights reserved.
REM
REM 
ct havok hid=n modname=c owner=c creationdate=c flag=c version=c
ct user_extend xid=n xtype=c xname=c args=c owner=c creationdate=c version=c
i havok 1 Genezzo::Havok SYSTEM 2004-12-07T23:01:42 0 1.06
i havok 2 Genezzo::Havok::UserExtend SYSTEM 2004-12-07T23:01:42 0 0

REM 
REM select * from tab1 where Genezzo::Havok::RedGreen::isRedGreen(col1)
i user_extend 1 require Genezzo::Havok::RedGreen isRedGreen SYSTEM 2004-12-07T23:01:42 0
i user_extend 2 require Text::Soundex soundex SYSTEM 2004-12-07T23:01:42 0



commit
shutdown
startup
