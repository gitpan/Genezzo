REM Copyright (c) 2004 Jeffrey I Cohen.  All rights reserved.
REM
REM 
ct havok hid=n modname=c owner=c creationdate=c flag=c
ct user_extend xid=n xtype=c xname=c args=c owner=c creationdate=c 
i havok 1 Genezzo::Havok::UserExtend SYSTEM 2004-09-21T12:12 0

REM 
REM select * from tab1 where Genezzo::Havok::RedGreen::isRedGreen(col1)
i user_extend 1 require Genezzo::Havok::RedGreen isRedGreen SYSTEM 2004-09-21T12:12
i user_extend 2 require Text::Soundex soundex SYSTEM 2004-09-21T12:12

REM select * from tab1 where isBlueYellow(col1)
i user_extend 3 function isBlueYellow '{return undef unless scalar(@_);   return ($_[0] =~ m/^(blue|yellow)$/i); }' SYSTEM 2004-09-21T12:12


commit
shutdown
startup

